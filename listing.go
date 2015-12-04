package main

// Import the AWS SDK for Go
import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/TuneDB/env"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"log"
	"os"
	"strings"
	"time"
	"sync"
	"github.com/rcrowley/go-metrics"
)

/**
 * Don't hard-code your credentials!
 * Export the following environment variables instead:
 *
 * export AWS_ACCESS_KEY_ID='AKID'
 * export AWS_SECRET_ACCESS_KEY='SECRET'
**/

type config struct {
	MaxPages            int    `env:"key=MAX_PAGES default=0"`
	MaxKeys             int    `env:"key=MAX_KEYS default=1000"`
	AdvMaxPages         int    `env:"key=ADV_MAX_PAGES default=5"`
	AdvMaxKeys          int    `env:"key=ADV_MAX_KEYS default=5"`
	FileMaxPages        int    `env:"key=FILE_MAX_PAGES default=0"`
	FileMaxKeys         int    `env:"key=FILE_MAX_KEYS default=1000"`
	Force               bool   `env:"key=FORCE default=false"`
	Region              string `env:"key=AWS_REGION required=true"`
	Bucket              string `env:"key=AWS_BUCKET required=true"`
	Prefix              string `env:"key=AWS_PREFIX required=true"`
	OutputFormat        string `env:"key=OUTPUT_FORMAT default=csv"`
	OutputLocation      string `env:"key=OUTPUT_LOCATION default=./target"`
	ListAllWorkersCount int    `env:"key=LIST_ALL_WORKERS_COUNT default=5"`
}

var cfg *config

// Don't take seriously, just playing around with func objects and redirection possibilities
var output OutputFunc

type OutputFunc func(p string)

func stdoutOutput(p string) {
	fmt.Fprint(os.Stdout, p)
}

func stderrOutput(p string) {
	fmt.Fprint(os.Stderr, p)
}

//var outputFileHandle *os.File

//func fileOutput(p string) {
//	outputFileHandle.WriteString(p)
//}

type ListObjectProcessFunc func(o *ListObject)

func outputObject(o *ListObject) {
	output(printObject(o))
}

var files []ListObject = make([]ListObject, 0)

func appendObject(o *ListObject) {
	files = append(files, *o)
}

type ListAllObjectsRequest struct {
	Region     string
	Bucket     string
	Prefix     string
	Process    ListObjectProcessFunc
	OutputFile string
}

func main() {
	t := metrics.NewTimer()
	metrics.Register("duration", t)
	
	cfg = &config{}

	if err := env.Process(cfg); err != nil {
		log.Fatal(err)
	}

	t.Time(run)
	
	log.Printf("Execution time: %v", time.Duration(t.Sum()) * time.Nanosecond)
	metrics.WriteJSONOnce(metrics.DefaultRegistry, os.Stderr)
}


func run() {
	cfg = &config{}

	if err := env.Process(cfg); err != nil {
		log.Fatal(err)
	}

	output = stdoutOutput


	listAllObjectsQueue := make(chan ListAllObjectsRequest, 5)
	var wg sync.WaitGroup
	
	doneCallback := func() { 
		defer wg.Done()
		log.Printf("worker done")
	}
	
	for i := 0; i < cfg.ListAllWorkersCount; i++ {
		log.Printf("Starting worker%d", i+1)
		worker := NewListObjectRequestWorker(i + 1, listAllObjectsQueue, doneCallback)
		
		wg.Add(1)
		
		go func(){
//			wg.Add(1)
			worker.Start()
		}()
	}

	go func() {
		ListAllObjectsProducer(listAllObjectsQueue)
		close(listAllObjectsQueue)
	}()
	
	wg.Wait()
}

func ListAllObjectsProducer(listAllObjectsQueue chan ListAllObjectsRequest) {
	log.Println("Starting producer")
	// Create services we will use, inlining the config instead of using `session`
	// V4 signing requires to connect to the explicit region
	s3Services := make(map[string]*s3.S3)
	s3Services[cfg.Region] = s3.New(session.New(), aws.NewConfig().WithRegion(cfg.Region))

	files = make([]ListObject, 0)
	cfg.MaxKeys = cfg.AdvMaxKeys
	cfg.MaxPages = cfg.AdvMaxPages
	// Find files in the source buckets in different regions
	listObjects(cfg.Bucket, cfg.Prefix, "/", s3Services[cfg.Region], appendObject)

	// TODO fix this hack... these need be in context of request not globals
	cfg.MaxKeys = cfg.FileMaxKeys
	cfg.MaxPages = cfg.FileMaxPages

	log.Printf("Found %v objects", len(files))

	//	files = make([]ListObject, 0)
	//	cfg.MaxPages = 1
	//	cfg.MaxKeys = 5
	//	// Find files in the source buckets in different regions
	//	listFiles(cfg.Bucket, cfg.Prefix, cfg.Delimiter, s3Services[cfg.Region], appendObject)
	//
	//	log.Printf("Found %v files", len(files))

	for _, file := range files {
		if file.Type != "PREFIX" {
			continue
		}

		fileParts := strings.Split(strings.Trim(file.File, "/"), "/")
		advertiserId := fileParts[len(fileParts) - 1]

		outputFile := fmt.Sprintf("%v/advertiser_%v", cfg.OutputLocation, advertiserId)

		if !cfg.Force {
			if _, err := os.Stat(outputFile); !os.IsNotExist(err) {
				log.Printf("%v file already exists, skipping", outputFile)
				continue
			}
		}

		request := ListAllObjectsRequest{Region: cfg.Region, Bucket: cfg.Bucket, Prefix: fmt.Sprintf("%v%v/", cfg.Prefix, advertiserId), Process: outputObject, OutputFile: outputFile }

		listAllObjectsQueue <- request
	}
	
	log.Println("Finish producer")
}


func (w ListObjectRequestWorker) Start() {

	processRequestChan := make(chan ListAllObjectsRequest)
	
	go func() {
		defer close(processRequestChan)
		
		for request := range w.RequestQueue {
			log.Printf("popping request from queue for worker%d", w.ID)
			select {
			case processRequestChan <- request:
				log.Printf("push request to process channel for worker%d", w.ID)
			case <-w.QuitChan:
				log.Printf("drainer for worker%d stopping", w.ID)
				return
			}
		}

		log.Printf("drainer for worker%d finished", w.ID)
	}()

	go func() {
		defer w.DoneCallback()
		
		for request := range processRequestChan {
			log.Printf("processing request for worker%d", w.ID)

			f, err := os.OpenFile(request.OutputFile, os.O_WRONLY | os.O_CREATE, 0666)

			if err != nil {
				log.Fatalf("Cannot create file: %v | %v", request.OutputFile, err)
			}
			
			fileOuput := func(o *ListObject) {
				f.WriteString(printObject(o))
			}

			f.WriteString("\n")
			
			listObjects(request.Bucket, request.Prefix, "", s3.New(session.New(), aws.NewConfig().WithRegion(request.Region)), fileOuput)
			
			f.Close()
		}

		log.Printf("processor for worker%d finished", w.ID)
	}()
}
			
//				f, err := os.OpenFile(request.OutputFile, os.O_WRONLY | os.O_CREATE, 0666)
//	
//				if err != nil {
//					log.Fatalf("Cannot create file: %v | %v", request.OutputFile, err)
//				}
//	
//				outputFileHandle = f
//				output = fileOutput

//				defer f.Close()

//				f.WriteString("\n")
//		}
//	}()
		
//			select {
//			case request := <-w.RequestQueue:
//			//				f, err := os.OpenFile(request.OutputFile, os.O_WRONLY | os.O_CREATE, 0666)
//			//	
//			//				if err != nil {
//			//					log.Fatalf("Cannot create file: %v | %v", request.OutputFile, err)
//			//				}
//			//	
//			//				outputFileHandle = f
//			//				output = fileOutput
//
//			//				defer f.Close()
//
//
//			// Find files in the source buckets in different regions
//				listObjects(request.Bucket, request.Prefix, "", request.Region, request.Output)
//
//			//				f.WriteString("\n")
//			case <-w.QuitChan:
//				log.Printf("worker%d stopping", w.ID)
//				return
//			}
//		}
//	}()
//}

func (w ListObjectRequestWorker) Stop() {
	go func() {
		w.QuitChan <- true
	}()
}

type ListObjectRequestWorker struct {
	ID           int
	RequestQueue chan ListAllObjectsRequest
	QuitChan     chan bool
	DoneCallback func()
}

func NewListObjectRequestWorker(id int, requestQueue chan ListAllObjectsRequest, doneCallback func()) ListObjectRequestWorker {

	worker := ListObjectRequestWorker{
		ID:    id,
		RequestQueue: requestQueue,
		QuitChan: make(chan bool),
		DoneCallback: doneCallback,
	}

	return worker
}

type ListObject struct {
	Type         string
	Bucket       string
	File         string
	Size         int64
	LastModified time.Time
	svc          *s3.S3
}

func listObjects(bucket string, prefix string, delimiter string, svc *s3.S3, process ListObjectProcessFunc) {
	log.Printf("Retrieving object listing for %v/%v using service: %+v", bucket, prefix, svc.ClientInfo.Endpoint)

	params := &s3.ListObjectsInput{
		Bucket:  aws.String(bucket), // Required
		MaxKeys: aws.Int64(int64(cfg.MaxKeys)),
	}

	if delimiter != "" {
		params.Delimiter = aws.String(delimiter)
	}

	if prefix != "" {
		params.Prefix = aws.String(prefix)
	}

	pageNum := 0
	objectNum := 0

	err := svc.ListObjectsPages(params, func(page *s3.ListObjectsOutput, lastPage bool) bool {
		pageNum++
		log.Printf("Processing page %v for %v/%v", pageNum, bucket, prefix)

		for _, value := range page.Contents {
			file := ListObject{Type: "OBJECT", Bucket: bucket, File: *value.Key, Size: *value.Size, LastModified: *value.LastModified, svc: svc}
			objectNum++

			process(&file)
		}

		for _, value := range page.CommonPrefixes {
			file := ListObject{Type: "PREFIX", Bucket: bucket, File: *value.Prefix, svc: svc}
			objectNum++

			process(&file)
		}

		return (pageNum < cfg.MaxPages) || (cfg.MaxPages == 0)
	})

	log.Printf("Found %v objects for %v/%v", objectNum, bucket, prefix)

	if err != nil {
		log.Println(err.Error())
	}
}

// TODO Should look at changing this to func definitions that are loaded into a struct and called dynamically
func printObject(object *ListObject) string {
	var output string

	switch cfg.OutputFormat {
	case "csv":
		output = printCsv(object)
	case "json":
		output = printJson(object)
	default:
		log.Fatal("Unknown output format: ", cfg.OutputFormat)
	}

	return output
}

func printJson(object *ListObject) string {
	b, err := json.Marshal(object)

	if err != nil {
		log.Println("error:", err)
	}

	// little trick to append newline to a byte slice
	return string(append(b, "\n"...))
}

func printCsv(object *ListObject) string {
	b := &bytes.Buffer{}
	writer := csv.NewWriter(b)
	writer.Write([]string{object.Type, object.Bucket, object.File, fmt.Sprint(object.Size), fmt.Sprint(object.LastModified)})
	writer.Flush()
	return b.String()
}
