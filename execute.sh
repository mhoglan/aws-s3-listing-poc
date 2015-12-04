#!/bin/bash

if [ -z "$1" ]
  then
    echo "Must supply profile as argument"
    exit
fi

PROFILE=$1

export AWS_ACCESS_KEY_ID=$(aws configure get aws_access_key_id --profile $PROFILE)
export AWS_SECRET_ACCESS_KEY=$(aws configure get aws_secret_access_key --profile $PROFILE)
go run listing.go
