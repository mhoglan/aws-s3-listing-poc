#!/bin/bash

if [ -z "$1" ]
  then
    echo "Must supply profile as argument"
    exit
fi

PROFILE=$1

export AWS_ACCESS_KEY_ID=$(aws configure get aws_access_key_id --profile $PROFILE)
export AWS_SECRET_ACCESS_KEY=$(aws configure get aws_secret_access_key --profile $PROFILE)

CONFIG_ENV_FILE=./config.env

if [ -f $CONFIG_ENV_FILE ]; then
    source $CONFIG_ENV_FILE
fi

go run listing.go
