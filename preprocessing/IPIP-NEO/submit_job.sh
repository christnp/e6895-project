#!/bin/bash


read -p "What cluster to run this job on [enter for default]? " -i "preprocess" -e CLUSTER

# check if $CLUSTER variable is empty; if it is, make it default
[[ -z "$CLUSTER" ]] && CLUSTER=preprocess

# JOb IPIP Input
read -p "What is the Python script location [enter for default]? " -i "gs://eecs-e6895-bucket/input/ipipneo_parser.py" -e SCRIPT

PROFILER_NAME='ipipneo_parser'
PROFILER_VER='debug'
gcloud dataproc jobs submit pyspark $SCRIPT \
    --cluster=$CLUSTER --region=us-west1 \
    --properties=cloud.profiler.enable=true,cloud.profiler.name=$PROFILER_NAME,cloud.profiler.service.version=$PROFILER_VER \
    
