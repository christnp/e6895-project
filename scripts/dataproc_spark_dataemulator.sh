#!/bin/bash

# Dataproc Image
IMAGE="1.4-ubuntu18"
# Cluster Name
read -p "What cluster to run this job on [enter for default]? " -i "preprocess" -e CLUSTER

# check if $CLUSTER variable is empty; if it is, make it default
[[ -z "$CLUSTER" ]] && CLUSTER=preprocess

# Scala Class
read -p "What class is the main method for the driver [enter for default]? " -i "DatasetEmulator" -e CLASS
[[ -z "$CLASS" ]] && CLASS=DatasetEmulator

# Scala Jar
read -p "What is the HCFS URI for the driver jar [enter for default]? " -i "gs://eecs-e6895-bucket/jars/personality-project_0.1-0.0.4-jar-with-dependencies.jar" -e JAR
[[ -z "$JAR" ]] && JAR="gs://eecs-e6895-bucket/jars/personality-project_0.1-0.0.4-jar-with-dependencies.jar"

# JOb IPIP Input
read -p "What is the input IPIP CSV [enter for default]? " -i "gs://eecs-e6895-bucket/input/IPIP120-final.csv" -e INPUT1

# Job Goals Input
read -p "What is the input Goals CSV [enter for default]? " -i "gs://eecs-e6895-bucket/input/goals.csv" -e INPUT2

# Define size of output data (i.e., might want it smaller for debugging)
read -p "For debug: limit output to size of... [enter 0 (zero) for all data, leave blank for Release mode] " -i "0" -e INPUT3

# Setup GCP profiler
read -p "Define the profiler name: " -i "DatasetEmu" -e PROFILER_NAME
[[ -z "$PROFILER_NAME" ]] && PROFILER_NAME=DatasetEmu
read -p "Define the profiler version: " -i "preprocessing" -e PROFILER_VER
[[ -z "$PROFILER_VER" ]] && PROFILER_VER=debug

echo ""
echo "Name: $CLUSTER"
echo "Class: $CLASS"
echo "Jar: $JAR"
echo "IPIP CSV: $INPUT1"
echo "Goals CSV: $INPUT2"
echo "Datasize (blank=Release): $INPUT3"
echo "GCP Profiler Name: $PROFILER_NAME"
echo "GCP Profiler Version: $PROFILER_VER"

echo -e "\nSubmit the job...\n"

# turn bash echo on
# Start the job
if [ -z "$INPUT3" ]
then
    set -x
    gcloud dataproc jobs submit spark \
        --cluster=$CLUSTER \
        --class=$CLASS \
        --region=us-west1 \
        --jars="$JAR,gs://spark-lib/bigquery/spark-bigquery-latest.jar" \
        --properties="cloud.profiler.enable=true,cloud.profiler.name=$PROFILER_NAME,cloud.profiler.service.version=$PROFILER_VER" \
        -- $INPUT1 $INPUT2
        
else
    set -x
    gcloud dataproc jobs submit spark \
        --cluster=$CLUSTER \
        --class=$CLASS \
        --region=us-west1 \
        --jars="$JAR,gs://spark-lib/bigquery/spark-bigquery-latest.jar" \
        --properties="cloud.profiler.enable=true,cloud.profiler.name=$PROFILER_NAME,cloud.profiler.service.version=$PROFILER_VER" \
        -- $INPUT1 $INPUT2 $INPUT3
fi