#!/bin/bash

# Dataproc Image
IMAGE="1.4-ubuntu18"
# Cluster Name
read -p "What cluster to run this job on [enter for default]? " -i "analysis" -e CLUSTER

# check if $CLUSTER variable is empty; if it is, make it default
[[ -z "$CLUSTER" ]] && CLUSTER=analysis

# Scala Class
read -p "What class is the main method for the driver [enter for default]? " -i "TensorCP" -e CLASS
[[ -z "$CLASS" ]] && CLASS=TensorCP

# Scala Jar
read -p "What is the HCFS URI for the driver jar [enter for default]? " -i "gs://eecs-e6895-bucket/jars/personality-project_0.1-0.0.4.jar" -e JAR
[[ -z "$JAR" ]] && JAR="gs://eecs-e6895-bucket/jars/personality-project_0.1-0.0.4.jar"

# Define size of output data (i.e., might want it smaller for debugging)
read -p "Input argument 1 (arg(0)) " -i "gs://eecs-e6895-bucket/input/test.md" -e INPUT1

read -p "Input argument 2 (arg(1)) " -i "gs://eecs-e6895-bucket/output/" -e INPUT2

# Setup GCP profiler
read -p "Define the profiler name: " -i "Generic" -e PROFILER_NAME
[[ -z "$PROFILER_NAME" ]] && PROFILER_NAME=Generic
read -p "Define the profiler version: " -i "analysis" -e PROFILER_VER
[[ -z "$PROFILER_VER" ]] && PROFILER_VER=analysis

# Start the cluster
echo ""
echo "Name: $CLUSTER"
echo "Class: $CLASS"
echo "Jar: $JAR"
echo "Input 1: $INPUT1"
echo "Input 2: $INPUT2"
echo "GCP Profiler Name: $PROFILER_NAME"
echo "GCP Profiler Version: $PROFILER_VER"


echo -e "\nSubmit the job...\n"


# turn bash echo on
# Start the job
if [ -z "$INPUT2" ]
then
    set -x
    gcloud dataproc jobs submit spark \
        --cluster=$CLUSTER \
        --class=$CLASS \
        --region=us-west1 \
        --jars="$JAR,gs://spark-lib/bigquery/spark-bigquery-latest.jar" \
        --properties="cloud.profiler.enable=true,cloud.profiler.name=$PROFILER_NAME,cloud.profiler.service.version=$PROFILER_VER" \
        --properties="spark.jars.packages=graphframes:graphframes:0.8.0-spark2.4-s_2.11","spark.jars.packages=neo4j-contrib:neo4j-spark-connector:2.4.1-M1" \
        -- $INPUT1
else
    set -x
    gcloud dataproc jobs submit spark \
        --cluster=$CLUSTER \
        --class=$CLASS \
        --region=us-west1 \
        --jars="$JAR,gs://spark-lib/bigquery/spark-bigquery-latest.jar" \
        --properties="cloud.profiler.enable=true,cloud.profiler.name=$PROFILER_NAME,cloud.profiler.service.version=$PROFILER_VER" \
        --properties="spark.jars.packages=graphframes:graphframes:0.8.0-spark2.4-s_2.11" \
        -- $INPUT1 $INPUT2
fi

