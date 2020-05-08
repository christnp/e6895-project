#!/bin/bash

# Dataproc Image
IMAGE="1.4-ubuntu18"
# Cluster Name
read -p "What cluster to run this job on [enter for default]? " -i "analysis" -e CLUSTER

# check if $CLUSTER variable is empty; if it is, make it default
[[ -z "$CLUSTER" ]] && CLUSTER=analysis

# Scala Class
read -p "What class is the main method for the driver [enter for default]? " -i "SimpleNetwork" -e CLASS
[[ -z "$CLASS" ]] && CLASS=SimpleNetwork

# Scala Jar
read -p "What is the HCFS URI for the driver jar [enter for default]? " -i "gs://eecs-e6895-bucket/jars/personality-project_0.1-0.0.4.jar" -e JAR
[[ -z "$JAR" ]] && JAR="gs://eecs-e6895-bucket/jars/personality-project_0.1-0.0.4-jar-with-dependencies.jar"

# Define size of output data (i.e., might want it smaller for debugging)
read -p "Limit output to size to... [enter 0 (zero) for all data] " -i "0" -e INPUT1

# JOb IPIP score Input
read -p "Use CSV data on Google Storage [enter DEBUG, or leave empty for RELEASE]? " -i "" -e DEBUG
if [ "$DEBUG" == "DEBUG" ]; then
    read -p "What is the input IPIP CSV [or empty string to continue]? " -i "gs://eecs-e6895-bucket/input/part-00000-b88c7ec6-a641-49e6-b24d-9cc994e8c7c0-c000.csv" -e INPUT2
else
    INPUT2=""
fi

# Setup GCP profiler
read -p "Define the profiler name: " -i "DatasetEmu" -e PROFILER_NAME
[[ -z "$PROFILER_NAME" ]] && PROFILER_NAME=DatasetEmu
read -p "Define the profiler version: " -i "analysis" -e PROFILER_VER
[[ -z "$PROFILER_VER" ]] && PROFILER_VER=analysis

# Start the cluster
echo ""
echo "Name: $CLUSTER"
echo "Class: $CLASS"
echo "Jar: $JAR"
echo "Data size: $INPUT1"
echo "IPIP CSV: $INPUT2"
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
        --properties="spark.jars.packages=graphframes:graphframes:0.8.0-spark2.4-s_2.11" \
        -- $INPUT1
        # --packages="graphframes:graphframes:0.8.0-spark2.4-s_2.11" \
        # --packages="com.google.cloud.spark:spark-bigquery-with-dependencies_2.11:0.14.0-beta" \
        # --jars="$JAR,gs://spark-lib/bigquery/spark-bigquery-latest.jar" \
        # --repositories="https://dl.bintray.com/spark-packages/maven/" \
        # https://cloud.google.com/dataproc/docs/guides/manage-spark-dependencies
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
        # --packages="graphframes:graphframes:0.8.0-spark2.4-s_2.11" \
        # --packages="com.google.cloud.spark:spark-bigquery-with-dependencies_2.11:0.14.0-beta" \
        # --repositories="https://dl.bintray.com/spark-packages/maven/" \
        # --jars="$JAR,gs://spark-lib/bigquery/spark-bigquery-latest.jar" \
        # https://cloud.google.com/dataproc/docs/guides/manage-spark-dependencies

fi

    # -- $INPUT1 $OUTPUT
