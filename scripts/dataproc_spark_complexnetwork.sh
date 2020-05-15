#!/bin/bash

# Dataproc Image
IMAGE="1.4-ubuntu18"
# Cluster Name
read -p "What cluster to run this job on [enter for default]? " -i "analysis" -e CLUSTER

# check if $CLUSTER variable is empty; if it is, make it default
[[ -z "$CLUSTER" ]] && CLUSTER=analysis

# Scala Class
read -p "What class is the main method for the driver [enter for default]? " -i "ComplexNetwork" -e CLASS
[[ -z "$CLASS" ]] && CLASS=ComplexNetwork

# Scala Jar
read -p "What is the HCFS URI for the driver jar [enter for default]? " -i "gs://eecs-e6895-bucket/jars/personality-project_0.1-0.0.4.jar" -e JAR
[[ -z "$JAR" ]] && JAR="gs://eecs-e6895-bucket/jars/personality-project_0.1-0.0.4.jar"

# Define size of output data (i.e., might want it smaller for debugging)
read -p "Limit output to size to... [enter 0 (zero) for all data] " -i "10" -e SIZE

read -p "Would you like to enter CP-ALS parameters? [yes/no] " -i "yes" -e PARAMS
if [ "$PARAMS" == "yes" ]; then
    read -p "Desired factor factor matrix rank... [must be >= 1] " -i "4" -e RANK
    read -p "Tolerance for the CP-ALS algorithm [mst be > 0] " -i "0.002" -e TOL
    read -p "Maximum iterations for CP-ALS befre exiting... [must be > 0] " -i "25" -e MAXITER
fi

# JOb IPIP score Input
read -p "Use CSV data on Google Storage [enter DEBUG, or leave empty for RELEASE]? " -i "" -e DEBUG
if [ "$DEBUG" == "DEBUG" ]; then
    read -p "What is the input IPIP CSV [or empty string to continue]? " -i "gs://eecs-e6895-bucket/input/part-00000-b88c7ec6-a641-49e6-b24d-9cc994e8c7c0-c000.csv" -e INPUT5
else
    CSV_FILE=""
fi

# Setup GCP profiler
read -p "Define the profiler name: " -i "ComplexNetwork" -e PROFILER_NAME
[[ -z "$PROFILER_NAME" ]] && PROFILER_NAME=ComplexNetwork
read -p "Define the profiler version: " -i "analysis" -e PROFILER_VER
[[ -z "$PROFILER_VER" ]] && PROFILER_VER=analysis

# Start the cluster
echo ""
echo "Name: $CLUSTER"
echo "Class: $CLASS"
echo "Jar: $JAR"
echo "Data size: $SIZE"
if [ "$PARAMS" == "yes" ]; then
    echo "CPALS Rank: $RANK"
    echo "CPALS Tolerance: $TOL"
    echo "CPALS Max Iterations: $MAXITER"
fi
echo "Data size: $SIZE"
echo "IPIP CSV: $CSV_FILE"
echo "GCP Profiler Name: $PROFILER_NAME"
echo "GCP Profiler Version: $PROFILER_VER"


echo -e "\nSubmit the job...\n"


# turn bash echo on
# Start the job
# if [ -z "$CSV_FILE" ]
# then
#     set -x
#     gcloud dataproc jobs submit spark \
#         --cluster=$CLUSTER \
#         --class=$CLASS \
#         --region=us-west1 \
#         --jars="$JAR,gs://spark-lib/bigquery/spark-bigquery-latest.jar" \
#         --properties="cloud.profiler.enable=true,cloud.profiler.name=$PROFILER_NAME,cloud.profiler.service.version=$PROFILER_VER" \
#         --properties="spark.jars.packages=graphframes:graphframes:0.8.0-spark2.4-s_2.11","spark.jars.packages=neo4j-contrib:neo4j-spark-connector:2.4.1-M1" \
#         -- $SIZE
#         # --properties="spark.jars.packages=graphframes:graphframes:0.8.0-spark2.4-s_2.11,neo4j-contrib:neo4j-spark-connector:2.4.1-M1" \
#         # --packages="graphframes:graphframes:0.8.0-spark2.4-s_2.11" \
#         # --packages="com.google.cloud.spark:spark-bigquery-with-dependencies_2.11:0.14.0-beta" \
#         # --jars="$JAR,gs://spark-lib/bigquery/spark-bigquery-latest.jar" \
#         # --repositories="https://dl.bintray.com/spark-packages/maven/" \
#         # https://cloud.google.com/dataproc/docs/guides/manage-spark-dependencies
# else if [ "$PARAMS" == "yes" ]; then
    set -x
    gcloud dataproc jobs submit spark \
        --cluster=$CLUSTER \
        --class=$CLASS \
        --region=us-west1 \
        --jars="$JAR,gs://spark-lib/bigquery/spark-bigquery-latest.jar" \
        --properties="cloud.profiler.enable=true,cloud.profiler.name=$PROFILER_NAME,cloud.profiler.service.version=$PROFILER_VER" \
        --properties="spark.jars.packages=graphframes:graphframes:0.8.0-spark2.4-s_2.11" \
        -- $SIZE $RANK $TOL $MAXITER $CSV_FILE
# else
#     set -x
#     gcloud dataproc jobs submit spark \
#         --cluster=$CLUSTER \
#         --class=$CLASS \
#         --region=us-west1 \
#         --jars="$JAR,gs://spark-lib/bigquery/spark-bigquery-latest.jar" \
#         --properties="cloud.profiler.enable=true,cloud.profiler.name=$PROFILER_NAME,cloud.profiler.service.version=$PROFILER_VER" \
#         --properties="spark.jars.packages=graphframes:graphframes:0.8.0-spark2.4-s_2.11" \
#         -- $SIZE $CSV_FILE
#         # --properties="spark.jars.packages=graphframes:graphframes:0.8.0-spark2.4-s_2.11,neo4j-contrib:neo4j-spark-connector:2.4.1-M1" \
#         # --packages="graphframes:graphframes:0.8.0-spark2.4-s_2.11" \
#         # --packages="com.google.cloud.spark:spark-bigquery-with-dependencies_2.11:0.14.0-beta" \
#         # --repositories="https://dl.bintray.com/spark-packages/maven/" \
#         # --jars="$JAR,gs://spark-lib/bigquery/spark-bigquery-latest.jar" \
#         # https://cloud.google.com/dataproc/docs/guides/manage-spark-dependencies

# fi

    # -- $SIZE $OUTPUT
