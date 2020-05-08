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
read -p "What is the HCFS URI for the driver jar [enter for default]? " -i "gs://eecs-e6895-bucket/jars/personality-project_0.1-0.0.3-jar-with-dependencies.jar" -e JAR
[[ -z "$JAR" ]] && JAR="gs://eecs-e6895-bucket/jars/personality-project_0.1-0.0.3-jar-with-dependencies.jar"

# JOb IPIP Input
read -p "What is the input IPIP CSV [enter for default]? " -i "gs://eecs-e6895-bucket/input/data-10.csv" -e INPUT1

# Job Goals Input
read -p "What is the input Goals CSV [enter for default]? " -i "gs://eecs-e6895-bucket/input/goals-small.csv" -e INPUT2

# Start the cluster
echo ""
echo "Name: $CLUSTER"
echo "Class: $CLASS"
echo "Jar: $JAR"
echo "IPIP CSV: $INPUT1"
echo "Goals CSV: $INPUT2"
# echo "Output: $OUTPUT"

# Check if output exists, then delete it
# echo ""
# if gsutil ls "${OUTPUT}" | grep -q 'CommandException: One or more URLs matched no objects.'; then
#    echo "'${OUTPUT}' does not exist!"
# else 
#     echo "Removing output directory '${OUTPUT}'"
#     gsutil rm -r "${OUTPUT}"
# fi

echo -e "\nSubmit the job...\n"

# turn bash echo on
set -x
# Start the job
gcloud dataproc jobs submit spark \
    --cluster=$CLUSTER \
    --class=$CLASS \
    --region=us-west1 \
    --jars=$JAR \
    -- $INPUT1 $INPUT2
    # -- $INPUT1 $OUTPUT
