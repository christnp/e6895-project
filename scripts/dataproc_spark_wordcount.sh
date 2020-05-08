#!/bin/bash

# Dataproc Image
IMAGE="1.4-ubuntu18"
# Cluster Name
read -p "What cluster to run this job on [enter for default]? " -i "mycluster" -e CLUSTER

# check if $CLUSTER variable is empty; if it is, make it default
[[ -z "$CLUSTER" ]] && CLUSTER=mycluster

# Scala Class
read -p "What class is the main method for the driver [enter for default]? " -i "WordCount" -e CLASS
[[ -z "$CLASS" ]] && CLASS=WordCount

# Scala Jar
read -p "What is the HCFS URI for the driver jar [enter for default]? " -i "gs://eecs-e6895-bucket/jars/sparkwordcount-0.0.2.jar" -e JAR
[[ -z "$JAR" ]] && JAR="gs://eecs-e6895-bucket/jars/sparkwordcount-0.0.2.jar"

# Job Input
read -p "What is the input [enter for default]? " -i "gs://eecs-e6895-bucket/input/othello.txt" -e INPUT
# [[ -z "$INPUT" ]] && INPUT="gs://eecs-e6895-bucket/input/othello.txt"
# Job Output
read -p "What is the output [enter for default]? " -i "gs://eecs-e6895-bucket/output/result.txt" -e OUTPUT
# [[ -z "$OUTPUT" ]] && OUTPUT="gs://eecs-e6895-bucket/output/result.txt"

# Start the cluster
echo ""
echo "Name: $CLUSTER"
echo "Class: $CLASS"
echo "Jar: $JAR"
echo "Input: $INPUT"
echo "Output: $OUTPUT"

# Check if output exists, then delete it
echo ""
if gsutil ls "${OUTPUT}" | grep -q 'CommandException: One or more URLs matched no objects.'; then
   echo "'${OUTPUT}' does not exist!"
else 
    echo "Removing output directory '${OUTPUT}'"
    gsutil rm -r "${OUTPUT}"
fi

# Start the job
gcloud dataproc jobs submit spark \
    --cluster=$CLUSTER \
    --class=$CLASS \
    --region=us-west1 \
    --jars=$JAR \
    -- $INPUT $OUTPUT
