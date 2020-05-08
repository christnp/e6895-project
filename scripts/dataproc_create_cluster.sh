#!/bin/bash

# Dataproc Image
IMAGE="1.4-ubuntu18"
# Cluster Name
read -p "What name to use for this cluster [enter for default]? " -i "preprocess" -e CLUSTER

# check if $CLUSTER variable is empty; if it is, make it default
[[ -z "$CLUSTER" ]] && CLUSTER=mycluster

# GCP Project
read -p "What project to use for this cluster [enter for default]? " -i "eecs-e6895-edu" -e PROJECT
[[ -z "$PROJECT" ]] && PROJECT="eecs-e6895-edu"

# GCP Bucket
read -p "What bucket to use for this cluster [enter for default]? " -i "eecs-e6895-bucket" -e BUCKET
[[ -z "$BUCKET" ]] && BUCKET="eecs-e6895-bucket"

echo ""
echo "Name: $CLUSTER"
echo "Image: $IMAGE"
echo "Project: $PROJECT"
echo "Bucket: $BUCKET"

echo -e "\n Create the cluster..."

# Start the cluster
set -x
gcloud beta dataproc clusters create $CLUSTER \
--image-version=$IMAGE \
--project=$PROJECT \
--bucket=$BUCKET \
--enable-component-gateway \
--scopes="cloud-platform" \
--properties="dataproc:dataproc.logging.stackdriver.enable=true,\
dataproc:dataproc.monitoring.stackdriver.enable=true,\
dataproc:dataproc.logging.stackdriver.job.yarn.container.enable=true" \
--properties="yarn:yarn.log-aggregation-enable=true,\
yarn:yarn.nodemanager.remote-app-log-dir=gs://${BUCKET}/logs,\
yarn:yarn.log-aggregation.retain-seconds=-1" \
--num-workers 2 
# --properties="spark:spark.jars=gs://spark-lib/bigquery/spark-bigquery-latest.jar" \
# --single-node 
# --master-machine-type=n1-highmem-8 \
# --worker-machine-type=n1-highmem-8 \
# --initialization-actions="gs://${BUCKET}/init/sadsasdaasd.asdfas"  \
