#!/bin/bash

# Dataproc Image
IMAGE="1.4-ubuntu18"
# Cluster Name
read -p "What name to use for this cluster [enter for default]? " -i "preprocess" -e CLUSTER

# check if $CLUSTER variable is empty; if it is, make it default
[[ -z "$CLUSTER" ]] && CLUSTER=preprocess

# GCP Project
read -p "What project to use for this cluster [enter for default]? " -i "eecs-e6895-edu" -e PROJECT
[[ -z "$PROJECT" ]] && PROJECT="eecs-e6895-edu"

# GCP Bucket
read -p "What bucket to use for this cluster [enter for default]? " -i "eecs-e6895-bucket" -e BUCKET
[[ -z "$BUCKET" ]] && BUCKET="eecs-e6895-bucket"

# Start the cluster
echo ""
echo "Name: $CLUSTER"
echo "Image: $IMAGE"
echo "Project: $PROJECT"
echo "Bucket: $BUCKET"

echo "\n Create the cluster..."

gcloud beta dataproc clusters create $CLUSTER \
--master-machine-type=n1-highmem-8 \
--worker-machine-type=n1-highmem-8 \
--image-version=$IMAGE \
--project=$PROJECT \
--bucket=$BUCKET \
--enable-component-gateway \
--properties="dataproc:dataproc.logging.stackdriver.enable=true,dataproc:dataproc.monitoring.stackdriver.enable=true" \
--properties="yarn:yarn.log-aggregation-enable=true,yarn:yarn.nodemanager.remote-app-log-dir=gs://${BUCKET}/logs,yarn:yarn.log-aggregation.retain-seconds=-1" \
--num-workers 2 \
--optional-components=ANACONDA,JUPYTER \
--metadata "PIP_PACKAGES=pycountry==19.8.18" \
--initialization-actions gs://dataproc-initialization-actions/python/pip-install.sh 
 #--single-node \
# --properties='dataproc:dataproc.logging.stackdriver.job.driver.enable=true, dataproc:dataproc.monitoring.stackdriver.enable=true',,spark:spark.submit.deployMode=cluster\
# https://stackoverflow.com/questions/47342132/where-are-the-individual-dataproc-spark-logs
# --properties="yarn:yarn.log-aggregation-enable=true,yarn:yarn.nodemanager.remote-app-log-dir=gs://${BUCKET}/logs,yarn:yarn.log-aggregation.retain-seconds=-1" \
# added teh YARN bit, this seems to put the logs on bitbucket even with some permision errors.
