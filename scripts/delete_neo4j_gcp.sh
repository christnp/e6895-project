#!/bin/bash
INSTANCE=${1:-my-neo4j}

echo "Deleting ${INSTANCE} compute instance"
gcloud compute instances delete $INSTANCE 
