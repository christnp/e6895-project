#!/bin/bash
INSTANCE=${1:-my-neo4j}

gcloud compute instances list

echo "Starting $INSTANCE..."
gcloud compute instances start $INSTANCE
