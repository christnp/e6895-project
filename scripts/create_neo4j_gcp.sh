#!/bin/bash
INSTANCE=${1:-my-neo4j}

echo "Setting firewall rules..."
gcloud compute firewall-rules create allow-neo4j-bolt-https \
   --allow tcp:7473,tcp:7687 \
   --source-ranges 0.0.0.0/0 \
   --target-tags neo4j

echo "Creating ${INSTANCE} compute instance"
gcloud compute instances create $INSTANCE \
   --scopes https://www.googleapis.com/auth/cloud-platform \
   --image-project launcher-public --tags neo4j \
   --image=neo4j-community-1-3-5-14-apoc
