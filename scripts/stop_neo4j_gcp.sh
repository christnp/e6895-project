#!/bin/bash

gcloud compute instances list

echo "Which instance would you like to stop?"

read INSTANCE

echo "Stopping $INSTANCE..."
gcloud compute instances stop $INSTANCE
