#!/bin/bash

# Cluster Name
read -p "What Maven project are you building [enter for default]? " -i "../e6895-maven" -e PROJECT

# Scala Jar
read -p "What is the HCFS URI for the driver jar [enter for default]? " -i "personality-project_0.1-0.0.4-jar-with-dependencies.jar" -e JAR
[[ -z "$JAR" ]] && JAR="personality-project_0.1-0.0.4-jar-with-dependencies.jar"

# GCP Bucket
read -p "What bucket to use for this cluster [enter for default]? " -i "eecs-e6895-bucket" -e BUCKET
[[ -z "$BUCKET" ]] && BUCKET="eecs-e6895-bucket"

# Start the cluster
echo ""
echo "Project: $PROJECT"
echo "Jar: $JAR"
echo "GCP Bucket: $BUCKET"

# get current location (not sure this is needed)
# CUR_DIR=pwd
# change focus to project directory to run the following commands
cd $PROJECT
# turn bash echo on

# clean build the project
echo -e "\nBuilding a clean project..."
set -x
mvn clean install
set +x

# assemble uberJar
echo -e "\nAssembling the uberJar with dependencies..."
set -x
mvn assembly:assembly -DdescriptorId=jar-with-dependencies
set +x

# copy target to Google Bucket
echo -e "\nCopying the target jar to \"jars\" directory in \"$BUCKET\" bucket dependencies..."
set -x
# gsutil cp $PROJECT/target/$JAR  gs://$BUCKET/jars
gsutil cp target/$JAR  gs://$BUCKET/jars
set +x

# change back to the starting directory (not sure this is needed)
# cd $CUR_DIR
