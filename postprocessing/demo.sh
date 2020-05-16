#!/bin/bash
DATA_DIR="/home/christnp/Development/e6895/data/notrack_MultiLayerAnalysis/demo"
NEO4J_DIR="/home/christnp/Development/e6895/neo4j/instances/neo4j/import/demo/"
CODE_DIR="/home/christnp/Development/e6895/postprocessing"
PROC_DIR="/home/christnp/Development/e6895/processing"
CYPHER_DIR="file:///demo" # defines teh first part of the node/edge path, i,e., file:///demo/
NODE_TAIL="_raw_vert.csv" # defines the last part of the node path, i.e., "_xyz_123.csv"
EDGE_TAIL="_raw_edges.csv" # ditto

BUCKET="eecs-e6895-bucket"

CLUSTER=analysis
CLASS=ComplexNetwork
JAR="gs://eecs-e6895-bucket/jars/personality-project_0.1-0.0.4.jar"
SIZE=15
RANK=4
TOL=0.002
MAXITER=25
OUTPUT="demo"

# run ComplexNetwork.scala
echo ""
echo "Running '$CLASS' on Dataproc cluster '$CLUSTER' with the following parameters:"
echo " Node Count:  $SIZE"
echo " CPALS Rank:  $RANK"
echo " CPALS Tol.:  $TOL"
echo " CPALS Iter.: $MAXITER"
echo " Output Path: $OUTPUT"

set -x # show command
# echo "testing gcloud"
echo -e "\nSubmitting the Spark job...\n"
gcloud dataproc jobs submit spark \
    --cluster=$CLUSTER \
    --class=$CLASS \
    --region=us-west1 \
    --jars="$JAR,gs://spark-lib/bigquery/spark-bigquery-latest.jar" \
    --properties="cloud.profiler.enable=true,cloud.profiler.name=ComplexNetwork,cloud.profiler.service.version=demo" \
    --properties="spark.jars.packages=graphframes:graphframes:0.8.0-spark2.4-s_2.11" \
    -- $SIZE $RANK $TOL $MAXITER $OUTPUT

set +x # turn off

# read -p "Spark job has completed. [ENTER to coninue, ctrl+c to exit]" -i "" -e 
read -n1 -rsp "Spark job has completed. Press enter to continue..." CONTINUE

echo -e "\nPreparing the new data for visualization...\n"
# copy files from bucket to neo4j import and rename them
gsutil -m cp -r gs://eecs-e6895-bucket/output/_graphframes/$OUTPUT/* $NEO4J_DIR 
python3 $CODE_DIR/rename_bucket_files.py -i $NEO4J_DIR
python3 $PROC_DIR/neo4j_loader.py -i $CYPHER_DIR


echo -e "\nPreparing the new data for analysis...\n"
# copy files from bucket to data analysis directory
gsutil -m cp -r gs://eecs-e6895-bucket/output/_multilayer/$OUTPUT/* $DATA_DIR

# rename files for postprocessing
python3 $CODE_DIR/rename_bucket_files.py -i $DATA_DIR

# run MultilayerAnalysis
echo -e "\nRunning the multilayer analysis...\n"
set -x # show command
python3 $CODE_DIR/multilayer_analysis.py -i $DATA_DIR -g $NEO4J_DIR -v
set +x # turn off

echo -e "\nComplete... ready for visualization!\n"

