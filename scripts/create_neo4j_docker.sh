#!/bin/bash

# Instance Name
read -p "What is the name of your neo4j instance [enter for default]? " -i "myneo4j" -e INSTANCE

# check if $INSTANCE variable is empty; if it is, make it default
[[ -z "$INSTANCE" ]] && INSTANCE=myneo4j

# Instance Version
# 1-3-5-14 is the lates GCP version, so let's work with that
read -p "What version of neo4j to use [enter for default]? " -i "3.5.14" -e VERSION
[[ -z "$VERSION" ]] && VERSION="3.5.14"

# TODO: import APOC in this script
# https://neo4j.com/docs/labs/apoc/3.4/introduction/#_using_apoc_with_the_neo4j_docker_image

# Instance Path
SCRIPT_PATH="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
read -p "Where is your neo4j instance going to run [enter for default]? " -i "$SCRIPT_PATH" -e INST_PATH

# check if $INST_PATH variable is empty; if it is, make it default
[[ -z "$INST_PATH" ]] && INST_PATH="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# Start the container
echo ""
echo "Name: $INSTANCE"
echo "Name: $VERSION"
echo "Path: $INST_PATH"

docker run \
    --name $INSTANCE \
    -p7474:7474 -p7687:7687 \
    -d \
    -v $INST_PATH/neo4j/data:/data \
    -v $INST_PATH/neo4j/logs:/logs \
    -v $INST_PATH/neo4j/import:/var/lib/neo4j/import \
    -v $INST_PATH/neo4j/plugins:/plugins \
    -v $INST_PATH/neo4j/conf:/conf \
    --env NEO4J_AUTH=neo4j/admin \
    neo4j:$VERSION

# Explanaiton of parameters
# Container name                -->  --name $INSTANCE \
# Expose HTTP and Bolt ports    -->  -p7474:7474 -p7687:7687 \
# Put container in background   -->  -d \
# Volume bind: auth for DBs     -->  -v $INST_PATH/neo4j/data:/data \
# Volume bind: logs for DBs     -->  -v $INST_PATH/neo4j/logs:/logs \
# Volume bind: import from here -->  -v $INST_PATH/neo4j/import:/var/lib/neo4j/import \
# Volume bind: algs, exts., etc -->  -v $INST_PATH/neo4j/plugins:/plugins \
# Volume bind: exponse config   -->  -v $INST_PATH/neo4j/conf:/conf \
# Sets the PW for user 'neo4j'  -->  --env NEO4J_AUTH=neo4j/admin \
# Sets the neo4j:version to use -->  neo4j:latest