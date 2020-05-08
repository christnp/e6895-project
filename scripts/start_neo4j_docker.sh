#!/bin/bash

 docker ps -a --format "table {{.Names}}\t{{.Image}}\t{{.Status}}\t{{.ID}}"

# Instance Name
echo ""
read -p "What neo4j instance (name) would you like to start [enter to exit]? " -e INSTANCE

# check if $INSTANCE variable is empty; if it is, exit
[[ -z "$INSTANCE" ]] && { echo "No instance selected, exiting now."; exit 1; }

# Stop the container
echo ""
echo "Starting docker container '$INSTANCE'"

docker start $INSTANCE