#!/bin/bash

# master name
NAME=${1}

# start docker container 
sudo mkdir -p /docker_volume/master  
eval $(weave env)
sudo docker run  --name $NAME -h $NAME.weave.local $(weave dns-args)  -v /docker_volume/master:/hadoop/dfs/name -e CLUSTER_NAME=Parallel uhopper/hadoop-namenode

