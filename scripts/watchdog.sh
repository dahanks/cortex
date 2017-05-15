#!/bin/bash

LOG_DIR=~/down_container_logs
date=`date +%Y-%m-%d_%H-%M-%S`

mkdir -p $LOG_DIR

for container in 'cortex-apollo' 'cortex-cassandra' 'cortex-elasticsearch' 'neuron' 'nlp' 'audrey-chat'; do
    docker ps | grep $container
    if [[ $? != 0 ]]; then
        echo "$container is DOWN!"
        docker logs $container &> $LOG_DIR/$date.$container.fail.log
        docker start $container
        #let each container start up nice and good
        sleep 20
    fi
done
