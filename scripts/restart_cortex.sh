#!/bin/bash

for container in cortex-apollo cortex-cassandra cortex-elasticsearch neuron nlp registrar audrey-chat; do
    docker start $container
    if [[ $container == "cortex-cassandra" ]]; then
        sleep 10
    fi
done
