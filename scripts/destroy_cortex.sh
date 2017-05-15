#!/bin/bash

for container in audrey-chat registrar nlp neuron cortex-apollo cortex-cassandra cortex-elasticsearch; do
    docker rm -f $container
done
