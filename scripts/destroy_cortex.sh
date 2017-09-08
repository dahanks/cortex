#!/bin/bash

for container in audrey-chat registrar nlp ake-bridge neuron cortex-apollo cortex-cassandra cortex-elasticsearch; do
    docker rm -f $container
done
