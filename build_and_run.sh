#!/bin/bash

#Cassandra
pushd cassandra/
make && make run
popd

#ElasticSearch
pushd elasticsearch/
make && make run
popd

#Maven
pushd maven/
make
popd

#Titan
pushd titan/
make
popd

#Neuron
pushd neuron/
make && make run
