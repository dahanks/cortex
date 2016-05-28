#!/bin/bash
set -e

#Cassandra
pushd cassandra/
make && make run
popd

#ElasticSearch
pushd elasticsearch/
make && make run
popd

#Apollo
pushd apollo/
make && make run
popd

#Maven
#pushd maven/
#make
#popd

#Titan
pushd titan/
make
popd

#Neuron
sleep 10 #wait for cassandra, just in case
pushd neuron/
make && make run
popd

#Wetware-Py
pushd wetware-py/
make
popd

#Wetware
pushd wetware-py/modules/nlp
make && make run
popd

#Audrey Chat
pushd audrey-chat/
make && make run
popd
