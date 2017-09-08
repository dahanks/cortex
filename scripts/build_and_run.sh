#!/bin/bash

#Cassandra
pushd ../cassandra/
make && make run
popd

#ElasticSearch
pushd ../elasticsearch/
make && make run
popd

#Apollo
pushd ../apollo/
make && make run
popd

#Maven
#pushd ../maven/
#make
#popd

#Titan
pushd ../titan/
make
popd

#Neuron
sleep 10 #wait for cassandra, just in case
pushd ../neuron/
make && make run
popd

#Wetware-Py
pushd ../wetware-py/
make
popd

#Wetware NLP for Audrey-Chat
# pushd ../wetware-py/modules/nlp
# make && make run
# popd

#Wetware Registrar for Apollo users
# pushd ../wetware-py/modules/apollo_registrar
# make && make run
# popd

# ake-bridge
pushd ../wetware-py/modules/ake-bridge
make && make run
popd

#Audrey Chat
# pushd ../audrey-chat/
# make && make run
# popd
