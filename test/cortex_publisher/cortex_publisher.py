#!/usr/bin/env python

import logging
import json
import sys
import time

from wetware.worker import Worker
from wetware.worker import ApolloConnection

class PublisherWorker(Worker):

    def run(self):
        with ApolloConnection(self.args) as self.apollo_conn:
#            self.sendStatements()
            self.sendNLP()

    def sendNLP(self):
        data = { 'statements': [
#            "Mark knows Zack",
#            "Mark knows George",
            "Does George know Zack?",
#            "Does David know Lorna?",
        ]}
        self.publish(data, '/topic/wetware.nlp', expect_reply=self.args['expect_reply'])

    def sendStatements(self):
        data = { 'statements': [
            'g.V().toList()',
            'g.E().toList()',
#            'graph.addVertex("name","other1", "isfirst", "yes")',
#            'graph.addVertex("name","other2", "isfirst", "no")',
#            'g.V().has("name").properties().toList()'
#            'g.V().has("name","other1").next().addEdge("goesTo", g.V().has("name","other2").next(), "iscooledge", "totally", "isreallycool", "absolutely")',
#            'g.V().has("name","other1").next().addEdge("goesTo", g.V().has("name","other2").next())',
#            'graph.addEdge("comesBefore","name","first", "name","last", "iscool", "yes")',
#            'g.V.addEdge("comesBefore","name","first", "name","last", "iscool", "yes")',
        ] }
        self.publish(data, '/topic/wetware.neuron', expect_reply=self.args['expect_reply'])

    def neuronAddVertex(self):
        data = { 'api': 'blueprints',
                 'statements': [
                     {
                         'fxns': [
                             {
                                 'fxn': 'addVertex',
                                 'args': ['name', 'david']
                             }
                         ]
                     },
                     {
                         'fxns': [
                             {
                                 'fxn': 'addVertex',
                                 'args': ['name', 'lorna']
                             }
                         ]
                     }
                 ]
             }
        self.publish(data, '/queue/neuron.operation', expect_reply=self.args['expect_reply'])

    def neuronAddEdge(self):
        data = { 'api': 'blueprints',
                 'statements': [
                     {
                         'fxns': [
                             {
                                 'fxn': 'addVertex',
                                 #this is a statement
                                 'fromVertex': {
                                     'fxns': [
                                         {
                                             'fxn': 'V',
                                             'args': []
                                         },
                                         {
                                             #not actually completed...just for show
                                         },
                                     ]
                                 },
                                 'args': ['name', 'david']
                             }
                         ]
                     },
                 ]
             }
        self.publish(data, '/queue/neuron.operation', expect_reply=self.args['expect_reply'])

    def handle_reply(self, frame):
        print json.dumps(json.loads(frame.body), sort_keys=True, indent=4, separators=(',', ': '))
        print "Got my reply...exiting."
        sys.exit(1)

def main():
    logging.basicConfig(level=logging.DEBUG)
    publisher = PublisherWorker("cortexpublisher")
    publisher.run()

if __name__ == "__main__":
    main()
