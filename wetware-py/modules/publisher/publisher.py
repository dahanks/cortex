#!/usr/bin/env python

import logging
import json
import time

from wetware.worker import Worker
from wetware.worker import ApolloConnection
from wetware.neuron import Statements

class WetwareWorker(Worker):

    def run_setup(self):
        self.send_neuron_statements()
        #self.send_nlp_statements()

    def send_neuron_statements(self):
        #Add vertex by string 'name'
        statements = Statements()
        statements.add_vertex("David")
        self.publish(statements)

        time.sleep(1)

        #Add vertex by dict with properties
        george = {'name': "George", 'age': 30, 'height': 1.6}
        statements = Statements()
        statements.add_vertex(george)
        self.publish(statements)

        time.sleep(1)

        #Add multiple vertices at once
        statements = Statements()
        statements.add_vertices("David", george)
        self.publish(statements)

        time.sleep(2)

        #Add an edge with properties (where one vertex is identified as a string
        # and the other by a dict)
        #Vertices are created, but not with properties!
        statements = Statements()
        statements.add_edge("David", "knows", george, {'rel': 0.4})
        self.publish(statements)

    def send_nlp_statements(self):
        statements = {"statements": ["David knows Ed.",
                                     "David is nice.",
                                     "The happiness of David is high.",
                                     "Does David know Ed?",
                                     "Is David nice?",
                                     "What is the happiness of David?",
                                 ]}
        self.publish(statements, topic='/queue/wetware.nlp')

def main():
    logging.basicConfig(level=logging.DEBUG)
    wetware_worker = WetwareWorker("wetware")
    wetware_worker.run()

if __name__ == "__main__":
    main()
