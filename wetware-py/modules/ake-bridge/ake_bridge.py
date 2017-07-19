#!/usr/bin/env python

import logging
import json

from wetware.worker import Worker
from wetware.neuron import Statements

class WetwareWorker(Worker):

    def __init__(self, subclass_section):
        super(WetwareWorker, self).__init__(subclass_section)

    def on_message(self, frame):
        ### This header must not be modified ###
        transaction = super(WetwareWorker, self).on_message(frame)
        message = json.loads(frame.body)
        ############## End header ##############

        if frame.headers['destination'] == self.args['input_topic']:
            try:
                self.process_ake_json(message, transaction)
            except KeyError:
                logging.error("Received invalid TSPI JSON structure")

    def process_ake_json(self, message, transaction):
        if message['Ontology']['Name'] != "TSPI":
            logging.warning("Received ontology other than TSPI. Ignoring")
            return
        statements = Statements()
        for node in message['Nodes']:
            name = node.keys()[0]
            vertex = node[name]
            vertex['name'] = name
            vertex['rules'] = str(vertex['rules'])
            if vertex['children']:
                for child in vertex['children']:
                    statements.add_edge(name, "precedes", child)
            del vertex['children']
            del vertex['parents']
            statements.add_vertex(vertex)
        logging.error(statements)
        self.publish(statements)

def main():
    logging.basicConfig(level=logging.DEBUG)
    wetware_worker = WetwareWorker("wetware")
    wetware_worker.run()

if __name__ == "__main__":
    main()
