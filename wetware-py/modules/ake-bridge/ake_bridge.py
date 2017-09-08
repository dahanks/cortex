#!/usr/bin/env python

import logging
import json

from wetware.worker import Worker
from wetware.neuron import Statements, Responses

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
        self.publish(statements)

    def run_setup(self):
        pass
        #self.query_for_ake()

    def query_for_ake(self):
        statements = Statements()
        statements.gremlin("g.V().valueMap()")
        self.publish(statements, callback=self.print_results)

    def print_results(self, frame, context, transaction):
        responses = Responses(frame)
        vertices = responses.get_vertex_objects()
        logging.info(vertices)
        import sys
        sys.exit(1)

def main():
    logging.basicConfig(level=logging.DEBUG)
    wetware_worker = WetwareWorker("wetware")
    wetware_worker.run()

if __name__ == "__main__":
    main()
