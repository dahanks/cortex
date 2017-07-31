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
        statements.add_edge('TSPI_Container', 'child', 'foo')
        for node in message['Nodes']:
            for name in node:
                statements.add_vertex_property(name, 'index', "%s"%node[name]['index'])
                statements.add_vertex_property(name, 'type', node[name]['type'])
                if node[name]['rules']!=None:
                    for prop in node[name]['rules']:
                        statements.add_vertex_property(name, prop, node[name]['rules'][prop])
                if node[name]['children']!=None:
                    for c in node[name]['children']:
                        statements.add_edge(name, 'child', c)
                if node[name]['parents']!=None:
                    for c in node[name]['parents']:
                        statements.add_edge(name, 'parent', c)
        self.publish(statements, callback=self.result_callback)

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
