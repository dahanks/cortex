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
            except KeyError, e:
                logging.error("Received invalid TSPI JSON structure")
                logging.error(e)
                raise

    def process_ake_json(self, json_data, transaction):
        if json_data['Ontology']['Name'] != "TSPI":
            logging.warning("Received ontology other than TSPI. Ignoring")
            return

        logging.debug(json_data)
        
        statements = Statements()
        for node in json_data['Nodes']:
            for name in node:
                statements.add_vertex(name)
                statements.add_vertex_property(name,'index',"%s"%node[name]['index'])
                statements.add_vertex_property(name,'type',node[name]['type'])
                if node[name]['rules']!=None:
                    for prop in node[name]['rules']:
                        statements.add_vertex_property(name,prop,node[name]['rules'][prop])
        for edge in json_data['Links']:
            for name in edge:
                statements.add_edge(edge[name]['source'],'next',edge[name]['target'],edge[name]['rules'])
        print "publishing the script"
        self.publish(statements, callback=self.result_callback)

    def print_results(self, frame, context, transaction):
        responses = Responses(frame)
        logging.debug(responses)

def main():
    logging.basicConfig(level=logging.DEBUG)
    wetware_worker = WetwareWorker("wetware")
    wetware_worker.run()

if __name__ == "__main__":
    main()
