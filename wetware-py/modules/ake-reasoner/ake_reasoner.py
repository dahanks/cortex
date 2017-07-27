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
                self.performScript(message['procedure'])
            except KeyError:
                logging.exception("Something went wrong")

    def performScript(self,name):
        ''' Get the named node's execution data (if any) and the names of its child nodes '''
        statements = Statements()
        statements.get_vertex_property(name,'Execute')
        statements.get_vertex_property(name,'Param')
        statements.gremlin('g.V().has("name","'+name+'").out("child").values("name")')
        self.publish(statements, callback=self.perform_callback)

    def perform_callback(self, frame, context, transaction):
        ''' Take the returned value of the performScript query, call the python script (if any), and then move on to querying the children. '''
        responses = Responses(frame)
        logging.debug(responses)
        if responses[0]!=u'':
            call(['python', responses[0]]+responses[1].split())
        if responses[2]!=u'[]':
            for r in responses[2][1:-1].split(", "):
                self.performScript(r)

def main():
    logging.basicConfig(level=logging.DEBUG)
    wetware_worker = WetwareWorker("wetware")
    wetware_worker.run()

if __name__ == "__main__":
    main()
