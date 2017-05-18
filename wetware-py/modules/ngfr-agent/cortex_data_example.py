#!/usr/bin/env python

import logging
import json
import sys
import time
import base64

from wetware.worker import Worker
from wetware.worker import WetwareException
from wetware.worker import FrameException
from wetware.worker import ApolloConnection

from wetware.neuron import Statements
from wetware.neuron import Responses
from wetware.neuron import get_vertex_object

class WetwareWorker(Worker):

    def __init__(self, subclass_section):
        super(WetwareWorker, self).__init__(subclass_section)
        
    #override base run
    def run_setup(self):
        self.insert_data()
            
            #    time.sleep(15)
            
        self.submit_query()
            
            #time.sleep(15)
        
    def submit_query(self):
        try:
            subj = 'George'
            pred = 'write'
            obj = 'novel'
            
            # create statements for checking 
            #gremlin = [ 'g.V().has("name","' + subj + '").both("' + pred + '").has("name","' + obj + '")' ]
            gremlin = [ "g.V().has('name','George').out('read').has('name','book')" ]
            
            statements = Statements()
            statements.gremlin(*gremlin)

            self.publish(statements, callback=self.result_callback)
            print '???'
        except:
            logging.exception("Something went wrong")

    def result_callback(self, frame, context, transaction):
        responses = Responses(frame)
        print '---'
        logging.debug(responses)
    
        
    def insert_data(self):
        try:
            #subj = 'George'
            #pred = 'read'
            #obj = 'book'
            
            #statements = Statements()
            #statements.add_edge(subj, pred, obj) # add nodes and edge (add_edge adds nodes and edge)
            #self.publish(statements, callback=self.result_callback)
            print '!!!'
        except:
            logging.exception("Something went wrong")

    def define_default_args(self):
        ### This header must not be modified ###
        defaults = super(WetwareWorker, self).define_default_args()
        ############## End header ##############

        #defaults['my_parameter'] = "my_default_parameter_value"

        ### This footer must not be modified ###
        return defaults
        ############## End footer ##############

    def add_argparse_args(self, parser):
        ### This headermust not be modified ###
        super(WetwareWorker, self).add_argparse_args(parser)
        ############## End header ##############

        #parser.add_argument("--my_parameter", "-m", dest="my_parameter",
        #                    help="this is my new, special parameter")

    def verify_frame(self, frame):
        ### This header must not be modified ###
        super(WetwareWorker, self).verify_frame(frame)
        message = json.loads(frame.body)
        ############## End header ##############

        for key in ['statements']:
            if key not in message:
                raise FrameException("Message has no {0} field".format(key))

def main():
    logging.basicConfig(level=logging.DEBUG)
    wetware_worker = WetwareWorker("wetware")
    wetware_worker.run()

if __name__ == "__main__":
    main()
