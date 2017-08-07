#!/usr/bin/env python

import json
import logging
import subprocess as sub

from subprocess import call
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
                logging.debug(message['procedure'])
                self.performScript(message['procedure'])
            except KeyError:
                logging.exception("Something went wrong")

    #def performScript(self,name):
    #    ''' Get the named node's execution data (if any) and the names of its child nodes '''
    #    statements = Statements()
    #    statements.get_vertex_property(name,'Execute')
    #    statements.get_vertex_property(name,'Param')
    #    statements.gremlin('g.V().has("name","'+name+'").out("child").values("name")')
    #    self.publish(statements, callback=self.perform_callback)
    
    def performScript(self,name):
        ''' Get the named node's execution data (if any) and the names of its child nodes '''
        statements = Statements()
        statements.gremlin('g.V().has("name","'+name+'").valueMap()')
        self.publish(statements, callback=self.perform_callback)

    #def perform_callback(self, frame, context, transaction):
    #    ''' Take the returned value of the performScript query, call the python script (if any), and then move on to querying the children. '''
    #    responses = Responses(frame)
    #    logging.debug(responses)
    #    logging.debug(responses[0])
    #    logging.debug(responses[1])
    #    if responses[0]!=u'':
    #        call(['python', responses[0]]+responses[1].split())
    #    if responses[2]!=u'[]':
    #        for r in responses[2][1:-1].split(", "):
    #            self.performScript(r)
    
    def perform_callback(self, frame, context, transaction):
        ''' Take the returned value of the performScript query, call the python script (if any), and then move on to querying the children. '''
        responses = Responses(frame)
        print responses
        nodes = responses.get_vertex_objects()
        for n in nodes:
            print n
            label = 'next'
            if 'Execute' in n:
                p = (sub.Popen(['python', n['Execute']],stdout=sub.PIPE,stderr=sub.PIPE) if 'Param' not in n else
                     sub.Popen(['python', n['Execute'], n['Param']],stdout=sub.PIPE,stderr=sub.PIPE))
                output, errors = p.communicate()
                if len(output)>0: output = output[:-1] # strip off the thrailing CR
                label = label+output
                print ">>%s<<"%output
            statements = Statements()
            statements.gremlin('g.V().has("name","'+n['name']+'").out("'+label+'").valueMap()')
            self.publish(statements, callback=self.perform_callback)

def main():
    logging.basicConfig(level=logging.DEBUG)
    wetware_worker = WetwareWorker("wetware")
    wetware_worker.run()

if __name__ == "__main__":
    main()
