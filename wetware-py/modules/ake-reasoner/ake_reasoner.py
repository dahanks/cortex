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

        self.parameters = {}
        self.lastParam = ''

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
    
    def performScript(self,name):
        ''' Get the named node's execution data (if any) and the names of its child nodes '''
        statements = Statements()
        statements.gremlin('g.V().has("name","'+name+'").valueMap()')
        print "starting script execution"
        self.publish(statements, callback=self.perform_callback)

    def perform_callback(self, frame, context, transaction):
        ''' Take the returned value of the performScript query, call the python script (if any), and then move on to querying the children. '''
        responses = Responses(frame)
        print responses
        nodes = responses.get_vertex_objects()
        for n in nodes:
            print n
            statements = Statements()
            returnValue = ''
            output = ''
            param = ''
            if 'Execute' in n:
                parameter = ''
                if 'Param' in n and n['Param'] in self.parameters:
                    parameter = self.parameters[n['Param']]
                    print "Parameter [%s] found: [%s]" % (n['Param'], parameter)
                
                p = (sub.Popen(['python', n['Execute']], stdout=sub.PIPE, stderr=sub.PIPE) if 'Param' not in n else
                     sub.Popen(['python', n['Execute'], parameter], stdout=sub.PIPE, stderr=sub.PIPE))
                returnValue, errors = p.communicate()

                if len(returnValue)>0: returnValue = returnValue.strip()
                print "Return Value: >>%s<<"%returnValue

                # split the return value here
                splitReturnValue = returnValue.split(',')
                if len(splitReturnValue) == 2:
                    output = splitReturnValue[0]
                    param = splitReturnValue[1]
                    if len(output)>0: output = output.strip()
                    if len(param)>0: param = param.strip()
                    self.lastParam = param
                else:
                    if len(splitReturnValue) == 1:
                        output = splitReturnValue[0]
                        if len(output)>0: output = output.strip()

                print "Output: >>%s<<"%output
                print "Last Known Param: >>%s<<"%self.lastParam

            if 'Param' in n and n['type'] == 'Parameter':
                self.parameters[n['Param']] = self.lastParam
                print "Set Parameter [%s] to [%s]" % (n['Param'], self.lastParam)
                
            if 'Execute' in n and n['type'] == 'Execute_Branch':
                statements.gremlin('g.V().has("name","'+n['name']+'").outE("next").has("Conditional","'+output+'").inV().valueMap()')
            else:
                statements.gremlin('g.V().has("name","'+n['name']+'").out("next").valueMap()')
                
            self.publish(statements, callback=self.perform_callback)

def main():
    logging.basicConfig(level=logging.DEBUG)
    wetware_worker = WetwareWorker("wetware")
    wetware_worker.run()

if __name__ == "__main__":
    main()
