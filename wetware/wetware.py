#!/usr/bin/env python

import logging
import json

from ccd.worker import Worker
from ccd.worker import WorkerException
from ccd.worker import FrameException

class WetwareWorker(Worker):

    def __init__(self, subclass_section):
        super(WetwareWorker, self).__init__(subclass_section)

    def on_message(self, frame):
        ### This header must not be modified ###
        super(WetwareWorker, self).on_message(frame)
        message = json.loads(frame.body)
        ############## End header ##############

        #TODO: do you want to let them mix statements later?
        if frame.headers['destination'] == self.args['neuron_topic']:
            self.publish_neuron_operation(message)
        else:
            raise WetwareException("Message came on unhandled topic")

    def compose_addEdge_statement(self, statement):
        #format: Vertex.addEdge(label, Vertex, key, value, ... )
        output_statement = {'fxns': []}
        fxn = "addEdge"
        fromGremlin = statement.split('addEdge')[0]

    def compose_standard_statement(self, statement):
            output_statement = {'fxns': []}
            for input_function in statement.split('.'):
                #these are the Tinker/TitanGraphs or traversal()
                # their usage is hardcoded at neuron
                if input_function == 'graph' or input_function == 'g':
                    continue
                else:
                    function_name = input_function.split('(')[0]
                    arguments = []
                    for argument in input_function.split('(')[1].split(')')[0].split(','):
                        #don't put in empty strings
                        if argument:
                            #strip whitespace, and strip quotes (on each side)
                            # there would be a problem here is quotes weren't there
                            arguments.append(argument.strip()[1:-1])
                    output_function = {
                        'fxn': function_name,
                        'args': arguments
                    }
                    output_statement['fxns'].append(output_function)
            return output_statement

    def publish_neuron_operation(self, input_message):
        output_data = { 'statements': []}
        for raw_statement in input_message['statements']:
            if "addVertex" in raw_statement or 'addEdge' in raw_statement:
                output_data['statements'].append(self.compose_blueprints_statement(raw_statement))
            else:
                output_data['statements'].append(self.compose_gremlin_statement(raw_statement))
        logging.debug(output_data)
        self.publish(output_data)

    def compose_blueprints_statement(self, raw_statement):
        #addEdge is a special case, addVertex is not (and don't expect others to be)
        if 'addEdge' in raw_statement:
            pass
        else:
            statement = self.compose_standard_statement(raw_statement)
        statement['api'] = 'blueprints'
        return statement

    def compose_gremlin_statement(self, raw_statement):
        #no special cases for Gremlin
        statement = self.compose_standard_statement(raw_statement)
        statement['api'] = 'gremlin'
        return statement

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

class WetwareException(WorkerException):
    pass

def main():
    logging.basicConfig(level=logging.DEBUG)
    wetware_worker = WetwareWorker("wetware")
    wetware_worker.run()

if __name__ == "__main__":
    main()
