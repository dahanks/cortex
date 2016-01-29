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
        if frame.headers['destination'] == self.args['blueprints_dest']:
            self.convert_to_blueprints(message)
        elif frame.headers['destination'] == self.args['gremlin_dest']:
            self.convert_to_gremlin(message)
        else:
            raise WetwareException("Statement came on unhandled topic")

    def compose_neuron_operation(self, input_message):
        output_data = { 'statements': []}
        for statement in input_message['statements']:
            output_statement = {'fxns': []}
            for input_function in statement.split('.'):
                if input_function == 'graph' or input_function == 'g':
                    continue
                else:
                    function_name = input_function.split('(')[0]
                    arguments = []
                    for argument in input_function.split('(')[1].split(')')[0].split(','):
                        #don't put in empty strings
                        if argument:
                            arguments.append(argument[1:-1])
                    output_function = {
                        'fxn': function_name,
                        'args': arguments
                    }
                    output_statement['fxns'].append(output_function)
            output_data['statements'].append(output_statement)
        return output_data

    def convert_to_blueprints(self, message):
        output_data = self.compose_neuron_operation(message)
        output_data['api'] = 'blueprints'
        print output_data
        self.publish(output_data)

    def convert_to_gremlin(self, message):
        output_data = self.compose_neuron_operation(message)
        output_data['api'] = 'gremlin'
        print output_data
        self.publish(output_data)

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
