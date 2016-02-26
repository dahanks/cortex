#!/usr/bin/env python

import logging
import json
import sys

from wetware.worker import Worker
from wetware.worker import WetwareException
from wetware.worker import FrameException

class WetwareWorker(Worker):

    def __init__(self, subclass_section):
        super(WetwareWorker, self).__init__(subclass_section)

    def on_message(self, frame):
        try:
            ### This header must not be modified ###
            super(WetwareWorker, self).on_message(frame)
            message = json.loads(frame.body)
            ############## End header ##############

            #TODO throw out replies in the super class
            if frame.headers['destination'] == self.args['neuron_topic']:
                self.process_neuron_operation(message)
            elif frame.headers['destination'] == self.args['nlp_topic']:
                self.process_nlp_operation(message)
        except:
            self.reply({'responses': "I'm terribly sorry.  I'm feeling faint.  Perhaps I should see a doctor..."})
            logging.error(sys.exc_info()[0])
            logging.error(sys.exc_info()[1])

    def process_nlp_operation(self, message):
        for statement in message['statements']:
            if '?' not in statement:
                self.compose_indicative_nlp_statement(statement)
            else:
                self.compose_interrogative_nlp_statement(statement)

    def compose_interrogative_nlp_statement(self, statement):
        words = statement.split(' ')
        if words[0] == 'Does':
            self.compose_does_statement(words)
        elif words[0] == 'Where':
            self.compose_where_statement(words)
        else:
            self.reply({'responses': "I'm terribly sorry, but I don't understand the question."})

    def compose_does_statement(self, words):
        try:
            does = words[0] #will disregard this
            subj = words[1].strip()
            pred = words[2].strip() + 's' #add back indicative verb conj 's'
            obj = words[3].strip()[:-1] #take off the question mark
            output_data = {'statements': []}
            output_data['statements'].append(self.compose_gremlin_statement('g.V().has("name","' + subj + '").both("' + pred + '").has("name","' + obj + '")'))
            output_data['statements'].append(self.compose_gremlin_statement('g.V().has("name","' + subj + '").both("' + pred + '").both("' + pred + '").simplePath().has("name","' + obj + '")'))
            output_data['statements'].append(self.compose_gremlin_statement('g.V().has("name","' + subj + '").both("' + pred + '").both("' + pred + '").both("' + pred + '").simplePath().has("name","' + obj + '")'))
            self.publish(output_data, expect_reply=True, callback=self.interpret_audrey_response)
        except AttributeError:
            self.reply({'responses': "I'm terribly sorry.  I'm feeling faint.  Perhaps I should see a doctor..."})
            logging.error(sys.exc_info()[0])
            logging.error(sys.exc_info()[1])
        except Exception:
            self.reply({'responses': "I'm terribly sorry, but I don't understand the question."})

    def compose_where_statement(self, words):
        try:
            where_is = words[0], words[1] #will disregard this
            obj = words[3].strip()[:-1] #take off the question mark
            output_data = {'statements': []}
            output_data['statements'].append(self.compose_gremlin_statement(
                'g.V().has("name","' + obj + '").value("location")'))
            self.publish(output_data, expect_reply=True, callback=self.interpret_audrey_response)
        except Exception:
            self.reply({'responses': "I'm terribly sorry, but I don't understand the question."})

    def compose_indicative_nlp_statement(self, statement):
        #we'll consider this is indicative statement
        words = statement.split(' ')
        output_data = {'statements': []}
        try:
            subj = words[0].strip()
            pred = words[1].strip()
            obj = words[2].strip()
            #take out the period
            if '.' in obj:
                obj = obj[:-1]
            #adding an edge adds the vertices, too
            output_data['statements'].append(self.add_edge_statement(subj, obj, pred))
            self.publish(output_data, expect_reply=True, callback=self.acknowledge_response)
        except WetwareException:
            self.reply({'responses': "I'm having trouble understanding what it is you want to say..."})

    def add_vertex_statement(self, *names):
        statement = {'fxns': [], 'api': 'neuron'}
        for name in names:
            fxn = {'fxn': 'addVertex', 'name': name}
            statement['fxns'].append(fxn)
        return statement

    def add_edge_statement(self, from_vertex, to_vertex, label):
        statement = {'fxns': [], 'api': 'neuron'}
        fxn = {'fxn': 'addEdge',
               'fromVertex': from_vertex,
               'toVertex': to_vertex,
               'label': label }
        statement['fxns'].append(fxn)
        return statement

    def acknowledge_response(self, frame):
        #TODO: clean this up
        reply = {'responses': ""}
        try:
            responses = json.loads(frame.body)['responses']
            for response in responses:
                if not response:
                    reply['responses'] = "Hmm, apologies...I've...lost my train of thought..."
            reply['responses'] = "Alright, then.  I'll note that."
        except KeyError, ValueError:
            reply['responses'] = "Hmm, apologies...I've...lost my train of thought..."
        self.reply(reply)

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

    def process_neuron_operation(self, input_message):
        output_data = { 'statements': []}
        for raw_statement in input_message['statements']:
            if "addVertex" in raw_statement or 'addEdge' in raw_statement:
                output_data['statements'].append(self.compose_blueprints_statement(raw_statement))
            else:
                output_data['statements'].append(self.compose_gremlin_statement(raw_statement))
        logging.debug(output_data)
        self.publish(output_data, expect_reply=True)

    def compose_addedge_statement(self, raw_statement):
        #when neuron sees it's blueprints, it will check if fxn is addEdge
        # and handle this different api accordingly
        output_statement = {'fxns': []}
        addedge_statement = {}
        #format: Vertex.addEdge(label, Vertex, key, value, ... )
        # so, everything before .addEdge is the fromVertex Gremlin
        fromGremlin = raw_statement.split('.addEdge(')[0]
        # the label is the first comma-separated argument inside .addEdge()
        label = raw_statement.split('.addEdge(')[1].split(',')[0].strip()[1:-1]
        # properties are all strings from the end of the function ')'
        #  to the end of the toGremlin function, which also ends in ')'
        properties = []
        for prop in raw_statement.split(')')[-2].split(','):
            if prop:
                properties.append(prop.strip()[1:-1])
        # grab everything after 'label'
        #  start at g.V
        #  end at second instance (in reverse) of ')'
        toGremlinMess = raw_statement.split(label)[1]
        beginToGremlin = toGremlinMess.find('g.V')
        endToGremlin = toGremlinMess.rfind(')', 0, len(toGremlinMess) - 1) + 1
        toGremlin = toGremlinMess[beginToGremlin:endToGremlin]
        # wrap them with a bow
        addedge_statement['fxn'] = 'addEdge'
        addedge_statement['fromVertex'] = self.compose_standard_statement(fromGremlin)
        addedge_statement['toVertex'] = self.compose_standard_statement(toGremlin)
        addedge_statement['label'] = label
        addedge_statement['properties'] = properties
        output_statement['fxns'].append(addedge_statement)
        return output_statement

    def compose_blueprints_statement(self, raw_statement):
        #addEdge is a special case, addVertex is not (and don't expect others to be)
        if 'addEdge' in raw_statement:
            statement = self.compose_addedge_statement(raw_statement)
        else:
            statement = self.compose_standard_statement(raw_statement)
        statement['api'] = 'blueprints'
        return statement

    def compose_gremlin_statement(self, raw_statement):
        #no special cases for Gremlin
        statement = self.compose_standard_statement(raw_statement)
        statement['api'] = 'gremlin'
        return statement

    def interpret_audrey_response(self, frame):
        #interpret response
        #respond to UI
        # if first answer if yes, conf = 1.0
        #  if second answer is yes, conf = 0.5
        #  if third answer is yes, conf = 0.25
        reply = {}
        try:
            responses = json.loads(frame.body)['responses']
            if len(responses) == 1:
                reply['responses'] = "Alright, then.  I'll note that."
            elif len(responses) == 3:
                if "OK" in responses:
                    reply['responses'] = "Alright, then.  I'll note that."
                elif responses[0]:
                    reply['responses'] = "Yes, most certainly."
                elif responses[1]:
                    reply['responses'] = "I think so, but I can't be sure."
                elif responses[2]:
                    reply['responses'] = "I suppose it's possible, but I doubt it."
                else:
                    reply['responses'] = "No, I don't believe so."
            else:
                reply['responses'] = "I'm terribly sorry.  I'm not sure how to answer that."
        except KeyError, ValueError:
            reply['responses'] = "Hmm, apologies...I've...lost my train of thought..."
        self.reply(reply)

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

        if frame.headers['destination'].startswith('/queue/temp'):
            for key in ['responses']:
                if key not in message:
                    raise FrameException("Message has no {0} field".format(key))
        else :
            for key in ['statements']:
                if key not in message:
                    raise FrameException("Message has no {0} field".format(key))

def main():
    logging.basicConfig(level=logging.DEBUG)
    wetware_worker = WetwareWorker("wetware")
    wetware_worker.run()

if __name__ == "__main__":
    main()
