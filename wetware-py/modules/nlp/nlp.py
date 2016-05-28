#!/usr/bin/env python

import logging
import json
import sys
import base64

from wetware.worker import Worker
from wetware.worker import WetwareException
from wetware.worker import FrameException

from wetware.neuron import Statements
from wetware.neuron import Responses
from wetware.neuron import get_vertex_object

class WetwareWorker(Worker):

    def __init__(self, subclass_section):
        super(WetwareWorker, self).__init__(subclass_section)

    def on_message(self, frame):
        ### This header must not be modified ###
        transaction = super(WetwareWorker, self).on_message(frame)
        message = json.loads(frame.body)
        ############## End header ##############

        if frame.headers['destination'] == self.args['input_topic']:
            self.process_nlp_statement(message, transaction)

    def process_nlp_statement(self, message, transaction):
        statements = message['statements']
        if len(statements) > 1 and transaction:
            self.reply(Statements("I can't possibly respond to all that at once!"), transaction)
        else:
            #We'll either have one statement and a transaction,
            # or multiple statements without expecting a reply
            for statement in statements:
                if '?' in statement:
                    self.parse_interrogative_statement(statement, transaction)
                else:
                    self.parse_indicative_statement(statement, transaction)

    def parse_interrogative_statement(self, statement, transaction):
        try:
            words = statement.split(' ')
            statements = None
            context = None
            if words[0] == 'Does':
                statements = self.parse_question_does(words)
                callback = self.interpret_audrey_does
            elif words[0] == 'Where':
                statements, context = self.parse_question_where(words)
                callback = self.interpret_audrey_where
            elif words[0] == 'Is':
                statements = self.parse_question_is(words)
                callback = self.interpret_audrey_is
            elif words[0] == 'What':
                statements = self.parse_question_what_is_the(words)
                callback = self.interpret_audrey_what_is_the
            elif words[0] == 'Who':
                statements = self.parse_question_who_is(words)
                callback = self.interpret_audrey_who_is
            elif len(words) == 3:
                statements = self.parse_question_generic_edge(words)
                callback = self.interpret_audrey_generic_edge
            elif transaction:
                self.reply(Statements("I'm terribly sorry, but I don't understand the question."), transaction)
                return

            if statements and transaction:
                self.publish(statements, callback=callback, context=context, transaction=transaction)
            elif statements:
                self.publish(statements)
        except:
            logging.exception("Caught Exception:")
            if transaction:
                self.reply(Statements("I'm terribly sorry, but I don't understand the question."), transaction)

    def parse_question_does(self, words):
        does = words[0] #will disregard this
        subj = words[1].strip()
        pred = words[2].strip() + 's' #add back indicative verb conj 's'
        obj = words[3].strip()[:-1] #take off the question mark
        gremlins = [
            'g.V().has("name","' + subj + '").both("' + pred + '").has("name","' + obj + '")',
            'g.V().has("name","' + subj + '").both("' + pred + '").both("' + pred + '").simplePath().has("name","' + obj + '")',
            'g.V().has("name","' + subj + '").both("' + pred + '").both("' + pred + '").both("' + pred + '").simplePath().has("name","' + obj + '")'
        ]
        statements = Statements()
        statements.gremlin(*gremlins)
        return statements

    def parse_question_generic_edge(self, words):
        subj = words[0].strip()
        pred = words[1].strip()
        obj = words[2].strip()[:-1] #take off the question mark
        gremlin = 'g.V().has("name","' + subj + '").out("' + pred + '").has("name","' + obj + '")'
        statements = Statements()
        statements.gremlin(gremlin)
        return statements

    def parse_question_is(self, words):
        is_word = words[0] #will disregard this
        subj = words[1].strip()
        key = words[2].strip()[:-1]
        statements = Statements()
        statements.get_vertex_property(subj, key)
        return statements

    def parse_question_what_is_the(self, words):
        #0,1,2 What is the
        key = words[3].strip()
        #4 of
        obj = words[5].strip()[:-1]
        statements = Statements()
        statements.get_vertex_property(obj, key)
        return statements

    def parse_question_where(self, words):
        #0,1 Where is
        subj = words[2].strip()[:-1]
        statements = Statements()
        statements.get_vertex_property(subj, "location")
        context = {'subj': subj}
        return statements, context

    def parse_question_who_is(self, words):
        #0,1 Who is
        subj = words[2].strip()[:-1]
        return get_vertex_object(subj)

    def parse_indicative_statement(self, statement, transaction):
        try:
            words = statement.split(' ')
            statements = None
            if words[0] == 'The' and words[2] == 'of' and words[4] == 'is':
                statements = self.parse_sentence_the_of_is(words)
            elif len(words) == 3:
                statements = self.parse_sentence_predicate(words)
            elif transaction:
                self.reply(Statements("I'm having trouble understanding what it is you want to say..."), transaction)
                return

            if statements and transaction:
                self.publish(statements, callback=self.acknowledge_response, transaction=transaction)
            elif statements:
                self.publish(statements)
        except:
            logging.exception("Caught Exception:")
            if transaction:
                self.reply(Statements("I'm having trouble understanding what it is you want to say..."), transaction)

    def parse_sentence_predicate(self, words):
        subj = words[0].strip()
        pred = words[1].strip()
        obj = words[2].strip()
        #take out the period
        if '.' in obj:
            obj = obj[:-1]
        statements = Statements()
        if pred == "is":
            statements.add_vertex_property(subj, obj, True)
        elif pred == "isn't":
            statements.add_vertex_property(subj, obj, False)
        else:
            #otherwise, add nodes and edge (add_edge adds nodes and edge)
            statements.add_edge(subj, pred, obj)
        return statements

    def parse_sentence_the_of_is(self, words):
        #0 The
        key = words[1].strip()
        #2 of
        subj = words[3].strip()
        #4 is
        if '"' in " ".join(words):
            value = " ".join(words).split('"')[1]
        else:
            value = words[5]
        #take out the period
        if '.' in value:
            value = value[:-1]
        statements = Statements()
        statements.add_vertex_property(subj, key, value)
        return statements

    def acknowledge_response(self, frame, context, transaction):
        responses = Responses(frame)
        for response in responses:
            if not response:
                reply = Statements("Hmm, apologies...I've...lost my train of thought...")
        reply = Statements("Alright, then.  I'll note that.")
        self.reply(reply, transaction)

    def interpret_audrey_is(self, frame, context, transaction):
        responses = Responses(frame)
        if responses[0] == 'true':
            reply = Statements("Yes, I do believe so.", "positive")
        elif responses[0] == 'false':
            reply = Statements("No, I don't believe that's true.", "negative")
        else:
            reply = Statements("Really, I haven't the faintest idea.")
        self.reply(reply, transaction)

    def interpret_audrey_where(self, frame, context, transaction):
        responses = Responses(frame)
        location = responses[0]
        if location:
            reply = Statements("Why, {0} is right at {1}.".format(context['subj'], location), "positive")
        else:
            reply = Statements("You know, I don't know where {0} is!".format(context['subj']), "negative")
        self.reply(reply, transaction)

    def interpret_audrey_what_is_the(self, frame, context, transaction):
        responses = Responses(frame)
        value = responses[0]
        if value:
            if value.startswith("base64:"):
                value = base64.b64decode(value.split(':')[1])
            reply = Statements("It appears to be {0}.".format(value), "positive")
        else:
            reply = Statements("You know, I'm just not sure.", "negative")
        self.reply(reply, transaction)

    def interpret_audrey_who_is(self, frame, context, transaction):
        responses = Responses(frame)
        value = responses.get_vertex_objects()
        if value:
            #we only look at the first because we don't support conjunctive
            # questions to Audrey chat
            vertex = value[0]
            if len(vertex) == 1:
                reply_str = "I just know the name {0}, and that's all.".format(vertex['name'])
            else:
                reply_str = "{0} ".format(vertex['name'])
                for prop in vertex:
                    if prop == 'name':
                        continue
                    elif vertex[prop] == "true":
                        reply_str += "is {0}; ".format(prop)
                    elif vertex[prop] == "false":
                        reply_str += "is not {0}; ".format(prop)
                    else:
                        reply_str += "has {0} {1}; ".format(vertex[prop], prop)
                reply_str += "and that's all I know."
            reply = Statements(reply_str)
        else:
           reply = Statements("I don't believe we know each other.")
        self.reply(reply, transaction)

    def interpret_audrey_does(self, frame, context, transaction):
        #interpret response
        #respond to UI
        # if first answer if yes, conf = 1.0
        #  if second answer is yes, conf = 0.5
        #  if third answer is yes, conf = 0.25
        responses = Responses(frame)
        if len(responses) == 3:
            #empty brackets is Gremlins empty string toList()
            if responses[0] != "[]":
                reply = Statements("Yes, most certainly.", "positive")
            elif responses[1] != "[]":
                reply = Statements("I think so, but I can't be sure.")
            elif responses[2] != "[]":
                reply = Statements("I suppose it's possible, but I doubt it.")
            else:
                reply = Statements("No, I don't believe so.", "negative")
        else:
            reply = Statements("I'm terribly sorry.  I'm not sure how to answer that.")
        self.reply(reply, transaction)

    def interpret_audrey_generic_edge(self, frame, context, transaction):
        #empty brackets is Gremlins empty string toList()
        responses = Responses(frame)
        if responses[0] != "[]":
            reply = Statements("Yes, most certainly.", "positive")
        else:
            reply = Statements("No, I don't believe so.", "negative")
        self.reply(reply, transaction)

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
