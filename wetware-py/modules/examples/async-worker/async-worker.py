#!/usr/bin/env python

import logging
import json
import sys

import time

from wetware.worker import Worker
from wetware.worker import WetwareException
from wetware.worker import FrameException
from wetware.worker import ApolloConnection

from wetware.neuron import Statements

class WetwareWorker(Worker):

    def __init__(self, subclass_section):
        super(WetwareWorker, self).__init__(subclass_section)

    def on_message(self, frame):
        ### This header must not be modified ###
        transaction = super(WetwareWorker, self).on_message(frame)
        message = json.loads(frame.body)
        ############## End header ##############

        if frame.headers['destination'] == self.args['input_topic']:
            if float(message['secs']) == 1:
                # TEST SYNCRONOUS (tests reply() with no transaction)
                #time.sleep(1)
                #self.reply({'seconds': 'that was JUST one second'})
                # TEST ASYNCRONOUS (tests publish() with callback and transaction)
                self.publish(message, callback=self.after_one, transaction=transaction)
            elif float(message['secs']) == 5:
                # TEST SYNCRONOUS
                #time.sleep(5)
                #self.reply({'seconds': 'that was TOTALLY five seconds'})
                # TEST ASYNCRONOUS
                self.publish(message, callback=self.after_five, transaction=transaction)
            else:
                logging.debug(message)

    # COMMENT THIS TO RUN THE ABOVE ASYNC and SYNC TESTS
    # Test for simply sending a message and receiving a reply
    #  tests publish() with a callback, but no transaction
    # def run(self):
    #     with ApolloConnection(self.args) as self.apollo_conn:
    #         self.publish({'secs': '1'}, callback=self.after_one)
    #         while True:
    #             frame = self.apollo_conn.receiveFrame()
    #             logging.info("Received message: {0}".format(frame.info()))
    #             self.on_message(frame)
    #             break

    def after_one(self, frame, context, transaction):
        #self.reply({'seconds':'that was one second'})
        self.reply({'seconds':'that was one second'}, transaction)

    def after_five(self, frame, context, transaction):
        #self.reply({'seconds':'that was five second'})
        self.reply({'seconds':'that was five seconds'}, transaction)

    def handle_reply(self, frame, transaction):
        super(WetwareWorker, self).handle_reply(frame, transaction)
        message = json.loads(frame.body)
        print "GOT BACK THIS MESSAGE:"
        print message

def main():
    logging.basicConfig(level=logging.DEBUG)
    wetware_worker = WetwareWorker("wetware")
    wetware_worker.run()

if __name__ == "__main__":
    main()
