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

        #transaction will be none if we don't need to reply
        if frame.headers['destination'] == self.args['input_topic']:
            # secs = self.publish(message, expect_reply=True)
            # self.after_secs(secs)

            if float(message['secs']) == 1:
                #time.sleep(1)
                #self.reply({'seconds': 'that was JUST one second'})
                self.publish(message, callback=self.after_one, transaction=transaction)
            elif float(message['secs']) == 5:
                #time.sleep(5)
                #self.reply({'seconds': 'that was TOTALLY five seconds'})
                self.publish(message, callback=self.after_five, transaction=transaction)
            else:
                logging.debug(message)

    # def handle_reply(self, frame, transaction):
    #     super(WetwareWorker, self).handle_reply(frame, transaction)
    #     message = json.loads(frame.body)
    #     print frame.headers['destination']
    #     print message

    def run(self):
        with ApolloConnection(self.args) as self.apollo_conn:
            self.publish({'secs': '1'}, callback=self.after_one)
            while True:
                frame = self.apollo_conn.receiveFrame()
                logging.info("Received message: {0}".format(frame.info()))
                self.on_message(frame)
                break;

    def after_secs(self, secs):
        self.publish({'seconds':'that was {0} seconds'.format(secs)})

    def after_one(self, frame, destination):
        #self.reply({'seconds':'that was one second'})
        self.publish({'seconds':'that was one second'}, destination)

    def after_five(self, frame, destination):
        #self.reply({'seconds':'that was five second'})
        self.publish({'seconds':'that was five seconds'}, destination)

def main():
    logging.basicConfig(level=logging.DEBUG)
    wetware_worker = WetwareWorker("wetware")
    wetware_worker.run()

if __name__ == "__main__":
    main()
