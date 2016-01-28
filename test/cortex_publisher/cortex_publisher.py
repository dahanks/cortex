#!/usr/bin/env python

import logging
import json
import sys
import time

from ccd.worker import Worker
from ccd.worker import ApolloConnection

class PublisherWorker(Worker):

    def run(self):
        with ApolloConnection(self.args) as self.apollo_conn:
            data = { 'api': 'blueprints',
                     'statements': [
                         {
                             'fxns': [
                                 {
                                     'fxn': 'addVertex',
                                     'args': ['name', 'david']
                                 }
                             ]
                         },
                         {
                             'fxns': [
                                 {
                                     'fxn': 'addVertex',
                                     'args': ['name', 'lorna']
                                 }
                             ]
                         }
                     ]
            }
            self.publish(data, expect_reply=self.args['expect_reply'])
            time.sleep(3)

            data = { 'api': 'gremlin',
                     'statements': [
                         {
                             'fxns': [
                                 { 'fxn': 'V', 'args': [] },
                                 { 'fxn': 'values', 'args': ["name"] },
                                 { 'fxn': 'toList', 'args': [] },
                             ]
                         }
                     ]
            }
            self.publish(data, expect_reply=self.args['expect_reply'])
            time.sleep(3)

            # while True:
            #     self.publish(data, expect_reply=self.args['expect_reply'])
            #     if self.args['expect_reply']:
            #         while True:
            #             frame = self.apollo_conn.receiveFrame()
            #             logging.info("Received message: {0}".format(frame.info()))
            #             self.on_message(frame)
            #     time.sleep(5)


    def handle_reply(self, frame):
        print json.dumps(json.loads(frame.body), sort_keys=True, indent=4, separators=(',', ': '))
        print "Got my reply...exiting."
        sys.exit(1)

def main():
    logging.basicConfig(level=logging.DEBUG)
    publisher = PublisherWorker("cortexpublisher")
    publisher.run()

if __name__ == "__main__":
    main()
