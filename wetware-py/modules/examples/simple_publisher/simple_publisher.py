#!/usr/bin/env python

import logging
import json
import sys

from wetware.worker import Worker
from wetware.worker import ApolloConnection

class PublisherWorker(Worker):

    def run(self):
        with ApolloConnection(self.args) as self.apollo_conn:
            while True:
                data = { 'secs': self.args['secs']}
                if self.args['expect_reply']:
                    self.publish(data, callback=self.print_reply)
                    while True:
                        frame = self.apollo_conn.receiveFrame()
                        logging.info("Received message: {0}".format(frame.info()))
                        self.on_message(frame)
                        break
                else:
                    self.publish(data)
                    break

    def print_reply(self, frame, context, transaction):
        print json.dumps(json.loads(frame.body), sort_keys=True, indent=4, separators=(',', ': '))

def main():
    logging.basicConfig(level=logging.DEBUG)
    publisher = PublisherWorker("simplepublisher")
    publisher.run()

if __name__ == "__main__":
    main()
