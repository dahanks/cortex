#!/usr/bin/env python

import logging
import json
import time

from wetware.worker import Worker
from wetware.worker import ApolloConnection

class WetwareWorker(Worker):

    def run(self):
        with ApolloConnection(self.args) as self.apollo_conn:
            #this object is getting overloaded to handle all cases (not minimal set)
            incident = {'incident_id': 'My new incident',
                        'user': {
                            'name': 'David Horres',
                        }}
            self.publish(incident, topic='/queue/wetware.ngfr.register.new', callback=self.ack)
            self.wait_for_response()
            time.sleep(3)
            self.publish(incident, topic='/queue/wetware.ngfr.register.join', callback=self.ack)
            self.wait_for_response()
            time.sleep(3)
            self.publish(incident, topic='/queue/wetware.ngfr.register.close', callback=self.ack)
            self.wait_for_response()

    def wait_for_response(self):
        while True:
            frame = self.apollo_conn.receiveFrame()
            logging.info("Received message: {0}".format(frame.info()))
            self.on_message(frame)
            break

    def ack(self, frame, context, transaction):
        logging.debug(json.loads(frame.body))

def main():
    logging.basicConfig(level=logging.DEBUG)
    wetware_worker = WetwareWorker("wetware")
    wetware_worker.run()

if __name__ == "__main__":
    main()
