#!/usr/bin/env python

import logging
import paho.mqtt.client as mqtt

from wetware.worker import Worker
from wetware.worker import ApolloConnection

class WetwareWorker(Worker):

    def __init__(self, subclass_section):
        super(WetwareWorker, self).__init__(subclass_section)

        self.mqtt_client = mqtt.Client()
        self.mqtt_client.on_connect = self.mqtt_on_connect
        self.mqtt_client.on_message = self.mqtt_on_message

    def mqtt_run(self):
        self.mqtt_client.connect(self.args['mqtt_host'], self.args['mqtt_port'])
        self.mqtt_client.loop_forever()

    def run(self):
        with ApolloConnection(self.args) as self.apollo_conn:
            self.mqtt_run()

    def mqtt_on_connect(self, client, userdata, flags, rc):
        pass

    def mqtt_on_message(self, client, userdata, msg):
        pass

def main():
    logging.basicConfig(level=logging.DEBUG)
    wetware_worker = WetwareWorker("wetware")
    wetware_worker.run()

if __name__ == "__main__":
    main()
