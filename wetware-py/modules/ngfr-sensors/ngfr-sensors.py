#!/usr/bin/env python

import logging
import json
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
        self.mqtt_client.subscribe("global/#")

    def mqtt_on_message(self, client, userdata, message):
        if (message.topic.endswith("/gps")):
            self.parse_gps_data(json.loads(message.payload))
        elif (message.topic.endswith("/sensortag")):
            self.parse_sensortag_data(json.loads(message.payload))

    def parse_gps_data(self, message):
        node = message['clientname']
        lat = message['latitude']
        lon = message['longitude']
        lat_lon = "[{0},{1}]".format(lat, lon)
        output_data = {'statements': []}
        output_data['statements'].append(self.add_vertex_property_statement(node, "location", str(lat_lon)))
        self.publish(output_data)

    def parse_sensortag_data(self, message):
        node = message['clientname']
        humidity = message['humidity']['relative_humidity']
        output_data = {'statements': []}
        output_data['statements'].append(self.add_vertex_property_statement(node, "humidity", str(humidity)))
        self.publish(output_data)

    def add_vertex_property_statement(self, name, prop_name, prop_value):
        statement = {'fxns': [], 'api': 'neuron'}
        fxn = {'fxn': 'addVertexProperty',
               'name': name,
               'property': prop_name,
               'value': prop_value }
        logging.debug(fxn)
        statement['fxns'].append(fxn)
        return statement

def main():
    logging.basicConfig(level=logging.DEBUG)
    wetware_worker = WetwareWorker("wetware")
    wetware_worker.run()

if __name__ == "__main__":
    main()
