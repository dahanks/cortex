#!/usr/bin/env python

import logging
import json
import paho.mqtt.client as mqtt

from wetware.worker import Worker
from wetware.worker import ApolloConnection

from wetware.neuron import Statements

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
        elif (message.topic.endswith("/grove")):
            self.parse_grove_data(json.loads(message.payload))

    def parse_gps_data(self, message):
        node = message['clientname']
        lat = message['latitude']
        lon = message['longitude']
        #lists get converted to Geoshapes by Neuron
        lat_lon = [lat, lon]
        logging.info("{0}: location: {1}".format(node, lat_lon))
        statements = Statements()
        statements.add_vertex_property(node, "location", lat_lon)
        self.publish(statements)

    def parse_sensortag_data(self, message):
        """Lots of data here, but let's just get the temperature
        """
        node = message['clientname']
        ir_temp = message['ir_temp']
        ambient_temp = ir_temp['ambient_temp']
        target_temp = ir_temp['target_temp']
        logging.info("{0}: ambient_temp: {1}, target_temp: {2}".format(node, ambient_temp, target_temp))
        statements = Statements()
        statements.add_vertex_property(node, "ambient_temp", ambient_temp)
        statements.add_vertex_property(node, "target_temp", target_temp)
        self.publish(statements)

    def parse_grove_data(self, message):
        """This is the alcohol sensor
        """
        node = message['clientname']
        sensors = message['sensors']
        for sensor in sensors:
            if sensor['name'] == "Alcohol":
                alcohol = sensor['value']
                break
        if alcohol:
            logging.info("{0}: alcohol: {1}".format(node, alcohol))
            statements = Statements()
            statements.add_vertex_property(node, "alcohol", alcohol)
            self.publish(statements)

def main():
    logging.basicConfig(level=logging.INFO)
    wetware_worker = WetwareWorker("wetware")
    wetware_worker.run()

if __name__ == "__main__":
    main()
