#!/usr/bin/env python

import logging
import json
import paho.mqtt.client as mqtt

from wetware.worker import Worker
from wetware.worker import ApolloConnection

from wetware.neuron import Statements

import math
DEGREES_PER_METER=0.000008983

def distance(p0, p1):
    return math.sqrt((p0[0] - p1[0])**2 + (p0[1] - p1[1])**2)

class WetwareWorker(Worker):

    def __init__(self, subclass_section):
        super(WetwareWorker, self).__init__(subclass_section)

        self.mqtt_client = mqtt.Client()
        self.mqtt_client.on_connect = self.mqtt_on_connect
        self.mqtt_client.on_message = self.mqtt_on_message
        self.known_sensors = {}

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
        if node not in self.known_sensors:
            self.known_sensors[node] = {'name': node }
            #only need to set this sensor type once for Cortex
            statements.add_vertex_property(node, "type", "sensor")
        self.known_sensors[node]['location'] = lat_lon
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
        if node not in self.known_sensors:
            self.known_sensors[node] = {'name': node }
            #only need to set this sensor type once for Cortex
            statements.add_vertex_property(node, "type", "sensor")
        self.known_sensors[node]['ambient_temp'] = ambient_temp
        self.known_sensors[node]['target_temp'] = target_temp
        self.publish(statements)
        if ambient_temp > self.args['temp_threshold'] or target_temp > self.args['temp_threshold']:
            self.check_for_danger()

    def parse_grove_data(self, message):
        """This is the alcohol sensor
        """
        node = message['clientname']
        sensors = message['sensors']
        alcohol = None
        for sensor in sensors:
            if sensor['name'] == "Alcohol":
                alcohol = sensor['value']
                break
        if alcohol:
            logging.info("{0}: alcohol: {1}".format(node, alcohol))
            statements = Statements()
            statements.add_vertex_property(node, "alcohol", alcohol)
            if node not in self.known_sensors:
                self.known_sensors[node] = {'name': node }
                #only need to set this sensor type once for Cortex
                statements.add_vertex_property(node, "type", "sensor")
            self.known_sensors[node]['alcohol'] = alcohol
            self.publish(statements)
            if alcohol > float(self.args['alcohol_threshold']):
                self.check_for_danger()

    def check_for_danger(self):
        high_alcohol = False
        high_temp = False
        epicenter = "Unknown"
        alcohol_location = None
        temp_location = None
        for sensor in self.known_sensors.values():
            if 'alcohol' in sensor and sensor['alcohol'] > float(self.args['alcohol_threshold']):
                high_alcohol = True
                if 'location' in sensor:
                    alcohol_location = sensor['location']
                    epicenter = sensor['location']
            if 'ambient_temp' in sensor and sensor['ambient_temp'] > float(self.args['temp_threshold']):
                high_temp = True
                if 'location' in sensor:
                    temp_location = sensor['location']
            if 'target_temp' in sensor and sensor['target_temp'] > float(self.args['temp_threshold']):
                high_temp = True
                if 'location' in sensor:
                    temp_location = sensor['location']
        if high_alcohol and high_temp:
            distance_in_meters = float(distance(alcohol_location, temp_location))/float(DEGREES_PER_METER)
            logging.warning("High alcohol and temperature, distance apart in meters: {0}".format(distance_in_meters))
            if alcohol_location and temp_location and (distance_in_meters <= float(self.args['danger_distance_in_meters'])):
                alert_msg = {
                    'message': "Dangerous levels of alcohol and temperature near {0}. Substances only {1} meters apart".format(epicenter,distance_in_meters)
                }
                logging.warning(str(alert_msg))
                self.mqtt_client.publish("global/alert", str(alert_msg))

def main():
    logging.basicConfig(level=logging.INFO)
    wetware_worker = WetwareWorker("wetware")
    wetware_worker.run()

if __name__ == "__main__":
    main()
