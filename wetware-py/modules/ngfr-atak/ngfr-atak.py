#!/usr/bin/env python

import logging
import json

from wetware.worker import Worker
from wetware.neuron import Statements

class WetwareWorker(Worker):

    #LONGTODO: move from incident_names and user_names as indexes to UUIDs
    #LONGTODO: topic name conversion only replace spaces with dashes; handle more special chars
    #LONGTODO: users can't leave an incident, only join (who cares)

    def __init__(self, subclass_section):
        super(WetwareWorker, self).__init__(subclass_section)
        self.open_incidents = self.get_neuron_incidents()
        self.responders = {} #TODO: get these from neuron
        #LONGTODO: add organizations for org-wide alerts
        #TODO: either rediscover sensors anew, or look up previously discovered info
        #    self.find sensors and subscribe to events for all existing incidents

    def get_neuron_incidents(self):
        #TODO: ask neuron what all open incidents are
        #TODO: for each incident
        #TODO:     ask neuron what users are currently responding
        return {}

    def on_message(self, frame):
        ### This header must not be modified ###
        transaction = super(WetwareWorker, self).on_message(frame)
        message = json.loads(frame.body)
        ############## End header ##############

        if frame.headers['destination'].endswith('new'):
            self.create_new_incident(message, transaction)
        elif frame.headers['destination'].endswith('join'):
            self.join_new_incident(message, transaction)
        elif frame.headers['destination'].endswith('close'):
            self.close_incident(message, transaction)

    def create_new_incident(self, message, transaction):
        #create new incident (as open)
        incident = message['incident_id']
        if incident not in self.open_incidents:
            self.open_incidents[incident] = {'status': 'open',
                                             'responders': []}
            #add all incident properties
            for key in message:
                self.open_incidents[incident][key] = message[key]
            #create incident topic
            alert_topic = '/topic/ngfr.alert.incident.' + incident.lower().replace(' ','-')
            livedata_topic = '/topic/ngfr.livedata.incident.' + incident.lower().replace(' ','-')
            self.open_incidents[incident]['alert_topic'] = alert_topic
            self.open_incidents[incident]['livedata_topic'] = livedata_topic
            #TODO: save incident to neuron
            logging.info("New incident: {0}".format(incident))
            logging.info(self.open_incidents[incident])
            if transaction:
                self.reply(message)
            #kick-off full incident analysis
            #TODO?: should this happen any time other than upon creation?
            self.analyze_incident(self.open_incidents[incident])
        elif transaction:
            self.reply({'error':'Incident already exists.'})

    def analyze_incident(self, incident_obj):
        sensors = self.discover_sensors(incident_obj)
        incident_obj['sensors'] = sensors
        #assuming there are any sensors around, figure out which to listen to
        if sensors:
            self.register_sensor_events(incident_obj)

    def register_sensor_events(self, incident_obj):
        for sensor in incident_obj['sensors']:
            events = self.analyze_sensor_context(sensor, incident_obj)
            for event in events:
                self.register_sensor_event(event)

    def register_sensor_event(event):
        #TODO: register events for those alerts, subscribe to a channel for them
        pass

    def analyze_sensor_context(self, sensor, incident_obj):
        #TODO: determine the kind of events that are important (if at all)
        #      for this sensor using knowledge.  supply a callback for each event
        #TODO: create an anonymous callback that, upon alert,
        #      determines what each party should do and tells them
        return []

    def discover_sensors(self, incident_obj):
        #TODO: implement OGC SOS query to get sensor info
        sensors = {
            'sensor1': {
                'lat': 123.123,
                'lon': 234.234,
                'type': 'gas'
            },
            'sensor2': {
                'lat': 123.124,
                'lon': 234.235,
                'type': 'carbon-monoxide'
            }
        }
        return sensors

    def join_new_incident(self, message, transaction):
        incident = message['incident_id']
        if incident in self.open_incidents:
            #add responder to incident
            self.open_incidents[incident]['responders'].append(message['user'])
            #add responder to responders
            username = message['user']['name']
            if username not in self.responders:
                self.responders[username] = message['user']
                #add user-specific topic for alerts
                user_topic = '/topic/ngfr.alert.user.' + username.lower().replace(' ','-')
                self.responders[username]['alert_topic'] = user_topic
            if transaction:
                #list of livedata and alert topics
                topics = {'livedata': [ self.open_incidents[incident]['livedata_topic'] ],
                          'alert': [ self.responders[username]['alert_topic'],
                                     self.open_incidents[incident]['alert_topic']]}
                self.reply(topics)
            logging.info("Added user {0} to incident {1}".format(username, incident))
            logging.info(self.open_incidents[incident])
        elif transaction:
            self.reply({'error': 'Incident does not exist'})

    def close_incident(self, message, transaction):
        incident = message['incident_id']
        if incident in self.open_incidents:
            logging.info("Closing incident: {0}".format(incident))
            #unnecessary to set this before deleting, but keeps consistent
            self.open_incidents[incident]['status'] = 'closed'
            del self.open_incidents[incident]
            #TODO: set incident as closed in neuron
            if transaction:
                self.reply(message)
            logging.info(self.open_incidents)
        elif transaction:
            self.reply({'error': 'Incident does not exist'})

    def verify_frame(self, frame):
        ### This header must not be modified ###
        super(WetwareWorker, self).verify_frame(frame)
        message = json.loads(frame.body)
        ############## End header ##############

        if frame.headers['destination'].endswith('new'):
            for key in ['incident_id']:
                if key not in message:
                    raise FrameException("Message has no {0} field".format(key))
        elif frame.headers['destination'].endswith('join'):
            for key in ['user', 'incident_id']:
                if key not in message:
                    raise FrameException("Message has no {0} field".format(key))
        elif frame.headers['destination'].endswith('closed'):
            for key in ['incident_id']:
                if key not in message:
                    raise FrameException("Message has no {0} field".format(key))

def main():
    logging.basicConfig(level=logging.INFO)
    wetware_worker = WetwareWorker("wetware")
    wetware_worker.run()

if __name__ == "__main__":
    main()
