#!/usr/bin/env python

import logging
import json

from wetware.worker import Worker
import wetware.neuron as Neuron

class WetwareWorker(Worker):

    #LONGTODO: move from incident_names and user_names as indexes to UUIDs
    #LONGTODO: topic name conversion only replace spaces with dashes; handle more special chars
    #LONGTODO: users can't leave an incident, only join (who cares)
    #LONGTODO: how do we continuously monitor the situation and update what we
    #          think is important? periodically? event-driven?
    #LONGTODO: make sure new instances of this Worker cooperate with old ones
    #          e.g., will a new worker care about new sensor events that an
    #          old worker isn't listening to; old workers listening to old events?

    def __init__(self, subclass_section):
        super(WetwareWorker, self).__init__(subclass_section)
        self.open_incidents = {}
        self.responders = {}
        self.event_callbacks = {}
        self.subscription_counts = {}
        #LONGTODO: add organizations for org-wide alerts

    def run_setup(self):
        #get all open incidents from Cortex
        #TODO: ask for responders based on edges
        query = "g.V().has('type','ngfr:atak:incident').has('status', 'open').valueMap()"
        self.publish(Neuron.gremlin(query), topic=Neuron.NEURON_DESTINATION, callback=self.handle_run_setup)
        #goto: handle_run_setup()

    def handle_run_setup(self, frame, context, transaction):
        #received all open incidents
        responses = Neuron.Responses(frame)
        incidents = responses.get_vertex_objects()
        for incident in incidents:
            incident_id = incident['incident_id']
            if incident_id not in self.open_incidents:
                self.open_incidents[incident_id] = incident
                #TODO: handle other repsonses to get responders
                self.open_incidents[incident_id]['responders'] = {}
            if 'responder' in incident:
                for responder in incident['responders']:
                    username = responder['name']
                    if username not in self.responders:
                        self.responders[username] = responder
            #now re-run analysis for all these open incidents
            self.analyze_incident(incident_id)
        logging.info("OPEN INCIDENTS")
        logging.info(self.open_incidents)
        logging.info("RESPONDERS")
        logging.info(self.responders)

    def on_message(self, frame):
        ### This header must not be modified ###
        transaction = super(WetwareWorker, self).on_message(frame)
        message = json.loads(frame.body)
        ############## End header ##############

        #Register incidents
        #wetware.ngfr.register.{new,join,close}
        if 'register' in frame.headers['destination']:
            if frame.headers['destination'].endswith('new'):
                self.create_incident(message, transaction)
            elif frame.headers['destination'].endswith('join'):
                self.join_incident(message, transaction)
            elif frame.headers['destination'].endswith('close'):
                self.close_incident(message, transaction)
        #Handle sensor events by running the callback
        #wetware.ngfr.event.<event-name>
        elif 'event' in frame.headers['destination']:
            event_id = frame.headers['destination'].split('.')[-1]
            callback = self.event_callbacks[event_id]['callback']
            incident_id = self.event_callbacks[event_id]['incident_id']
            #callback won't be called if incident is over
            # (but the subscription ought to be deleted by closing the incident)
            if incident_id in self.open_incidents:
                callback(incident_id, message)

    def create_incident(self, message, transaction):
        #create new incident (as open)
        incident_id = message['incident_id']
        if incident_id not in self.open_incidents:
            self.open_incidents[incident_id] = {'status': 'open',
                                                'responders': {}}
            #add all incident properties
            for key in message:
                self.open_incidents[incident_id][key] = message[key]
            #create incident topic
            alert_topic = '/topic/ngfr.alert.incident.' + incident_id.lower().replace(' ','-')
            livedata_topic = '/topic/ngfr.livedata.incident.' + incident_id.lower().replace(' ','-')
            self.open_incidents[incident_id]['alert_topic'] = alert_topic
            self.open_incidents[incident_id]['livedata_topic'] = livedata_topic
            #need this for Cortex API
            self.open_incidents[incident_id]['name'] = "ngfr:atak:incident:{0}".format(
                incident_id.replace(' ','_'))
            self.open_incidents[incident_id]['type'] = "ngfr:atak:incident"
            #store incident in Cortex
            self.store_in_cortex(self.open_incidents[incident_id])
            logging.info("New incident: {0}".format(incident_id))
            logging.info(self.open_incidents[incident_id])
            if transaction:
                self.reply(message)
            #kick-off full incident analysis
            self.analyze_incident(incident_id)
        elif transaction:
            self.reply({'error':'Incident already exists.'})

    def analyze_incident(self, incident_id):
        #TODO: ask for sensors using lat-lon indexing and elasticsearch
        #discover sensors
        query = "g.V().has('type','sensor').valueMap()"
        context = {'incident_id': incident_id}
        self.publish(Neuron.gremlin(query), topic=Neuron.NEURON_DESTINATION, callback=self.handle_sensor_discovery, context=context)
        #goto: handle_sensor_discovery()

    def handle_sensor_discovery(self, frame, context, transaction):
        responses = Neuron.Responses(frame)
        sensors = responses.get_vertex_objects()
        incident_id = context['incident_id']
        logging.info("HERE ARE THE SENSORS FROM CORTEX")
        logging.info(sensors)

        #if there are no sensors in Cortex, let's just seed it real quick
        if not sensors:
            sensors = self.seed_cortex_with_sensors_for_testing()

        if incident_id in self.open_incidents:
            self.open_incidents[incident_id]['sensors'] = sensors
            #assuming there are any sensors around, figure out which to listen to
            if sensors:
                self.register_sensor_events(incident_id)
            #LONGTODO: if this function doesn't result in anything useful, you might
            #          want to let people know Audrey couldn't figure out how to help
        else:
            logging.info("Finished analyzing an incident that has already closed.")

    def seed_cortex_with_sensors_for_testing(self):
        sensors = [
            {
                'name': 'sensor1',
                'lat': 123.123,
                'lon': 234.234,
                'type': 'sensor',
                'sensor_type': 'gas'
            },
            {
                'name': 'sensor2',
                'lat': 123.124,
                'lon': 234.235,
                'type': 'sensor',
                'sensor_type': 'carbon-monoxide'
            }
        ]
        for sensor in sensors:
            self.store_in_cortex(sensor)
        return sensors

    def register_sensor_events(self, incident_id):
        if incident_id in self.open_incidents:
            for sensor in self.open_incidents[incident_id]['sensors']:
                events = self.analyze_sensor_context(sensor, incident_id)
                for event in events:
                    self.register_sensor_event(event, incident_id)

    def register_sensor_event(self, event, incident_id):
        #TODO: register event via George's makeshift SES (but for now...)
        self.publish({'trigger': event['trigger'], 'topic': event['topic']}, topic='/topic/some-sensor.event.register')
        #TODO: change this if the sensor wants to dictate the event topic
        event_sub = None
        #check your list of subscriptions to see if you're already subscribed
        # via another incident
        if event['topic'] not in self.subscription_counts:
            event_sub = self.subscribe(event['topic'])
            self.subscription_counts[event['topic']] = {
                'sub': event_sub,
                'count': 1
            }
        else:
            event_sub = self.subscription_counts[event['topic']]['sub']
            self.subscription_counts[event['topic']]['count'] += 1
        #register the callback function with the incident
        self.event_callbacks[event['id']] = {
            'incident_id': incident_id,
            'callback': event['callback'],
            'subscription': event_sub
        }

    def analyze_sensor_context(self, sensor, incident_id):

        def callback(incident_id, message):
            if incident_id in self.open_incidents:
                incident_obj = self.open_incidents[incident_id]
                #TODO: determine who should do what based on the context (incident_obj)
                incident_alert = {
                    'alert': "All non-emergency staff evacuate the area."
                }
                self.publish(incident_alert, incident_obj['alert_topic'])
                for responder in incident_obj['responders'].values():
                    if responder['name'] == "David Horres":
                        responder_alert = {
                            'alert': "{0}, please head to {1} to investigate"
                            " high levels of {2}".format(responder['name'],
                                                         message['location'],
                                                         message['event'])
                        }
                        self.publish(responder_alert, topic=responder['alert_topic'])

        #TODO: use knowledge to determine the kind of events that are important
        event_id = sensor['name'] + '-event'
        events = [
            {
                'id': event_id,
                'sensor': sensor,
                'trigger': ('gas-level', 'gt', 10),
                'callback': callback,
                'topic': '/topic/wetware.ngfr.event.' + event_id
            },
        ]

        return events

    def join_incident(self, message, transaction):
        incident_id = message['incident_id']
        if incident_id in self.open_incidents:
            username = message['user']['name']
            #add responder to responders
            if username not in self.responders:
                self.responders[username] = message['user']
                #add user-specific topic for alerts
                user_topic = '/topic/ngfr.alert.user.' + username.lower().replace(' ','-')
                self.responders[username]['alert_topic'] = user_topic
            #add responder to incident
            if username not in self.open_incidents[incident_id]['responders']:
                self.open_incidents[incident_id]['responders'][username] = self.responders[username]
            #store responder and edge in Cortex
            self.responders[username]['name'] = username
            self.responders[username]['type'] = 'person'
            self.store_in_cortex(self.responders[username])
            edge_tuple = (username, 'responded_to', self.open_incidents[incident_id]['name'])
            self.store_in_cortex(edge_tuple)
            if transaction:
                #list of livedata and alert topics
                topics = {'livedata_topics': [ self.open_incidents[incident_id]['livedata_topic'] ],
                          'alert_topics': [ self.responders[username]['alert_topic'],
                                            self.open_incidents[incident_id]['alert_topic']]}
                self.reply(topics)
            logging.info("Added user {0} to incident {1}".format(username, incident_id))
            logging.info(self.open_incidents[incident_id])
        elif transaction:
            self.reply({'error': 'Incident does not exist'})

    def close_incident(self, message, transaction):
        incident_id = message['incident_id']
        if incident_id in self.open_incidents:
            logging.info("Closing incident: {0}".format(incident_id))
            #Setting this before deleting to make it easy to store in Cortex
            self.open_incidents[incident_id]['status'] = 'closed'
            #Store now-closed incident in Cortex
            self.store_in_cortex(self.open_incidents[incident_id])
            del self.open_incidents[incident_id]
            #unsubscribe from events, but only if you're the last one subscribed
            for event_id in self.event_callbacks:
                event = self.event_callbacks[event_id]
                event_sub = event['subscription']
                if (event['incident_id'] == incident_id and
                    event_sub[1] in self.subscription_counts):
                    if self.subscription_counts[event_sub[1]]['count'] == 1:
                        logging.info("Unsubscribing from {0}".format(event_id))
                        #TODO: publish the unsubscribe request to the sensor, itself
                        self.unsubscribe(event['subscription'])
                        del self.subscription_counts[event_sub[1]]
                    else:
                        logging.info("Decrementing sub count for {0}".format(event_id))
                        self.subscription_counts[event_sub[1]]['count'] -= 1
            if transaction:
                self.reply(message)
            logging.info(self.open_incidents)
        elif transaction:
            self.reply({'error': 'Incident does not exist'})

    def store_in_cortex(self, vertex_or_edge):
        """Pass in a dict to store it as a vertex.
        Or pass in a tuple (from, label, to) to store an edge.
        """
        if isinstance(vertex_or_edge, dict):
            self.publish(Neuron.add_vertex_object(vertex_or_edge), topic=Neuron.NEURON_DESTINATION)
        elif isinstance(vertex_or_edge, tuple):
            statements = Neuron.Statements()
            statements.add_edge(vertex_or_edge)
            self.publish(statements, topic=Neuron.NEURON_DESTINATION)

    def verify_frame(self, frame):
        ### This header must not be modified ###
        super(WetwareWorker, self).verify_frame(frame)
        message = json.loads(frame.body)
        ############## End header ##############

        #TODO: update this (always)
        if 'register' in frame.headers['destination']:
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
        elif 'event' in frame.headers['destination']:
            for key in ['event', 'location']:
                if key not in message:
                    raise FrameException("Message has no {0} field".format(key))

def main():
    logging.basicConfig(level=logging.INFO)
    wetware_worker = WetwareWorker("wetware")
    wetware_worker.run()

if __name__ == "__main__":
    main()
