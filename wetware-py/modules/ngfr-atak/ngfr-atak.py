#!/usr/bin/env python

import logging
import json

from wetware.worker import Worker
from wetware.neuron import Statements

class WetwareWorker(Worker):

    def __init__(self, subclass_section):
        super(WetwareWorker, self).__init__(subclass_section)
        self.open_incidents = self.get_neuron_incidents()
        #TODO: self.find sensors and subscribe to events for all existing incidents

    def get_neuron_incidents(self):
        #TODO: ask neuron what all open incidents are
        #TODO: for each incidnet:
        #TODO: ask neuron what users are currently responding
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
            self.open_incidents[incident] = {'status': 'open'}
            #add all incident properties
            for key in message:
                self.open_incidents[incident][key] = message[key]
            #TODO: save incident to neuron
            #TODO: based on incident GEO, query SOS for sensors
            #TODO: based on incident metadata + cortex, determine what events to set with sensor
            #TODO: set those events/subscribe to a channel for those alerts
            #TODO: upon alert: determine what each party should do and alert them
            logging.debug("New incident: {0}".format(incident))
            logging.debug(self.open_incidents[incident])
            if transaction:
                self.reply(message)
        elif transaction:
            self.reply({'error':'Incident already exists.'})

    def join_new_incident(self, message, transaction):
        #TODO: reply with topic for invident livedata
        #TODO: reply with topics for alerts (group and user)
        pass

    def close_incident(self, message, transaction):
        incident = message['incident_id']
        if incident in self.open_incidents:
            logging.debug("Closing incident: {0}".format(incident))
            #unnecessary to set this before deleting, but keeps consistent
            self.open_incidents[incident]['status'] = 'closed'
            del self.open_incidents[incident]
            #TODO: set incident as closed in neuron
            if transaction:
                self.reply(incident)
            logging.debug(self.open_incidents)
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
    logging.basicConfig(level=logging.DEBUG)
    wetware_worker = WetwareWorker("wetware")
    wetware_worker.run()

if __name__ == "__main__":
    main()
