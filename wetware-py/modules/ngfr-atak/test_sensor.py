#!/usr/bin/env python

import logging
import sys
import json
import time

from wetware.worker import Worker

class WetwareWorker(Worker):

    def on_message(self, frame):
        ### This header must not be modified ###
        transaction = super(WetwareWorker, self).on_message(frame)
        message = json.loads(frame.body)
        ############## End header ##############
        print json.dumps(message, sort_keys=True, indent=4, separators=(',', ': '))
        if 'topic' in message:
            time.sleep(10)
            event_data = {'event': 'High gas levels', 'location': 123.234}
            print event_data
            self.publish(event_data, topic=message['topic'])

def main():
    logging.basicConfig(level=logging.DEBUG)
    listener = WetwareWorker("wetware")
    listener.run()

if __name__ == "__main__":
    main()
