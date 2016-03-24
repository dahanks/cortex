#!/usr/bin/env python

import logging
import sys
import json
import time

from wetware.worker import Worker

class ListenerWorker(Worker):

    def on_message(self, frame):
        ### This header must not be modified ###
        transaction = super(ListenerWorker, self).on_message(frame)
        message = json.loads(frame.body)
        ############## End header ##############
        print json.dumps(message, sort_keys=True, indent=4, separators=(',', ': '))
        if 'secs' in message:
            time_to_sleep = float(message['secs'])
            time.sleep(time_to_sleep)
            reply_data = {'my_reply': 'here is your reply after {0} seconds'.format(time_to_sleep)}
            print reply_data
            self.reply(reply_data)

def main():
    logging.basicConfig(level=logging.DEBUG)
    listener = ListenerWorker("simplelistener")
    listener.run()

if __name__ == "__main__":
    main()
