#!/usr/bin/env python

import logging
import json

from wetware.worker import Worker
from wetware.worker import ApolloConnection

class WetwareWorker(Worker):

    def run(self):
        with ApolloConnection(self.args) as self.apollo_conn:
            statements = {"statements": ["David knows Ed.",
                                         "David is nice.",
                                         "The coolness of David is high.",
                                         "Does David know Ed?",
                                         "Is David nice?",
                                         "What is the coolness of David?",
                                         "Where is David?",
                                     ]}
            self.publish(statements)

def main():
    logging.basicConfig(level=logging.DEBUG)
    wetware_worker = WetwareWorker("wetware")
    wetware_worker.run()

if __name__ == "__main__":
    main()
