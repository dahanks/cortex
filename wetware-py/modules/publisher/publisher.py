#!/usr/bin/env python

import logging
import json
import time

from wetware.worker import Worker
from wetware.worker import ApolloConnection
from wetware.neuron import Statements
from wetware.neuron import Responses

class WetwareWorker(Worker):

    def my_callback(self, frame, context, transaction):
        responses = Responses(frame)
        print responses

    def run_setup(self):
        statements = Statements()
        statements.gremlin("g.V().has('name','mark').outE('knows').propertyMap()")
        self.publish(statements, topic='/queue/neuron.operation', callback=self.my_callback)

        # statements.add_vertex('mark')
        # statements.add_vertex('zack')
        # statements.add_edge('mark', 'knows', 'zack', {'amount':'alot'})
        # self.publish(statements, '/queue/neuron.operation')
        # import sys
        # sys.exit(0)


        msg = """{
            "Ontology": {
                "Name": "TSPI"
            },
            "Nodes": [
                {
                    "TSPI": {
                        "index": 0,
                        "type": "Class",
                        "parents": [
                            "Wait_For_Command"
                        ],
                        "children": null,
                        "rules": null
                    }
                },
                {
                    "Start": {
                        "index": 1,
                        "type": "Class",
                        "parents": [
                            "Wait_For_Command"
                        ],
                        "children": null,
                        "rules": null
                    }
                },
                {
                    "TSPI_Container": {
                        "isRoot": true,
                        "index": 9,
                        "type": "Class",
                        "parents": null,
                        "children": [
                            "Wait_For_Command"
                        ],
                        "rules": null
                    }
                },
                {
                    "Read_File": {
                        "index": 5,
                        "type": "Execute",
                        "parents": [
                            "TSPI_Workflow"
                        ],
                        "children": [
                            "Run_Moses"
                        ],
                        "rules": {
                            "Execute": "doReadFile.py",
                            "Param": "tspi.csv"
                        }
                    }
                },
                {
                    "Calculate_Plateau": {
                        "index": 3,
                        "type": "Execute",
                        "parents": [
                            "Run_Moses"
                        ],
                        "children": [
                            "Run_Optimization"
                        ],
                        "rules": {
                            "Execute": "runMoses.py",
                            "Param": "1 17 4"
                        }
                    }
                },
                {
                    "Run_Moses": {
                        "index": 4,
                        "type": "Execute",
                        "parents": [
                            "Read_File"
                        ],
                        "children": [
                            "Calculate_Plateau"
                        ],
                        "rules": {
                            "Execute": "doCalcPlat.py"
                        }
                    }
                },
                {
                    "Run_Optimization": {
                        "index": 2,
                        "type": "Execute",
                        "parents": [
                            "Calculate_Plateau"
                        ],
                        "children": null,
                        "rules": {
                            "Execute": "doOptimization.py",
                            "Param": "--debug -v -f -xv"
                        }
                    }
                },
                {
                    "Execute": {
                        "index": 7,
                        "type": "Execute",
                        "parents": [
                            "Wait_For_Command"
                        ],
                        "children": [
                            "TSPI_Workflow"
                        ],
                        "rules": null
                    }
                },
                {
                    "Wait_For_Command": {
                        "index": 8,
                        "type": "Entry",
                        "parents": [
                            "TSPI_Container"
                        ],
                        "children": [
                            "TSPI",
                            "Start",
                            "Execute"
                        ],
                        "rules": {
                            "Command": "collectParents"
                        }
                    }
                },
                {
                    "TSPI_Workflow": {
                        "index": 6,
                        "type": "Alias",
                        "parents": [
                            "Execute"
                        ],
                        "children": [
                            "Read_File"
                        ],
                        "rules": null
                    }
                }
            ]
        }"""
        #self.publish(msg)


def main():
    logging.basicConfig(level=logging.DEBUG)
    wetware_worker = WetwareWorker("wetware")
    wetware_worker.run()

if __name__ == "__main__":
    main()
