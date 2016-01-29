#!/usr/bin/env python

import logging
import json
import os
import glob
import struct
import shutil
from datetime import datetime

from ccd.worker import Worker
from ccd.worker import WorkerException
from ccd.worker import FrameException

class WetwareWorker(Worker):

    def __init__(self, subclass_section):
        super(WetwareWorker, self).__init__(subclass_section)

    def on_message(self, frame):
        ### This header must not be modified ###
        super(WetwareWorker, self).on_message(frame)
        message = json.loads(frame.body)
        ############## End header ##############

    def define_default_args(self):
        ### This header must not be modified ###
        defaults = super(WetwareWorker, self).define_default_args()
        ############## End header ##############

        #defaults['my_parameter'] = "my_default_parameter_value"

        ### This footer must not be modified ###
        return defaults
        ############## End footer ##############

    def add_argparse_args(self, parser):
        ### This headermust not be modified ###
        super(WetwareWorker, self).add_argparse_args(parser)
        ############## End header ##############

        #parser.add_argument("--my_parameter", "-m", dest="my_parameter",
        #                    help="this is my new, special parameter")

    def verify_frame(self, frame):
        ### This header must not be modified ###
        super(WetwareWorker, self).verify_frame(frame)
        message = json.loads(frame.body)
        ############## End header ##############

        #for key in ('data', 'data_count', 'host'):
        #    if key not in message:
        #        raise FrameException("Message has no {0} field".format(key))

class WetwareException(WorkerException):
    pass

def main():
    logging.basicConfig(level=logging.DEBUG)
    wetware_worker = WetwareWorker("wetware")
    wetware_worker.run()

if __name__ == "__main__":
    main()
