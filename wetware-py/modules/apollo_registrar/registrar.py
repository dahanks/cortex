#!/usr/bin/env python

import logging
import json

from subprocess import call

from wetware.worker import Worker

class Registrar(Worker):

    def on_message(self, frame):
        ### This header must not be modified ###
        transaction = super(Registrar, self).on_message(frame)
        message = json.loads(frame.body)
        ############## End header ##############

        if 'username' and 'password' in message:
            user = message['username']
            pw = message['password']
            logging.info("Registering user: {0}".format(user))
            call(['/bin/bash', 'register_user.sh', user, pw])

def main():
    logging.basicConfig(level=logging.DEBUG)
    registrar = Registrar("registrar")
    registrar.run()

if __name__ == "__main__":
    main()
