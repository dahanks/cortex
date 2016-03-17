#!/usr/bin/env python

import logging
import json

# these workers are designed to run with the module installed
# don't use relative paths
from wetware.worker import Worker
from wetware.worker import FrameException

"""So you want to create a Worker class that talks over Apollo.  Here's
what you need to do:

1. Define a method to do the work you need to do when you get a message
In this example, the method is "do_my_special_work"

2. Override "on_message(self, frame)"
In the body of the message, call the method(s) you wrote to do the work
while handling the message however you need to.

3a. Create an instance of your new subclass
In your main(), create an instance of your new subclass.  Pass in as an
argument a string that represents your class.  If you need to add parameters
to a config file relevant to your class, do it in a section with the string
you passed.

3b. Add any configuration parameters you may need
Add to the worker.config file a section with the string you passed, and add
any new parameters under there that you may need.

(OPTIONAL) 4. Override "define_default_args(self)"
If you want to add default values for the new parameters you added to the
config, you can do that in this method (more instructions inside the method).

(OPTIONAL) 5. Override "add_argparse_args(self)"
If you want to be able to override config values (specific to your new class)
by passing command line arguments, add those parameters to this method (more
instructions inside the method).

(OPTIONAL) 6. Override "verify_frame(self, frame)"
If you want to handle errors when you get a message that doesn't look like
you expect, add the handling in this method (more instructions inside the
method).
"""
class MySpecialWorker(Worker):

    """If you need to create a constructor, just make sure you call the super
    first
    """
    def __init__(self, subclass_section):
        super(MySpecialWorker, self).__init__(subclass_section)
        self.member_var = 'member_value'

    """This is called whenever your worker gets a message.  The dictionary var
    'message' has all the fields you're expecting.  So call whatever methods you
    wrote above based on whatever values you're looking for in 'message'.

    Transaction is used in case you need to reply to someone but first need to
    send/receive your own message.
    """
    def on_message(self, frame):
        ### This header should not be modified ###
        transaction = super(MySpecialWorker, self).on_message(frame)
        message = json.loads(frame.body)
        ############### End header ###############

        # Here is an example where just want to do work and publish results
        if message['my_operation'] == 'my_operation':
            self.do_my_special_work()
        # Here is an example where you want to reply to whomever sent this
        if message['my_operation'] == 'reply_to_me':
            self.reply_to_this_guy()
        # Here we need to reply, but we first need to make our own call
        #  to someone else
        if message['my_operation'] == 'async_operation':
            self.async_call(transaction)

    """This is your big moment.  Whatever unique work you need to get done,
    define it in one or more methods called whatever you want.

    If you need to publish output to a topic, use the self.publish(message)
    method.  By default it sends to the OUTPUT_TOPIC specified in your
    config file.  You can specify a different topic by adding the optional
    argument topic="mytopic" to the method.
    """
    def do_my_special_work(self):
        chant = {'gnomes': "Time to go to work, work all day...!"}
        logging.debug(chant)
        self.publish(chant)

    """If (and only if) you are running entirely synchronously, you can
    use the reply() method to send your response back to whomever sent
    you the request.

    The Worker base class will keep track of the details.  But again,
    this is only for performing synchronous work where you're only
    handling a single request at a time.

    The method will yell at you if it discovers you're doing asynchronous
    works--or if no one asked for a reply.
    """
    def reply_to_this_guy(self):
        data = {'msg': 'this is my reply!'}
        self.reply(data)

    """Here, we need to reply to someone--but first we need to ask someone else
    to do some work.

    If we don't want to synchronously busy-wait, we need to register a callback
    and keep track of who sent the original request.  Use the 'transaction' from
    the on_message() method, and pass in a callback function.

    In the callback, you will call publish(message, destination) instead of
    reply().  You will define the callback function to take (frame, destination)
    as parameters, and the destination will be provided by the base class, so
    just pass it in.

    If you want to make a call to someone and expect a response--but you DON'T
    intend to respond to some original caller, you can use publish() without
    passing in a 'transaction' yet still register a callback.
    """
    def async_call(self, transaction):
        data = {'work': 'passing this work to you!'}
        self.publish(data, callback=self.my_callback, transaction=transaction)

    """This is a typical callback for when you publish() data and want to handle
    a response.

    You must define the method to accept 'frame' and 'destination', even if you
    don't intend to reply to anyone.  If you didn't pass a 'transaction' in the
    correlated publish() call, destination will be None (but you must still
    define the callback function to accept it).
    """
    def my_callback(frame, destination):
        data = {'response': 'I got someone else to do that work you asked for!'}
        self.publish(data, destination)

    ######
    #Below are some configuration methods to override, all of which are optional
    ######

    """Define default parameters values here.  Just add the dictionary
    key and value to the 'defaults' dictionary.
    """
    def define_default_args(self):
        ### This header must not be modified ###
        defaults = super(MySpecialWorker, self).define_default_args()
        ############## End header ##############

        # Just add your key and value to the 'defaults' dictionary
        defaults['my_parameter'] = "my_default_parameter_value"

        ### This footer must not be modified ###
        return defaults
        ############## End footer ##############

    """If you want to add a command line argument to override a config
    parameter, call the argparse.add_argument() function for each one.
    """
    def add_argparse_args(self, parser):
        ### This headermust not be modified ###
        super(MySpecialWorker, self).add_argparse_args(parser)
        ############## End header ##############

        # Call this function for each argument you want to add
        parser.add_argument("--my_parameter", "-m", dest="my_parameter",
                            help="this is my new, special parameter")


    """Add to the verification of all messages you receive.  If a message
    isn't valid without a field that you expect, add that logic here. Just
    return False if anything is wrong with the message.
    """
    def verify_frame(self, frame):
        ### This header must not be modified ###
        super(MySpecialWorker, self).verify_frame(frame)
        message = json.loads(frame.body)
        ############## End header ##############

        # Add verification steps here.  For example: this statement says,
        # I expect the message to have a field called 'my_parameter', and if
        # it's not there, the message is no good.  Return False if anything
        # is bad.
        if not 'my_operation' in message:
            raise FrameException("Could not find my_operation in message!")
        if not 'my_parameter' in message:
            raise FrameException("Could not find my_parameter in message!")

def main():
    # Add this line if you want to see logging output
    logging.basicConfig(level=logging.DEBUG)

    # Instantiate your worker subclass.  The string you pass in the
    # constructor is the section name in the config file under which
    # you can add parameters specific to your worker.
    my_worker = MySpecialWorker("myspecialworker")

    # And run.  Your listener will wait for messages and do what you
    # want with them as they arrive.
    my_worker.run()

if __name__ == "__main__":
    main()
