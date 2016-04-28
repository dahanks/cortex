#!/usr/bin/env python

import os
import argparse
import ConfigParser
import logging
import json
import subprocess

from uuid import uuid4 as UUID

from stompest.config import StompConfig
from stompest.sync import Stomp
from stompest.protocol import StompSpec

from wetware.neuron import Statements
from wetware.neuron import Responses
from wetware.neuron import NEURON_DESTINATION
from wetware.neuron import NeuronException
# Section of the config file for base class properties
BASE_SECTION = "main"

class Worker(object):
    """Worker

    Worker agent that subscribes to messages from an Apollo server, does some
    kind of work, and possibly publishes a result back to Apollo.
    """
    def __init__(self, subclass_section=None):
        self.args = self.__parse_all_params(subclass_section)
        self.apollo_conn = None
        self.transactions = {}

    def run(self):
        """Initialize Worker and loop while waiting for input.

        If you want to use the Worker base class without running a service,
        you'll need to override this function, but keep the first 'with' line.
        """
        with ApolloConnection(self.args) as self.apollo_conn:
            # subscribe to topic and handle messages;
            #  otherwise, just end and let something override run()
            if "input_topic" in self.args and self.args['input_topic']:
                while True:
                    frame = self.apollo_conn.receiveFrame()
                    logging.info("Received message: {0}".format(frame.info()))
                    try:
                        self.on_message(frame)
                    # skip over bad frames, but halt on other exceptions
                    except FrameException, e:
                        logging.exception(e)
            else:
                logging.warning("No input topic was specified, so unless this"
                                " function is overridden, nothing will happen")

    def on_message(self, frame):
        """Handles overhead of incoming messages, then let's subclass handle the
        work.

        This method is designed to be overridden (call super first!). Perform
        work based on the queue/topic on which your Worker got the message.

        If the message received expects a reply, a transaction will be recorded,
        with the reply-to destination tracked.  If, before replying, this worker
        needs to publish and receive a response, those elements will be recorded
        and linked to this transaction.

        When you override this method (which is effectively a requirement), you
        should perform work based on the queue/topic on which this message was
        received.  As a desired side-effect, this will ensure that you filter
        out messages that came in as replies on temp-queues.  Otherwise, you'll
        accidentally call self.on_message() for replies the same you would for
        original messages.

        Returns a transaction so that you may modify it in the base class and
        optionally pass it into any secondary publish calls.
        """
        # must ack to remove from queue
        self.apollo_conn.ack(frame)

        # check if this is something you need to reply to, and create a
        #  a transaction if so; otherwise, transaction is None
        transaction_uuid = None
        if 'reply-to' in frame.headers:
            transaction_uuid = str(UUID())
            self.transactions[transaction_uuid] = {'reply-to': frame.headers['reply-to']}

        try:
            self.verify_frame(frame)
            # check if this is a reply you're waiting for
            if frame.headers['destination'].startswith('/queue/temp'):
                # the destination we used to subscribe looks a little different
                #  than the destination coming in this time; hence, the weird
                #  tuple check with string concatentation below.
                transaction = str(frame.headers['destination'].split('.')[-1])
                self.handle_reply(frame, transaction)
        except FrameException, e:
            # Frame not verified; send an error in reply (if expected)
            #  otherwise, just skip it and continue outside loop...
            if 'reply-to' in frame.headers:
                self.publish({ 'Error': 'Frame failed verification' }, frame.headers['reply-to'])
            raise
        # returning transaction ID for subclass to pass into publish/callback
        return transaction_uuid

    def handle_reply(self, frame, transaction):
        """Handles a reply over a temp queue to a request you already submitted

        Calling this methods requires that you've already created a transaction
        with a callback function.  That callback will be called in this method.
        We do not guarantee much error/exception handling here, but we will make
        sure what you passed is actually a function.

        This method can be overridden (call SUPER first!).  If overridden
        (with super), callbacks will still be called, then the subclass
        implementation of this method will run--sort of like a 'finally'.
        """
        try:
            # calling handle_reply implies there was a transaction
            #  with a callback and a temp_sub
            callback = self.transactions[transaction]['callback']
            context = self.transactions[transaction]['context']
            temp_sub = self.transactions[transaction]['temp_sub']
            self.apollo_conn.unsubscribe(temp_sub)
        except (KeyError, ValueError):
            logging.exception("Somehow you got a message on a temp queue that"
                              " you weren't keeping track of. You may not have"
                              " specified a callback in publish().")
            raise

        if (callback
            and hasattr(callback, '__name__')
            and hasattr(callback, '__call__')
            and callback.__name__ in dir(self)):
            try:
                callback(frame, context, transaction)
            except TypeError:
                raise WetwareException("You implemented a callback with an invalid definition")
        else:
            raise WetwareException("Invalid callback provided: {0}".format(callback))

    def publish(self, message, topic=None, callback=None, context=None, transaction=None):
        """Publish a message to a topic using your Apollo Connection

        If no topic is supplied, we'll assume you want to publish output to the
        topic you configured with your OUTPUT_TOPIC parameter in your config
        file. You know, cause we're nice.

        If you expect a reply, you must supply a callback function to run when
        for when you get the response.  This is required, or else your message
        won't get the appropriate reply-to headers.

        If you would like to remember some context for when the callback is
        invoked, pass a dictionary as the 'context' parameter.

        Callback functions must have the signature:
            def your_callback(frame, context, transaction):

        If you had previously received a message requiring a response and are
        calling publish() as part of that work, specify the transaction related
        to the original request.  This will be passed into the callback so you
        can respond to the appropriate destination.

        You can specify a callback without a transaction, but that means you
        cannot be trying to respond to anyone else.

        You cannot specify a transaction without a callback--that's basically
        replying to someone and asking us to infer whom it is.  Use reply() for
        that.
        """

        # If you pass a dict, we'll convert it to JSON for you
        if isinstance(message, dict):
            message_str = json.dumps(message)
        # Otherwise, we'll try to cast whatever you passed as a string
        else:
            message_str = str(message)

        try:
            # If no topic is provided, use the output_topic from config file
            if not topic:
                if 'output_topic' not in self.args:
                    raise ConfigException("Tried to publish a message but there is no"
                                    " output_topic specified in the config!")
                else:
                    topic = self.args['output_topic']
            # Just check to see if we've specified a topic at this point.  This
            # is totally redundant, but feels safer.
            if topic:
                #Suuuper esoteric corner-case where you set a callback that is
                # not a function, but a variable set to 0 or False
                if callback == 0 or callback == False:
                    raise WetwareException("Provided a callback that wasn't a"
                                           " function! Callback: {0}".format(callback))
                # a transaction without a callback should really just be reply()
                if transaction and not callback:
                    raise WetwareException("Provided a transaction without a"
                                           " callback! Use reply() instead!")
                # specifying a callback without a transaction is totally valid
                if callback and not transaction:
                    transaction = str(UUID())
                    self.transactions[transaction] = {}
                # done with checks: now send the message registering the callback
                if callback:
                    self.transactions[transaction]['callback'] = callback
                    temp_sub = self.apollo_conn.subscribe('/temp-queue/' + transaction,
                                                          {StompSpec.ACK_HEADER:
                                                           StompSpec.ACK_CLIENT_INDIVIDUAL})
                    self.transactions[transaction]['temp_sub'] = temp_sub
                    self.transactions[transaction]['context'] = context
                    self.apollo_conn.send(topic, message_str,
                                          headers={'reply-to': '/temp-queue/' + transaction})
                else:
                    self.apollo_conn.send(topic, message_str)
        except AttributeError:
            raise WetwareException("Tried to publish a message but there is no"
                                   " Apollo connection! (Did you try to"
                                   " publish() without calling run() in your"
                                   " Worker?)")

    def reply(self, message, transaction=None):
        """Send a reply to whomever sent you a message with a reply-to.

        In synchronous workers, this function is just a convenient way for you
        to publish without having to specify the destination, since there is
        only ever one transaction at a time.

        In asynchronous workers, this is what you would use in the callback to
        respond to something waiting for your asynchronous call to finish. It's
        really just a convenient way to pass the transaction instead of looking
        up the destination in the transactions map.
        """
        if transaction and 'reply-to' in self.transactions[transaction]:
            self.publish(message, self.transactions[transaction]['reply-to'])
            del self.transactions[transaction]
        elif len(self.transactions) == 1:
            # grab the only transaction, publish to it, and delete it
            trans_id, transaction = self.transactions.items()[0]
            if 'reply-to' in transaction:
                self.publish(message, transaction['reply-to'])
                del self.transactions[trans_id]
            else:
                raise WetwareException("Tried to use reply() when no one was expecting it!")
        elif len(self.transactions) == 0:
            raise WetwareException("Tried to use reply() when no one was expecting it!")
        else:
            raise WetwareException("Tried to use reply() when there are multiple, asynchronous transactions!")

    def subscribe(self, topic):
        """Subscribe to an additional queue/topic specified by a string

        Will throw an exception if you are already subscribed to the topic.

        Returns the subscription object to be passed into unsubscribe().  Worker
        is responsible for keeping track of subscriptions.
        """
        return self.apollo_conn.subscribe(topic,
                                          {StompSpec.ACK_HEADER:
                                           StompSpec.ACK_CLIENT_INDIVIDUAL})

    def unsubscribe(self, subscription):
        """Unsubscribes from a topic.

        Must supply the subscription object returned during subscribe().

        TODO: consider extending this to allow specification of topic by string.
        """
        self.apollo_conn.unsubscribe(subscription)

    def add_neuron_vertex(self, vertex_obj):
        """Take a Python dict and make a vertex in Neuron from it.

        Will generate properties based on key-values in object.  Will not
        attempt to create any kind of edges.
        """
        if 'name' not in vertex_obj:
            raise NeuronException("Tried to create vertex with no 'name' field!")
        elif not isinstance(vertex_obj, dict):
            raise NeuronException("Tried to create a vertex from a non-dict!")
        statements = Statements()
        for key in vertex_obj:
            if key != 'name':
                statements.add_vertex_property(vertex_obj['name'],
                                               key,
                                               vertex_obj[key])
        self.publish(statements, topic=NEURON_DESTINATION)
    def verify_frame(self, frame):
        """Verify a frame (OVERRIDE and SUPER)

        Verify that a frame is valid JSON and has the appropriate fields.
        This function may be overrided by a subclass to add to the verification
        but it must call the SUPER() first.

        Raise a FrameException if frame is invalid; no need for a return value.
        """
        try:
            message = json.loads(frame.body)
            #DEPRECATED, but serves as a good example:
            # handle "operation" messages for sync and async commands
            if ('operation' in message and
                message['operation'].startswith('command')):
                if 'command' not in message:
                    raise FrameException({'message': "Received a 'command' operation"
                                           " without a 'command' field",
                                           'info': frame.info(),
                                           'body': frame.body})
                if message['operation'] not in ('command_sync',
                                                'command_async'):
                    raise FrameException({'message': "Received an unknown 'command' operation",
                                           'info': frame.info(),
                                           'body': frame.body})
        except (TypeError, ValueError):
            # raising FrameException so we can skip it--this shouldn't be fatal
            raise FrameException({'message': "Received an invalid JSON object in message",
                                   'info': frame.info(),
                                   'body': frame.body})

    def __parse_all_params(self, subclass_section=None):
        """Parse all command line and config args of base and optional subclass

        This method first collects all the command line args to find the config
        file.  The config file must the "main" section for the base class.  If
        we are instantiating a subclass, the constructor must be supplied with a
        string for the name of the section in the config relevant to the base
        class.

        Next, this method reads in the config file all at once.  It parses the
        "main" section and the subclass section if the string if supplied.

        Finally, it overrides any parameters found in the config file with those
        supplied on the command-line.

        The subclass may optionally add command-line args and parameter defaults
        by overriding (and calling super() within) the following two methods:
            self.add_argparse_args()
            self.define_default_args()
            self.verify_frame(frame)
            self.on_message(frame)
        """

        # grab the command line args first so we can find the config file
        cmd_line_args = self.__parse_command_line_args()
        config_file_path = os.path.abspath(cmd_line_args['config_file'])
        logging.debug("config_file full path: " + config_file_path)

        # check to see that the file exists
        if not os.path.exists(config_file_path):
            raise ConfigException("Config file path is not valid")

        # parse the config file, and supply the subclass section
        config_args = self.__parse_config_file(config_file_path,
                                               subclass_section)

        # override config file values with command line parameters
        # only if command line values are defined
        for key in cmd_line_args:
            if cmd_line_args[key]:
                config_args[key] = cmd_line_args[key]

        # output final list of args
        logging.debug("Worker config parameters:")
        for key in config_args:
            logging.debug(key + ": " + str(config_args[key]))

        return config_args

    def add_argparse_args(self, parser):
        """Add arguments for argparse (OVERRIDE and call SUPER)

        This method can be overridden by a subclass in order to add
        command line arguments beyond what are in the base class.  The
        subclass MUST call the super version of this function in order
        to get these base class arguments.
        """
        # this "config" argument should be the only one with a default
        # all other defaults are added to ConfigParser, since
        # it will be overridden by command line args
        parser.add_argument("--config_file", "-c", dest="config_file",
                            help="configuration file")
        parser.add_argument("--apollo_host", "-a", dest="apollo_host",
                            help="Apollo server hostname or IP")
        parser.add_argument("--apollo_port", "-p", dest="apollo_port",
                            help="Apollo server port")
        parser.add_argument("--apollo_user", "-u", dest="apollo_user",
                            help="Apollo server username")
        parser.add_argument("--apollo_password", "-w", dest="apollo_password",
                            help="Apollo server password")
        parser.add_argument("--input_topic", "-i", dest="input_topic",
                            help="Subscribe to this topic for your input")
        parser.add_argument("--output_topic", "-o", dest="output_topic",
                            help="Publish any output to this topic by default")

    def __parse_command_line_args(self):
        parser = argparse.ArgumentParser()
        self.add_argparse_args(parser)
        args_dict = parser.parse_args().__dict__
        if ('config_file' not in args_dict or
            not args_dict['config_file']):
            parser.print_help()
            raise ConfigException("You must specify a config file")
        else:
            return args_dict

    def define_default_args(self):
        """Define the default values for config params (OVERRIDE and call SUPER)

        This method can be overridden by a subclass in order to add
        default values for the subclass section of the config file.  The
        subclass MUST call the super version of this function in order
        to get these base class defaults.
        """
        defaults = dict()
        defaults['apollo_host'] = "127.0.0.1"
        defaults['apollo_port'] = "61613"
        defaults['apollo_user'] = "admin"
        defaults['apollo_password'] = "password"
        return defaults

    def __parse_config_file(self, config_file_path, subclass_section=None):
        # get the config defaults
        defaults = self.define_default_args()

        config = ConfigParser.ConfigParser()
        config.read(config_file_path)
        config_dict = dict()

        # wish we could use ConfigParsers defaults parameters, but it always
        # returns defaults in its options() and items() functions. so we'll do
        # it ourselves...
        for default in defaults:
            config_dict[default] = defaults[default]

        # iterate through all config options and put in a dict
        base_options = config.options(BASE_SECTION)
        for option in base_options:
            value = config.get(BASE_SECTION, option)
            # convert to real bool if it is
            value = check_for_bool(value)
            config_dict[option] = value

        # iterate through subclass options if specified
        if subclass_section:
            try:
                sub_options = config.options(subclass_section)
                for option in sub_options:
                    # prevent the subclass from using config params of the
                    # base class
                    if option in base_options:
                        raise ConfigException("Your subclass is trying to reuse a config"
                                              " file option of the base class")
                    value = config.get(subclass_section, option)
                    # convert to real bool if it is
                    value = check_for_bool(value)
                    config_dict[option] = value
            except ConfigParser.NoSectionError:
                logging.exception("Your config does not have a '{0}' section"
                                  " for your Worker subclass!".format(subclass_section))
                raise
        return config_dict

def check_for_bool(value):
    if value.lower() == 'true':
        return True
    elif value.lower() == 'false':
        return False
    else:
        return value

def run_command(command_args, sync=False, log_file=None, cwd=None):
    """Runs a command synchronously or asynchronously

    subprocess runs async by default.  Sync is achieved by waiting
    for stdout and stderr.

    Logs output if log_file is supplied; otherwise, does not track it.

    Returns the process exit status if sync.
    Returns the pid if async. Output from async command is not recovered if not
    logged to a file.

    Processes should exit if Python terminates.
    """
    logging.info("Running command: {0}".format(' '.join(command_args)))
    if log_file:
        logging.info("Command output logging to: {0}".format(log_file))
        with open(log_file,'a') as log_file:
            proc = subprocess.Popen(command_args,
                                    stdout=log_file,
                                    stderr=subprocess.STDOUT,
                                    cwd=cwd)
    else:
        proc = subprocess.Popen(command_args,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT,
                                cwd=cwd)
    if sync:
        stdout, stderr = proc.communicate()
        if stdout:
            logging.debug("stdout: {0}".format(stdout))
        if stderr:
            logging.warning("stderr: {0}".format(stderr))
        ret_code = proc.returncode
        logging.info("command_sync process exited with status: "
                     "{0}".format(ret_code))
        return ret_code
    else:
        pid = proc.pid
        logging.info("command_async process is running at pid: "
                     "{0}".format(pid))
        return pid

class ApolloConnection(object):
    def __init__(self, args):
        self.args = args

    def __enter__(self):
        self.apollo_conn = Stomp(
            StompConfig('tcp://{0}:{1}'.format(self.args['apollo_host'],
                                               self.args['apollo_port']),
                                               self.args['apollo_user'],
                                               self.args['apollo_password']))
        self.apollo_conn.connect()
        if self.args.get('input_topic'):
            logging.info("Subscribing to {0}".format(self.args['input_topic']))
            self.apollo_conn.subscribe(self.args['input_topic'],
                                       {StompSpec.ACK_HEADER:
                                        StompSpec.ACK_CLIENT_INDIVIDUAL})
        return self.apollo_conn

    def __exit__(self, type, value, tb):
        logging.info("Closing connection to {0}:{1}".format(
            self.args['apollo_host'],
            self.args['apollo_port']))
        self.apollo_conn.disconnect()

class WetwareException(Exception):
    pass

class ConfigException(WetwareException):
    pass

class FrameException(WetwareException):
    pass
