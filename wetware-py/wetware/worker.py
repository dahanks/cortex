#!/usr/bin/env python

import sys
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
        self.reply_topics = []
        self.reply_subs = []

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
                logging.error("Your config does not have a '{0}' section"
                              " for your Worker subclass!".format(subclass_section))
                raise

        return config_dict

    def run(self):
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
        """Handles incoming messages and runs operations (OVERRIDE and SUPER)

        Handles the following operations in the base class:
            'quit' and 'exit'
            'command_sync'
            'command_async'

        This method is designed to be overridden (call super first!) with
        additional operations.

        If the message received expects a reply, the temp queue topic will
        be placed in a queue (a queue of temp queue topics, yes).  To reply
        to the requestor, call self.reply() instead of self.publish().

        Returns False if message fails verification.
        Otherwise returns the message frame so subclass has access to it on
        overriding this function.
        """
        # must ack to remove from queue
        self.apollo_conn.ack(frame)

        # check if this is something you need to reply to
        # NOTE: YOU MUST NOT REPLY if you're not expected to!
        #  Since this is a list (queue), if you reply(), you might
        #  be replying to somebody else's request
        if 'reply-to' in frame.headers:
            self.reply_topics.append(frame.headers['reply-to'])

        # Raise FrameException if frame is bad
        try:
            self.verify_frame(frame)
            # check if this is a reply you're waiting on
            if frame.headers['destination'].startswith('/queue/temp'):
                # the destination we used to subscribe looks a little different
                #  than the destination coming in this time; hence, the weird
                #  tuple check with string concatentation below.
                temp_sub = str(frame.headers['destination'].split('.')[-1])
                try:
                    sub_index = self.reply_subs.index(('destination',
                                                       '/temp-queue/' + temp_sub))
                except ValueError:
                    logging.error("Somehow you got a message on a temp queue"
                                  " that you lost track of.  That is very weird"
                                  " so let's cut our losses.")
                    raise
                self.apollo_conn.unsubscribe(self.reply_subs.pop(sub_index))
                self.handle_reply(frame)
        except FrameException, e:
            # Frame not verified; send an error in reply (if expected)
            #  otherwise, just skip it and continue outside loop...
            if 'reply-to' in frame.headers:
                self.reply({ 'Error': 'Frame failed verification' })
            raise

        # returning frame so subclass can have it when overriding this function
        return frame

        ###No, I don't think we want to allow this remote access right now...
        # if 'operation' in message:
        #     operation = message['operation'].lower()
        #     if operation == "quit" or operation == "exit":
        #         logging.info("Received command to exit. Disconnecting...")
        #         sys.exit(0)

        #     # TODO: need some security to messages that can run arbitrary cmds!
        #     elif operation.startswith("command"):
        #         command = message['command']
        #         command_args = command.split()
        #         # log if the message contained a log_file; otherwise, don't log
        #         log = None if 'log_file' not in message else message['log_file']
        #         if operation == 'command_sync':
        #             logging.info("Received message to run a synchronous command")
        #             return_code = run_command(command_args, sync=True, log_file=log)
        #         elif operation == 'command_async':
        #             logging.info("Received message to run an asynchronous command")
        #             pid = run_command(command_args, sync=False, log_file=log)

    def handle_reply(self, frame):
        """Handles a reply over a temp queue to a request you already submitted

        This is intended to be a virtual method that you *don't* need to
        override.  That is, you don't need to override this if you never expect
        to make request that don't require replies.  However, if you *do* need
        to override this, you wouldn't ever need to call this super().  So by
        implementing the method here, you won't get an error simply by
        instantiating the object.
        """
        logging.info("Got a reply!")
        logging.info(frame)

    def verify_frame(self, frame):
        """Verify a frame (OVERRIDE and SUPER)

        Verify that a frame is valid JSON and has the appropriate fields.
        This function may be overrided by a subclass to add to the verification
        but it must call the SUPER() first.

        Returns True/False when validation passes/fails.
        """
        try:
            message = json.loads(frame.body)
            #handle "operation" messages for sync and async commands
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
        except ValueError:
            # raising FrameException so we can skip it--this shouldn't be fatal
            raise FrameException({'message': "Received an invalid JSON object in message",
                                   'info': frame.info(),
                                   'body': frame.body})

    """Publish a message to a topic using your Apollo Connection

    If no topic is supplied, we assume you want to publish output to the
    topic you configured with your OUTPUT_TOPIC parameter in your config
    file. You know, cause we're nice.

    Setting expect_reply to True establishes a subscription to a temp-queue
    for your response.  When you get the response, this worker will call
    self.handle_reply() to handle it, and the worker will unsubscribe from
    the temp queue.

    Returns False if you never specified an OUTPUT_TOPIC.
    """
    def publish(self, message, topic=None, expect_reply=False):
        # If you pass a dict, we'll convert it to JSON for you
        if isinstance(message, dict):
            message_str = json.dumps(message)
        # Otherwise, we'll try to cast whatever you passed as a string
        else:
            message_str = str(message)

        # Hopefully you've initialized your Apollo connection via run()
        if self.apollo_conn:
            # If no topic is provided, use the output_topic from config file
            if not topic:
                # But, of course, make sure there is one in the config
                # If not, return early
                if 'output_topic' not in self.args:
                    raise ConfigException("Tried to publish a message but there is no"
                                    " output_topic specified in the config!")
                else:
                    topic = self.args['output_topic']
            # Just check to see if we've specified a topic at this point.  This
            # is totally redundant, but feels safer.
            if topic:
                if expect_reply:
                    temp_uuid = str(UUID())
                    self.reply_subs.append(
                        self.apollo_conn.subscribe('/temp-queue/' + temp_uuid,
                                                   {StompSpec.ACK_HEADER:
                                                    StompSpec.ACK_CLIENT_INDIVIDUAL}))
                    self.apollo_conn.send(topic, message_str,
                                          headers={'reply-to': '/temp-queue/' + temp_uuid})
                else:
                    self.apollo_conn.send(topic, message_str)
        else:
            logging.warning("Tried to publish a message but there is no Apollo"
                            " connection! (Did you call run() on your Worker?)")

    def reply(self, message):
        """Send a reply to the last person who was expecting it.

        Call this after you've received a message expecting a reply and you've
        done the work to send in the reply.  This takes the longest-waiting
        reply topic out of the queue, so make sure you're replying to things
        in order!
        """
        try:
            self.publish(message, self.reply_topics.pop(0))
        except IndexError:
            logging.warning("Tried to reply when no one was expecting a reply!"
                            " Be careful: whatever you're doing may cause you"
                            " to reply to the wrong thing in the future!")

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

class WorkerException(Exception):
    pass

class ConfigException(WorkerException):
    pass

class FrameException(WorkerException):
    pass
