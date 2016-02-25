# ApolloWorker
An ApolloWorker subscribes to a topic, waiting for messages.  Upon receiving a message, it does some amount of work based on what you define for it to do.  Finally (and optionally), it may publish results to another topic.

### Builtin Features
By inheriting the ApolloWorker base class, you get a handful of neat features.
* Basic operations over Apollo message (add these to the 'operation' field of your Apollo message):
  * quit - Terminate your Worker via Apollo
  * command_sync - Run a synchronous linux command via your Worker (WOW, that's dangerous! We'll make this better)
  * command_async - Run asynchronous linux command
    * Add a filename to a 'log_file' field in the message to log the output to a file
* Publish any output you produce to any topic.  If no topic is specified, the OUTPUT_TOPIC you specify in your config file is used as default.
* Basic message verification so you don't end up handling malformed messages.
* Reading in Apollo parameters from a config file (see worker.config as an example).
* Passing command line arguments to override those config file parameters.

### Abilities You Can Add to Your Worker
In addition to defining the work your Worker does, ApolloWorker allows you to override and add to methods to allow you to:
* Add a specialized section in the config file for your Worker
* Add command line arguments for your Worker
* Add to message verification so you can assert that people aren't sending you garbage messages.

# HOWTO
### Installation
Easiest way to install ApolloWorker is to clone this repo, then run `pip install apollo_worker`.  (Use virtual environments if you're hygienic!)

### Creating your own Worker subclass
(This section is copied from the docstring of example_worker.py.  Use that file as a reference for the below instructions.)

So you want to create a Worker class that talks over Apollo.  Here's
what you need to do:

1. Define a method to do the work you need to do when you get a message
In this example, the method is "do_my_special_work"

2. Override "on_message(self, frame)"
In the body of the message, call the method(s) you wrote to do the work
while handling the message however you need to.

3. Create an instance of your new subclass
In your main(), create an instance of your new subclass.  Pass in as an
argument a string that represents your class.  If you need to add parameters
to a config file relevant to your class, do it in a section with the string
you passed.

4. Add any configuration parameters you may need
Add to the worker.config file a section with the string you passed, and add
any new parameters under there that you may need.

5. (OPTIONAL) Override "define_default_args(self)"
If you want to add default values for the new parameters you added to the
config, you can do that in this method (more instructions inside the method).

6. (OPTIONAL) Override "add_argparse_args(self)"
If you want to be able to override config values (specific to your new class)
by passing command line arguments, add those parameters to this method (more
instructions inside the method).

7. (OPTIONAL) Override "verify_frame(self, frame)"
If you want to handle errors when you get a message that doesn't look like
you expect, add the handling in this method (more instructions inside the   
method).
