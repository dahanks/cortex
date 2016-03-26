# Worker
An Wetware Worker subscribes to a topic, waiting for messages.  Upon receiving a message, it does some amount of work based on what you define for it to do.  Finally (and optionally), it may publish results to another topic.

### Communication API
The Worker class supports synchronous and asynchronous communication with other Workers (or elements in Cortex, like Neuron).

### Other Builtin Features
By inheriting the Worker base class, you get a handful of other neat features.
* Basic message verification so you don't end up handling malformed messages.
* Reading in Apollo parameters from a config file (see worker.config as an example).
* Passing command line arguments to override those config file parameters.
* Static methods for running system commands:
  * command_sync - Run a synchronous linux command via your Worker (WOW, that's dangerous! We'll make this better)
  * command_async - Run asynchronous linux command
    * Add a filename to a 'log_file' field in the message to log the output to a file

### Optional Abilities You Can Add to Your Worker
In addition to defining the work your Worker does, Worker allows you to override and add to methods to allow you to:
* Add command line arguments for your Worker
* Specify default values for your config parameters.
* Add to message verification so you can assert that people aren't sending you garbage messages.

# Neuron-py
This library gives you the ability to communicate with Neuron in your Wetware worker.  There are some lightweight wrappers to encapsulate the API, which you may use or ignore at your will.

### Statements
Neuron expects all requests as an array under the key 'statements'.  The Statements() constructor will build this for you.  You can also pass Statements(<string>) to construct a quick Statements object to pass into a publish() or reply().

### Responses
Neuron will return its responses as a list of 'statements' as well.  But if you'd like to quickly convert that responses to a list, just use Responses(frame) in your worker callback.  You can iterate over that list as normal.
