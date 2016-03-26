# Worker
An Wetware Worker subscribes to a topic, waiting for messages.  Upon receiving a message, it does some amount of work based on what you define for it to do.  Finally (and optionally), it may publish results to another topic.

### Builtin Features
By inheriting the Worker base class, you get a handful of neat features.
* Publish any output you produce to any topic.  If no topic is specified, the OUTPUT_TOPIC you specify in your config file is used as default.
* Basic message verification so you don't end up handling malformed messages.
* Reading in Apollo parameters from a config file (see worker.config as an example).
* Passing command line arguments to override those config file parameters.
* Static methods for running system commands:
  * command_sync - Run a synchronous linux command via your Worker (WOW, that's dangerous! We'll make this better)
  * command_async - Run asynchronous linux command
    * Add a filename to a 'log_file' field in the message to log the output to a file

### Abilities You Can Add to Your Worker
In addition to defining the work your Worker does, Worker allows you to override and add to methods to allow you to:
* Add a specialized section in the config file for your Worker
* Add command line arguments for your Worker
* Add to message verification so you can assert that people aren't sending you garbage messages.
