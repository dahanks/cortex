# Worker
An Wetware Worker subscribes to a topic, waiting for messages.  Upon receiving a message, it does some amount of work based on what you define for it to do.  Finally (and optionally), it may publish results to another topic.

## Communication API
The Worker class supports synchronous and asynchronous communication with other Workers (or elements in Cortex, like Neuron).

### Publishing messages
Use publish() to send a message to a topic or queue.  The following parameters allow you to control what happens next:
* If you don't specify the *topic*, you will publish the message to whatever topic/queue was specified in the config file.
* If you are expecting a response from you rececipient, you can specify a *callback* function to run when you get the response.  More on callbacks below.
* If you want to remember some context as to what's happening when you get to your callback, add that as *context*.

### Asynchronous calls
If you received a request for some work, and your worker needs help from something downstream, you can pass in the *transaction* to the publish() call.  You get this variable from the header of the on_message() method.  You should then specify a callback, and in the callback, you'll use the transaction again in the reply() (see below).

### Callbacks
Your callback function must abide by the following definition:

    def your_callback(frame, context, transaction):

### Reply
In synchronous workers, you're only ever handling one message at a time.  You can use reply() to send a response to whomever is waiting on you.

In asynchronous workers, you can still use reply() in your callback to respond to whomever is waiting on you.  Just pass in *transaction* from the callback definition.

## Other Builtin Features
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
