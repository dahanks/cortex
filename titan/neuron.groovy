import java.net.SocketTimeoutException;

import org.apache.activemq.transport.stomp.Stomp.Headers.Subscribe;
import org.apache.activemq.transport.stomp.StompConnection;
import org.apache.activemq.transport.stomp.StompFrame;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class NeuronException extends Exception {
    private static final long serialVersionUID = 1L;

    public NeuronException() { super(); }
    public NeuronException(String message) { super(message); }
    public NeuronException(String message, Throwable cause) { super(message, cause); }
    public NeuronException(Throwable cause) { super(cause); }
}

public class NeuronOperationException extends NeuronException {
    private static final long serialVersionUID = 1L;

    public NeuronOperationException() { super(); }
    public NeuronOperationException(String message) { super(message); }
    public NeuronOperationException(String message, Throwable cause) { super(message, cause); }
    public NeuronOperationException(Throwable cause) { super(cause); }
}

public class Neuron {
    public static final Logger logging = Logger.getRootLogger();

    //public static final String NEURON_HOME = System.getenv("NEURON_HOME");
    //public static final String AUDREY_CONF_PATH = NEURON_HOME + "/conf/audrey.properties";
    public static final String AUDREY_CONF_PATH = "conf/audrey.properties";

    public static final String APOLLO_HOSTNAME = "cortex-apollo";
    public static final int APOLLO_PORT = 61613;
    public static final String APOLLO_USERNAME = "admin";
    public static final String APOLLO_PASSWORD = "password";
    public static final String INPUT_DESTINATION = "/queue/neuron.operation";

    public static final String NEURON_OPERATION = "operation";
    //public static final String OPERATION_FUNCTION = "function";
    //public static final String FUNCTION_ADDVERTEX = "addVertex";
    //public static final String FUNCTION_ADDEDGE = "addEdge";
    //public static final String FUNCTION_ADDPROPERTY = "addProperty";
    //public static final String FUNCTION_ADDEDGE_TO = "to";
    //public static final String FUNCTION_ADDEDGE_FROM = "from";
    //public static final String FUNCTION_ADDEDGE_LABEL = "label";

    public graph;
    public StompConnection connection;

    public Neuron() { };

    public void initialize() throws Exception {
        graph = TitanFactory.open(AUDREY_CONF_PATH);

        connection = new StompConnection();
        connection.open(APOLLO_HOSTNAME, APOLLO_PORT);
        connection.connect(APOLLO_USERNAME, APOLLO_PASSWORD);
        connection.subscribe(INPUT_DESTINATION, Subscribe.AckModeValues.CLIENT);
    }

    public void onMessage(StompFrame frame) throws NeuronException {
        //TODO: move all this checking out into a verify() function
        JSONParser parser = new JSONParser();
        JSONObject message;
        try {
            message = (JSONObject) parser.parse(frame.getBody());
            if (message.containsKey(NEURON_OPERATION)) {
                runOperation(message);
            } else {
                throw new NeuronOperationException("Neuron received a valid JSON message without 'operation' field");
            }
        } catch (ParseException e) {
            logging.warn("Neuron could not parse a JSON message it received");
            e.printStackTrace();
        }
    }

    public void runOperation(JSONObject message) throws NeuronOperationException {
        //TODO: move all this checking out into a verify() function
        //if (message.get("operation") instanceof JSONArray) {
        //    parseOperations((JSONArray)message.get("operation"));
        //} else {
        //    throw new NeuronOperationException("Neuron received operation that was not a JSON Array");
        //}
        println message;
    }

    public void run() throws Exception {
        while (true) {
            try {
                StompFrame frame = connection.receive();
                onMessage(frame);
                connection.ack(frame);
            } catch (SocketTimeoutException e) {
                //timeouts are not fatal
                logging.warn("Timeout on connection to Apollo.  Trying again (infinitely)...");
            } catch (NeuronException e) {
                //malformed messages are not fatal
                e.printStackTrace();
            }
        }
    }
}

neuron = new Neuron();
try {
    neuron.initialize();
    neuron.run();
} catch (Throwable t) {
    t.printStackTrace();
} finally {
    neuron.finalize();
    //make sure we exit hard here; seen some weird behavior with Java in Docker
    System.exit(0);
}
