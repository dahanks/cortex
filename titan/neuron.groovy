import java.net.SocketTimeoutException;

import org.apache.activemq.transport.stomp.Stomp.Headers.Subscribe;
import org.apache.activemq.transport.stomp.StompConnection;
import org.apache.activemq.transport.stomp.StompFrame;

public class NeuronException extends Exception {
    private static final long serialVersionUID = 1L;

    public NeuronException() { super(); }
    public NeuronException(String message) { super(message); }
    public NeuronException(String message, Throwable cause) { super(message, cause); }
    public NeuronException(Throwable cause) { super(cause); }
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

    public StompConnection connection;
    public graph;
    public g;

    public Neuron() { };

    public void initialize() throws Exception {
        graph = TitanFactory.open(AUDREY_CONF_PATH);
        g = graph.traversal();
        connection = new StompConnection();
        connection.open(APOLLO_HOSTNAME, APOLLO_PORT);
        connection.connect(APOLLO_USERNAME, APOLLO_PASSWORD);
        connection.subscribe(INPUT_DESTINATION, Subscribe.AckModeValues.CLIENT);
    }

    public void onMessage(StompFrame frame) throws NeuronException {
        //TODO: move all this checking out into a verify() function
        try {
            def parser = new JsonSlurper();
            def message = parser.parseText(frame.getBody());
            def operation = message['operation'];
            println graph."$operation"("name","david");
            println g.V().values("name").next();
            //graph.addVertex("name", message['operation']);
        } catch (JsonException e) {
            logging.warn("Neuron received invalid JSON message");
        } catch (Exception e) {
            e.printStackTrace();
            logging.warn("Neuron failed to run operation; probably invalid Gremlin");
        }
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
neuron.initialize();
neuron.run();
k