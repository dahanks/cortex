package gov.nasa.jpl.audrey.cortex.neuron;

import java.net.SocketTimeoutException;
import java.util.ArrayList;
import org.apache.activemq.transport.stomp.Stomp.Headers.Subscribe;
import org.apache.activemq.transport.stomp.StompConnection;
import org.apache.activemq.transport.stomp.StompFrame;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanFactory;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class Neuron {
    public static final Logger logging = Logger.getRootLogger();

    private static final String NEURON_HOME = System.getenv("NEURON_HOME");
    private static final String AUDREY_CONF_PATH = NEURON_HOME + "/conf/audrey.properties";
    private static final String APOLLO_HOSTNAME = "cortex-apollo";
    private static final int APOLLO_PORT = 61613;
    private static final String APOLLO_USERNAME = "admin";
    private static final String APOLLO_PASSWORD = "password";
    private static final String INPUT_DESTINATION = "/queue/neuron.operation";

    private TitanGraph graph;
    private StompConnection connection;

    public Neuron() { }

    private void initialize() throws Exception {
        Configuration conf = new PropertiesConfiguration(AUDREY_CONF_PATH);
        graph = TitanFactory.open(conf);

        connection = new StompConnection();
        connection.open(APOLLO_HOSTNAME, APOLLO_PORT);
        connection.connect(APOLLO_USERNAME, APOLLO_PASSWORD);
        connection.subscribe(INPUT_DESTINATION, Subscribe.AckModeValues.CLIENT);
    }

    @Override
    public void finalize() throws Throwable {
        try {
            if (graph != null) {
                graph.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            super.finalize();
        }
    }

    private void onMessage(StompFrame frame) throws NeuronException {
        JSONParser parser = new JSONParser();
        JSONObject message;
        try {
            message = (JSONObject) parser.parse(frame.getBody());
            if (message.containsKey("operation")) {
                runOperation(message);
            } else {
                throw new NeuronOperationException("Neuron received a valid JSON message without 'operation' field");
            }
        } catch (ParseException e) {
            logging.warn("Neuron could not parse a JSON message it received");
            e.printStackTrace();
        }
    }

    private void runOperation(JSONObject message) throws NeuronOperationException {
        if (message.get("operation") instanceof JSONArray) {
            parseOperations((JSONArray)message.get("operation"));
        } else {
            throw new NeuronOperationException("Neuron received operation that was not a JSON Array");
        }
    }

    private void parseOperations(JSONArray operations) throws NeuronOperationException {
        for (int i = 0; i < operations.size(); ++i) {
            if (!(operations.get(i) instanceof JSONObject)) {
                throw new NeuronOperationException("An operation was parsed that was not a valid JSONObject");
            }
            JSONObject operation = (JSONObject)operations.get(i);
            if (!operation.containsKey("function")) {
                throw new NeuronOperationException("An operation was parsed that did not have a 'function' key");
            }
            switch (operation.get("function").toString()) {
            case "addVertex":
                addVertex(operation);
                break;
            case "addEdge":
                break;
            case "addProperty":
                break;
            //this case is really just for debugging
            case "getVertices":
                GraphTraversalSource g = graph.traversal();
                GraphTraversal<Vertex, Vertex> gt = g.V();
                ArrayList<Vertex> vertices = new ArrayList<Vertex>();
                while (gt.hasNext()) {
                    vertices.add(gt.next());
                }
                for (Vertex vertex : vertices) {
                    System.out.println(vertex.property("name"));
                }
                break;
            default:
                throw new NeuronOperationException("Neuron received operation with unrecognized function");
            }
        }
    }

    private void addVertex(JSONObject operation) {
        return;
    }

    private void run() throws Exception {
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

    public static void main(String[] args) throws Throwable {
        Neuron neuron = new Neuron();
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
    }
}
