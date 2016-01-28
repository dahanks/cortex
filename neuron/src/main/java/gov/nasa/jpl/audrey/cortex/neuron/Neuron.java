package gov.nasa.jpl.audrey.cortex.neuron;

import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.activemq.transport.stomp.Stomp.Headers.Subscribe;
import org.apache.activemq.transport.stomp.StompConnection;
import org.apache.activemq.transport.stomp.StompFrame;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.TitanVertex;

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

    private static final String NEURON_OPERATION = "operation";
    private static final String OPERATION_FUNCTION = "function";
    private static final String FUNCTION_ADDVERTEX = "addVertex";
    private static final String FUNCTION_ADDEDGE = "addEdge";
    private static final String FUNCTION_ADDPROPERTY = "addProperty";
    private static final String FUNCTION_ADDEDGE_TO = "to";
    private static final String FUNCTION_ADDEDGE_FROM = "from";
    private static final String FUNCTION_ADDEDGE_LABEL = "label";

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

    private void runOperation(JSONObject message) throws NeuronOperationException {
        //TODO: move all this checking out into a verify() function
        if (message.get("operation") instanceof JSONArray) {
            parseOperations((JSONArray)message.get("operation"));
        } else {
            throw new NeuronOperationException("Neuron received operation that was not a JSON Array");
        }
    }

    private void parseOperations(JSONArray operations) throws NeuronOperationException {
        //TODO: move all this checking out into a verify() function
        for (int i = 0; i < operations.size(); ++i) {
            if (!(operations.get(i) instanceof JSONObject)) {
                throw new NeuronOperationException("An operation was parsed that was not a valid JSONObject");
            }
            JSONObject operation = (JSONObject)operations.get(i);
            if (!operation.containsKey(OPERATION_FUNCTION)) {
                throw new NeuronOperationException("An operation was parsed that did not have a '" + OPERATION_FUNCTION + "' key");
            }
            switch (operation.get(OPERATION_FUNCTION).toString()) {
            case FUNCTION_ADDVERTEX:
                addVertex(operation);
                break;
            case FUNCTION_ADDEDGE:
                addEdge(operation);
                break;
            case FUNCTION_ADDPROPERTY:
                break;
            //TODO: remove this, this case is really just for debugging
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
        //start the transaction by creating the vertex
        TitanVertex newVertex = graph.addVertex();
        //add a property for every key in this operation (except 'function')
        for (Iterator<?> keys = operation.keySet().iterator(); keys.hasNext();) {
            Object keyObj = keys.next();
            if (keyObj instanceof String && !(keyObj.toString().equals(OPERATION_FUNCTION))) {
                String key = keyObj.toString();
                newVertex.property(key, operation.get(key));
            }
        }
        logging.debug("Adding Vertex");
        graph.tx().commit();
    }

    private void addEdge(JSONObject operation) throws NeuronOperationException {
        //TODO: move all this checking out into a verify() function
        //need to find the TO and FROM first
        if (!operation.containsKey(FUNCTION_ADDEDGE_TO) || !(operation.get(FUNCTION_ADDEDGE_TO) instanceof String) ||
                !operation.containsKey(FUNCTION_ADDEDGE_FROM) || !(operation.get(FUNCTION_ADDEDGE_FROM) instanceof String) ||
                !operation.containsKey(FUNCTION_ADDEDGE_LABEL) || !(operation.get(FUNCTION_ADDEDGE_LABEL) instanceof String)) {
            throw new NeuronOperationException("Received addEdge operation without missing or invalid TO, FROM, or LABEL Strings");
        }
        //TODO: replace this with a Gremlin Pipe, if you can figure it out
        //TODO: don't just base it off of 'name', use something more generic
        GraphTraversal <Vertex, Vertex> fromVertices = graph.traversal().V().has("name", operation.get(FUNCTION_ADDEDGE_FROM));
        ArrayList<TitanVertex> toVertices = getVertices(graph.traversal().V().has("name", operation.get(FUNCTION_ADDEDGE_TO)));
        //add all edge properties to a list
        ArrayList<String> edgeProperties = new ArrayList<String>();
        for (Iterator<?> keys = operation.keySet().iterator(); keys.hasNext();) {
            Object keyObj = keys.next();
            //TODO: move all this checking out into a verify() function
            if ((keyObj instanceof String) && !(keyObj.toString().equals(OPERATION_FUNCTION)) &&
                    !(keyObj.toString().equals(FUNCTION_ADDEDGE_TO)) && !(keyObj.toString().equals(FUNCTION_ADDEDGE_FROM)) &&
                    !(keyObj.toString().equals(FUNCTION_ADDEDGE_LABEL))) {
                edgeProperties.add(keyObj.toString());
            }
        }
        //create edges from all FROMs to all TOs
        while (fromVertices.hasNext()) {
            TitanVertex fromVertex = (TitanVertex) fromVertices.next();
            for (TitanVertex toVertex : toVertices) {
                TitanTransaction tx = graph.newTransaction();
                TitanEdge edge = fromVertex.addEdge(operation.get(FUNCTION_ADDEDGE_LABEL).toString(), (Vertex)toVertex);
                tx.commit();
                System.out.println(edge);
/*                for (String property : edgeProperties) {
                    edge.property(property, operation.get(property).toString());
                }
*/                //TODO: can I move this commit out until after we create all the edges?
                graph.tx().commit();
            }
        }
        logging.debug("Adding Edge");
    }

    private ArrayList<TitanVertex> getVertices(GraphTraversal<Vertex, Vertex> graphTraversal) {
        ArrayList<TitanVertex> vertices = new ArrayList<TitanVertex>();
        while (graphTraversal.hasNext()) {
            vertices.add((TitanVertex)graphTraversal.next());
        }
        return vertices;
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
