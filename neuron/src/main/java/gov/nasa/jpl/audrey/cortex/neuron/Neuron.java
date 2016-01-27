package gov.nasa.jpl.audrey.cortex.neuron;

import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.ArrayList;

import org.apache.activemq.transport.stomp.Stomp.Headers.Subscribe;
import org.apache.activemq.transport.stomp.StompConnection;
import org.apache.activemq.transport.stomp.StompFrame;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanFactory;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class Neuron {

    public static void main(String[] args) throws Exception {
        //set up graph connection
        Configuration conf = new PropertiesConfiguration("/usr/local/neuron/conf/audrey.properties");
        TitanGraph graph = TitanFactory.open(conf);
        GraphTraversalSource g = graph.traversal();

        //set up STOMP connection
        StompConnection connection = new StompConnection();
        try {
            connection.open("cortex-apollo", 61613);
            connection.connect("admin", "password");
            connection.subscribe("/topic/gremlin", Subscribe.AckModeValues.CLIENT);
        } catch (UnknownHostException e) {
            //make super sure we exit cleanly
            e.printStackTrace();
            System.exit(1);
        }

        JSONParser parser = new JSONParser();

        while (true) {
            try {
                StompFrame message = connection.receive();
                JSONObject json = (JSONObject) parser.parse(message.getBody());
                if (json.containsKey("operation")) {
                    if (json.get("operation").equals("getVertices")) {
                        GraphTraversal<Vertex, Vertex> gt = g.V();
                        ArrayList<Vertex> vertices = new ArrayList<Vertex>();
                        while (gt.hasNext()) {
                            vertices.add(gt.next());
                        }
                        for (Vertex vertex : vertices) {
                            System.out.println(vertex.property("name"));
                        }
                    } else {
                        System.out.println("Message has no 'operation' field!");
                    }
                }
                connection.ack(message);
            } catch (SocketTimeoutException e) {
                System.out.println("Timeout...");
            }
        }
        //connection.disconnect();
    }
}
