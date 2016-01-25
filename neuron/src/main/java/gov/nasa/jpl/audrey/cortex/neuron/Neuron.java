package gov.nasa.jpl.audrey.cortex.neuron;

import java.util.ArrayList;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanFactory;


public class Neuron {

    public static void main(String[] args) {
        try {
            Configuration conf = new PropertiesConfiguration("/opt/titan-1.0.0-hadoop1/conf/audrey.properties");
            TitanGraph graph = TitanFactory.open(conf);
            GraphTraversalSource g = graph.traversal();
            GraphTraversal<Vertex, Vertex> gt = g.V();
            ArrayList<Vertex> vertices = new ArrayList<Vertex>();
            while (gt.hasNext()) {
                vertices.add(gt.next());
            }
            for (Vertex vertex : vertices) {
                System.out.println(vertex.property("name"));
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        } finally {
            System.exit(0);
        }
    }

}
