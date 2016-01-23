package gov.nasa.jpl.audrey.cortex.neuron;

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanFactory;

public class Neuron {

    public static void main(String[] args) {
        try {
            TitanGraph graph = TitanFactory.build()
                    .set("storage.backend", "cassandra")
                    .set("hostname", "cs1")
                    .open();
            System.out.println("Hello!!!");
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        } finally {
            System.exit(0);
        }
    }

}
