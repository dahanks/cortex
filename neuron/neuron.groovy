import java.net.SocketTimeoutException;

import org.apache.activemq.transport.stomp.Stomp.Headers.Subscribe;
import org.apache.activemq.transport.stomp.StompConnection;
import org.apache.activemq.transport.stomp.StompFrame;

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

    public void onMessage(StompFrame frame) {
        try {
            def parser = new JsonSlurper();
            def message = parser.parseText(frame.getBody());
            def api = message['api'];
            for (statement in message['statements']) {
                def fxns = statement['fxns'];
                if (fxns.size() == 1) {
                    if (api == "blueprints") {
                        println graph."${fxns[0]['fxn']}"(*fxns[0]['args']);
                    } else if (api == "gremlin") {
                        println g."${fxns[0]['fxn']}"(*fxns[0]['args']);
                    }
                } else if (fxns.size() == 2) {
                    if (api == "blueprints") {
                        println graph."${fxns[0]['fxn']}"(*fxns[0]['args'])."${fxns[1]['fxn']}"(*fxns[1]['args']);
                    } else if (api == "gremlin") {
                        println g."${fxns[0]['fxn']}"(*fxns[0]['args'])."${fxns[1]['fxn']}"(*fxns[1]['args']);
                    }
                } else if (fxns.size() == 3) {
                    if (api == "blueprints") {
                        println graph."${fxns[0]['fxn']}"(*fxns[0]['args'])."${fxns[1]['fxn']}"(*fxns[1]['args'])."${fxns[2]['fxn']}"(*fxns[2]['args']);
                    } else if (api == "gremlin") {
                        println g."${fxns[0]['fxn']}"(*fxns[0]['args'])."${fxns[1]['fxn']}"(*fxns[1]['args'])."${fxns[2]['fxn']}"(*fxns[2]['args']);
                    }
                } else if (fxns.size() == 4) {
                    if (api == "blueprints") {
                        println graph."${fxns[0]['fxn']}"(*fxns[0]['args'])."${fxns[1]['fxn']}"(*fxns[1]['args'])."${fxns[2]['fxn']}"(*fxns[2]['args'])."${fxns[3]['fxn']}"(*fxns[3]['args']);
                    } else if (api == "gremlin") {
                        println g."${fxns[0]['fxn']}"(*fxns[0]['args'])."${fxns[1]['fxn']}"(*fxns[1]['args'])."${fxns[2]['fxn']}"(*fxns[2]['args'])."${fxns[3]['fxn']}"(*fxns[3]['args']);
                    }
                } else if (fxns.size() == 5) {
                    if (api == "blueprints") {
                        println graph."${fxns[0]['fxn']}"(*fxns[0]['args'])."${fxns[1]['fxn']}"(*fxns[1]['args'])."${fxns[2]['fxn']}"(*fxns[2]['args'])."${fxns[3]['fxn']}"(*fxns[3]['args'])."${fxns[4]['fxn']}"(*fxns[4]['args']);
                    } else if (api == "gremlin") {
                        println g."${fxns[0]['fxn']}"(*fxns[0]['args'])."${fxns[1]['fxn']}"(*fxns[1]['args'])."${fxns[2]['fxn']}"(*fxns[2]['args'])."${fxns[3]['fxn']}"(*fxns[3]['args'])."${fxns[4]['fxn']}"(*fxns[4]['args']);
                    }
                } else if (fxns.size() == 6) {
                    if (api == "blueprints") {
                        println graph."${fxns[0]['fxn']}"(*fxns[0]['args'])."${fxns[1]['fxn']}"(*fxns[1]['args'])."${fxns[2]['fxn']}"(*fxns[2]['args'])."${fxns[3]['fxn']}"(*fxns[3]['args'])."${fxns[4]['fxn']}"(*fxns[4]['args'])."${fxns[5]['fxn']}"(*fxns[5]['args']);
                    } else if (api == "gremlin") {
                        println g."${fxns[0]['fxn']}"(*fxns[0]['args'])."${fxns[1]['fxn']}"(*fxns[1]['args'])."${fxns[2]['fxn']}"(*fxns[2]['args'])."${fxns[3]['fxn']}"(*fxns[3]['args'])."${fxns[4]['fxn']}"(*fxns[4]['args'])."${fxns[5]['fxn']}"(*fxns[5]['args']);
                    }
                } else if (fxns.size() == 7) {
                    if (api == "blueprints") {
                        println graph."${fxns[0]['fxn']}"(*fxns[0]['args'])."${fxns[1]['fxn']}"(*fxns[1]['args'])."${fxns[2]['fxn']}"(*fxns[2]['args'])."${fxns[3]['fxn']}"(*fxns[3]['args'])."${fxns[4]['fxn']}"(*fxns[4]['args'])."${fxns[5]['fxn']}"(*fxns[5]['args'])."${fxns[6]['fxn']}"(*fxns[6]['args']);
                    } else if (api == "gremlin") {
                        println g."${fxns[0]['fxn']}"(*fxns[0]['args'])."${fxns[1]['fxn']}"(*fxns[1]['args'])."${fxns[2]['fxn']}"(*fxns[2]['args'])."${fxns[3]['fxn']}"(*fxns[3]['args'])."${fxns[4]['fxn']}"(*fxns[4]['args'])."${fxns[5]['fxn']}"(*fxns[5]['args'])."${fxns[6]['fxn']}"(*fxns[6]['args']);
                    }
                } else if (fxns.size() == 8) {
                    if (api == "blueprints") {
                        println graph."${fxns[0]['fxn']}"(*fxns[0]['args'])."${fxns[1]['fxn']}"(*fxns[1]['args'])."${fxns[2]['fxn']}"(*fxns[2]['args'])."${fxns[3]['fxn']}"(*fxns[3]['args'])."${fxns[4]['fxn']}"(*fxns[4]['args'])."${fxns[5]['fxn']}"(*fxns[5]['args'])."${fxns[6]['fxn']}"(*fxns[6]['args'])."${fxns[7]['fxn']}"(*fxns[7]['args']);
                    } else if (api == "gremlin") {
                        println g."${fxns[0]['fxn']}"(*fxns[0]['args'])."${fxns[1]['fxn']}"(*fxns[1]['args'])."${fxns[2]['fxn']}"(*fxns[2]['args'])."${fxns[3]['fxn']}"(*fxns[3]['args'])."${fxns[4]['fxn']}"(*fxns[4]['args'])."${fxns[5]['fxn']}"(*fxns[5]['args'])."${fxns[6]['fxn']}"(*fxns[6]['args'])."${fxns[7]['fxn']}"(*fxns[7]['args']);
                    }
                } else if (fxns.size() == 9) {
                    if (api == "blueprints") {
                        println graph."${fxns[0]['fxn']}"(*fxns[0]['args'])."${fxns[1]['fxn']}"(*fxns[1]['args'])."${fxns[2]['fxn']}"(*fxns[2]['args'])."${fxns[3]['fxn']}"(*fxns[3]['args'])."${fxns[4]['fxn']}"(*fxns[4]['args'])."${fxns[5]['fxn']}"(*fxns[5]['args'])."${fxns[6]['fxn']}"(*fxns[6]['args'])."${fxns[7]['fxn']}"(*fxns[7]['args'])."${fxns[8]['fxn']}"(*fxns[8]['args']);
                    } else if (api == "gremlin") {
                        println g."${fxns[0]['fxn']}"(*fxns[0]['args'])."${fxns[1]['fxn']}"(*fxns[1]['args'])."${fxns[2]['fxn']}"(*fxns[2]['args'])."${fxns[3]['fxn']}"(*fxns[3]['args'])."${fxns[4]['fxn']}"(*fxns[4]['args'])."${fxns[5]['fxn']}"(*fxns[5]['args'])."${fxns[6]['fxn']}"(*fxns[6]['args'])."${fxns[7]['fxn']}"(*fxns[7]['args'])."${fxns[8]['fxn']}"(*fxns[8]['args']);
                    }
                } else if (fxns.size() == 10) {
                    if (api == "blueprints") {
                        println graph."${fxns[0]['fxn']}"(*fxns[0]['args'])."${fxns[1]['fxn']}"(*fxns[1]['args'])."${fxns[2]['fxn']}"(*fxns[2]['args'])."${fxns[3]['fxn']}"(*fxns[3]['args'])."${fxns[4]['fxn']}"(*fxns[4]['args'])."${fxns[5]['fxn']}"(*fxns[5]['args'])."${fxns[6]['fxn']}"(*fxns[6]['args'])."${fxns[7]['fxn']}"(*fxns[7]['args'])."${fxns[8]['fxn']}"(*fxns[8]['args'])."${fxns[9]['fxn']}"(*fxns[9]['args']);
                    } else if (api == "gremlin") {
                        println g."${fxns[0]['fxn']}"(*fxns[0]['args'])."${fxns[1]['fxn']}"(*fxns[1]['args'])."${fxns[2]['fxn']}"(*fxns[2]['args'])."${fxns[3]['fxn']}"(*fxns[3]['args'])."${fxns[4]['fxn']}"(*fxns[4]['args'])."${fxns[5]['fxn']}"(*fxns[5]['args'])."${fxns[6]['fxn']}"(*fxns[6]['args'])."${fxns[7]['fxn']}"(*fxns[7]['args'])."${fxns[8]['fxn']}"(*fxns[8]['args'])."${fxns[9]['fxn']}"(*fxns[9]['args']);
                    }
                } else if (fxns.size() < -1) {
                    logging.warn("Received Gremlin operation with no function calls");
                } else if (fxns.size() > 10) {
                    logging.warn("Neuron can not process Gremlin operations with greater than 10 linked calls");
                }
            }
        } catch (JsonException e) {
            logging.warn("Neuron received invalid JSON message");
        } catch (Exception e) {
            e.printStackTrace();
            logging.warn("Neuron failed to run operation; probably invalid Gremlin");
        } finally {
            connection.ack(frame);
        }
    }

    public void run() {
        while (true) {
            try {
                StompFrame frame = connection.receive();
                onMessage(frame);
                //acking is done inside the above method
            } catch (SocketTimeoutException e) {
                //timeouts are not fatal
                logging.warn("Timeout on connection to Apollo.  Trying again (infinitely)...");
            }
        }
    }
}

neuron = new Neuron();
neuron.initialize();
neuron.run();
