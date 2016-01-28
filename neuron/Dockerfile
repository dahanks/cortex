FROM cortex-titan

RUN curl -o $TITAN_HOME/lib/activemeq-all-5.13.0.jar http://central.maven.org/maven2/org/apache/activemq/activemq-all/5.13.0/activemq-all-5.13.0.jar

ADD audrey.properties $TITAN_HOME/conf/audrey.properties
ADD neuron.groovy $TITAN_HOME/scripts/neuron.groovy

CMD ["/bin/bash", "bin/gremlin.sh", "scripts/neuron.groovy"]