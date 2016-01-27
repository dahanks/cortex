FROM cortex-maven

ENV NEURON_HOME /usr/local/neuron

ADD pom.xml $NEURON_HOME/pom.xml
ADD src/ $NEURON_HOME/src
ADD audrey.properties $NEURON_HOME/conf/audrey.properties

WORKDIR $NEURON_HOME

RUN mvn package

CMD ["mvn", "exec:java", "-D", "exec.mainClass=gov.nasa.jpl.audrey.cortex.neuron.Neuron"]