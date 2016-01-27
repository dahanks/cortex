FROM cortex-maven

ADD pom.xml /usr/local/neuron/pom.xml
ADD src/ /usr/local/neuron/src
ADD audrey.properties /usr/local/neuron/conf/audrey.properties

WORKDIR /usr/local/neuron

RUN mvn package

CMD ["mvn", "exec:java", "-D", "exec.mainClass=gov.nasa.jpl.audrey.cortex.neuron.Neuron"]