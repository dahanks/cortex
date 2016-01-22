FROM cortex-titan

ADD pom.xml /usr/local/neuron/pom.xml
ADD src/ /usr/local/neuron/src

WORKDIR /usr/local/neuron

RUN mvn package

CMD ["java", "-cp", "target/neuron-1.0-SNAPSHOT.jar", "gov.nasa.jpl.audrey.cortex.neuron.App"]