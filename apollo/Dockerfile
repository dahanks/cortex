FROM java:8

RUN apt-get update -y \
  && apt-get install --no-install-recommends -y -q curl

ENV APOLLO_VERSION 1.7.1
ENV APOLLO apache-apollo-$APOLLO_VERSION
ENV APOLLO_TAR $APOLLO-unix-distro.tar.gz
ENV APOLLO_HOME /opt/apollo
ENV APOLLO_USER apollo

RUN \
    curl -s -O http://archive.apache.org/dist/activemq/activemq-apollo/$APOLLO_VERSION/$APOLLO_TAR && \
    mkdir -p /opt && \
    tar xzf $APOLLO_TAR -C /opt/ && \
    rm $APOLLO_TAR && \
    ln -s /opt/$APOLLO $APOLLO_HOME && \
    useradd -r -M -d $APOLLO_HOME $APOLLO_USER && \
    chown $APOLLO_USER:$APOLLO_USER /opt/$ACTIVEMQ -R

ENV APOLLO_BROKER /opt/apollo-broker

RUN $APOLLO_HOME/bin/apollo create $APOLLO_BROKER

COPY apollo.xml $APOLLO_BROKER/etc/

COPY users.properties $APOLLO_BROKER/etc/

EXPOSE 61613 61623

ENTRYPOINT ["/opt/apollo-broker/bin/apollo-broker"]
CMD ["run"]