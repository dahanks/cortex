FROM cortex-maven

RUN apt-get update && apt-get install unzip

RUN curl -o /opt/titan.zip http://s3.thinkaurelius.com/downloads/titan/titan-1.0.0-hadoop1.zip

RUN unzip /opt/titan.zip -d /opt/ && \
    rm /opt/titan.zip

ENV TITAN_HOME /opt/titan-1.0.0-hadoop1

WORKDIR $TITAN_HOME

ADD audrey.properties $TITAN_HOME/conf/audrey.properties

EXPOSE 8182
EXPOSE 8183
EXPOSE 8184

CMD ["/bin/bash", "bin/gremlin-server.sh"]