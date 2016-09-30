FROM wetware-py:latest

RUN curl -sSL https://get.docker.com/ | sh

COPY . /usr/local/apollo-registrar
WORKDIR /usr/local/apollo-registrar

ENTRYPOINT ["python"]
CMD ["registrar.py", "--config", "registrar.config"]
