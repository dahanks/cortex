FROM wetware-py:latest

COPY . /usr/local/publisher
WORKDIR /usr/local/publisher

ENTRYPOINT ["python"]
CMD ["publisher.py", "--config", "publisher.config"]
