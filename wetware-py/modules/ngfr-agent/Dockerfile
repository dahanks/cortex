FROM wetware-py:latest

COPY . /usr/local/wetware-ngfr-agent
WORKDIR /usr/local/wetware-ngfr-agent

ENTRYPOINT ["python"]
CMD ["agent1wet.py", "--config", "agent1wet.config"]
