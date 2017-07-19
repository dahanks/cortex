FROM wetware-py:latest

COPY . /usr/local/wetware-ake-bridge
WORKDIR /usr/local/wetware-ake-bridge

ENTRYPOINT ["python"]
CMD ["ake_bridge.py", "--config", "ake_bridge.config"]
