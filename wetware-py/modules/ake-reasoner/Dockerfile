FROM wetware-py:latest

COPY . /usr/local/wetware-ake-reasoner
WORKDIR /usr/local/wetware-ake-reasoner

ENTRYPOINT ["python"]
CMD ["ake_reasoner.py", "--config", "ake_reasoner.config"]
