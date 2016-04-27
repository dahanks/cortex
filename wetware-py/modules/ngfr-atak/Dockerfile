FROM wetware-py:latest

COPY . /usr/local/wetware-ngfr-atak
WORKDIR /usr/local/wetware-ngfr-atak

ENTRYPOINT ["python"]
CMD ["ngfr-atak.py", "--config", "ngfr-atak.config"]
