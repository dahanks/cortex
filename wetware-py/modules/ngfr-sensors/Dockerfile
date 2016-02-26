FROM wetware-py:latest

COPY . /usr/local/wetware-ngfr-sensors
WORKDIR /usr/local/wetware-ngfr-sensors

ENTRYPOINT ["python"]
CMD ["ngfr-sensors.py", "--config", "ngfr-sensors.config"]
