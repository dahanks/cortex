.PHONY: build run

IMAGE=cortex-neuron
VERSION=$(shell grep version ../wetware-py/setup.py | cut -d "'" -f 2)
FROM_IMAGE=$(shell grep FROM Dockerfile | cut -d " " -f 2)
CONTAINER=neuron

build:
	docker build -t $(IMAGE) .
	docker tag -f $(IMAGE):latest $(IMAGE):$(VERSION)

run:
	docker run -d \
	--name $(CONTAINER) \
	--link cortex-cassandra \
	--link cortex-elasticsearch \
	--link cortex-apollo \
	$(IMAGE):$(VERSION)

shell:
	docker run -it \
	--name $(CONTAINER) \
	--link cortex-cassandra \
	--link cortex-elasticsearch \
	$(IMAGE):$(VERSION) \
	/bin/bash

clean:
	docker rm -f $(CONTAINER)

update:
	docker pull $(FROM_IMAGE)
	$(MAKE) build
