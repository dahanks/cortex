.PHONY: build run

IMAGE=cortex-neuron
VERSION=1.0.0
FROM_IMAGE=$(shell grep FROM Dockerfile | cut -d " " -f 2)

build:
	docker build -t $(IMAGE) .
	docker tag $(IMAGE):latest $(IMAGE):$(VERSION)

run:
	docker run -d \
	--name neuron \
	--link cortex-cassandra \
	--link cortex-elasticsearch \
	--link cortex-apollo \
	$(IMAGE):$(VERSION)

shell:
	docker run -it \
	--name neuron \
	--link cortex-cassandra \
	--link cortex-elasticsearch \
	--link cortex-apollo \
	$(IMAGE):$(VERSION) \
	/bin/bash

update:
	docker pull $(FROM_IMAGE)
	$(MAKE) build
