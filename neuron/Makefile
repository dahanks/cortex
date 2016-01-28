.PHONY: build run

IMAGE=cortex-neuron
VERSION=1.0.0
FROM_IMAGE=$(shell grep FROM Dockerfile | cut -d " " -f 2)

build:
	docker build -t $(IMAGE) .
	docker tag -f $(IMAGE):latest $(IMAGE):$(VERSION)

run:
	docker run -d \
	--name $(IMAGE) \
	--link cortex-cassandra \
	--link cortex-elasticsearch \
	--link cortex-apollo \
	$(IMAGE):$(VERSION)

shell:
	docker run -it \
	--name $(IMAGE) \
	--link cortex-cassandra \
	--link cortex-elasticsearch \
	$(IMAGE):$(VERSION) \
	/bin/bash

clean:
	docker rm -f $(IMAGE)

update:
	docker pull $(FROM_IMAGE)
	$(MAKE) build
