.PHONY: build run

IMAGE=apollo-registrar
VERSION=1.0.0
FROM_IMAGE=$(shell grep FROM Dockerfile | cut -d " " -f 2)
CONTAINER=registrar

build:
	docker build -t $(IMAGE) .
	docker tag $(IMAGE):latest $(IMAGE):$(VERSION)

run:
	docker run -d \
	--link cortex-apollo \
	--name $(CONTAINER) \
	-v /var/run/docker.sock:/var/run/docker.sock \
	$(IMAGE):$(VERSION)

shell:
	docker run -it \
	--link cortex-apollo \
	--name $(CONTAINER) \
	-v /var/run/docker.sock:/var/run/docker.sock \
	$(IMAGE):$(VERSION) \
	/bin/bash

clean:
	docker rm -f $(CONTAINER)

update:
	$(MAKE) build
