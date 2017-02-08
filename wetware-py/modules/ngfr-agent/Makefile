.PHONY: build run

IMAGE=wetware-ngfr-agent
VERSION=1.0.0
FROM_IMAGE=$(shell grep FROM Dockerfile | cut -d " " -f 2)
CONTAINER=reasoner

build:
	docker build -t $(IMAGE) .
	docker tag $(IMAGE):latest $(IMAGE):$(VERSION)

run:
	docker run -d \
	--link cortex-apollo \
	--name $(CONTAINER) \
	$(IMAGE):$(VERSION)

shell:
	docker run -it \
	--link cortex-apollo \
	--name $(CONTAINER) \
	$(IMAGE):$(VERSION) \
	/bin/bash

clean:
	docker rm -f $(CONTAINER)

update:
	$(MAKE) build
