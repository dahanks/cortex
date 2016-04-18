.PHONY: build run

IMAGE=wetware-nlp
VERSION=1.1.0
FROM_IMAGE=$(shell grep FROM Dockerfile | cut -d " " -f 2)
CONTAINER=nlp

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
