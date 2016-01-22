.PHONY: build run

IMAGE=cortex-maven
FROM_IMAGE=$(shell grep FROM Dockerfile | cut -d " " -f 2)
VERSION=3.3.9

build:
	docker build -t $(IMAGE) .
	docker tag -f $(IMAGE):latest $(IMAGE):$(VERSION)

run:
	docker run -it \
	--name cortex-maven \
	$(IMAGE):$(VERSION) \
	/bin/bash

update:
	docker pull $(FROM_IMAGE)
	$(MAKE) build
