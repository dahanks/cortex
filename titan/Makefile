.PHONY: build run

IMAGE=cortex-titan
VERSION=1.0.0
FROM_IMAGE=$(shell grep FROM Dockerfile | cut -d " " -f 2)

build:
	docker build -t $(IMAGE) .
	docker tag -f $(IMAGE):latest $(IMAGE):$(VERSION)

run:
	docker run -it \
	--name titan \
	$(IMAGE):$(VERSION) \
	/bin/bash

update:
	docker pull $(FROM_IMAGE)
	$(MAKE) build
