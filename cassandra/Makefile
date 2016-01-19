.PHONY: build run

IMAGE=cortex-cassandra
VERSION=1.0.0
FROM_IMAGE=$(shell grep FROM Dockerfile | cut -d " " -f 2)

build:
	docker build -t $(IMAGE) .
	docker tag -f $(IMAGE):latest $(IMAGE):$(VERSION)

run:
	docker run -d \
	--name cs1 \
	$(IMAGE):$(VERSION)

update:
	docker pull $(FROM_IMAGE)
	$(MAKE) build
