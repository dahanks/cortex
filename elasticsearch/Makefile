.PHONY: build run

IMAGE=cortex-elasticsearch
VERSION=1.0.0
FROM_IMAGE=$(shell grep FROM Dockerfile | cut -d " " -f 2)

build:
	docker build -t $(IMAGE) .
	docker tag -f $(IMAGE):latest $(IMAGE):$(VERSION)

run:
	docker run -d \
	--name cortex-elasticsearch \
	$(IMAGE):$(VERSION)

local:
	docker run -d \
	-p 9200:9200 \
	-p 9300:9300 \
	--name es1 \
	$(IMAGE):$(VERSION)

clean:
	docker rm -f cortex-elasticsearch || \
	docker rm -f es1

update:
	docker pull $(FROM_IMAGE)
	$(MAKE) build
