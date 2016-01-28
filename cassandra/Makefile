.PHONY: build run

IMAGE=cortex-cassandra
VERSION=1.0.0
FROM_IMAGE=$(shell grep FROM Dockerfile | cut -d " " -f 2)

build:
	docker build -t $(IMAGE) .
	docker tag -f $(IMAGE):latest $(IMAGE):$(VERSION)

run:
	docker run -d \
	--name cortex-cassandra \
	$(IMAGE):$(VERSION)

local:
	docker run -d \
	-p 7000:7000 \
	-p 7001:7001 \
	-p 7199:7199 \
	-p 8182:8182 \
	-p 8183:8183 \
	-p 8184:8184 \
	-p 9042:9042 \
	-p 9160:9160 \
	--name cs1 \
	$(IMAGE):$(VERSION)

clean:
	docker rm -f cortex-cassandra || \
	docker rm -f cs1

update:
	docker pull $(FROM_IMAGE)
	$(MAKE) build
