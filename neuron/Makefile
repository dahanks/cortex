.PHONY: build run

IMAGE=cortex-neuron
VERSION=1.0.0
FROM_IMAGE=$(shell grep FROM Dockerfile | cut -d " " -f 2)

build:
	docker build -t $(IMAGE) .
	docker tag -f $(IMAGE):latest $(IMAGE):$(VERSION)

run:
	docker run -d \
	--name neuron \
	--link cs1 \
	--link es1 \
	--link ccd-apollo \
	$(IMAGE):$(VERSION)

update:
	docker pull $(FROM_IMAGE)
	$(MAKE) build
