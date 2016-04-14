.PHONY: build run

IMAGE=cortex-apollo
VERSION=1.0.1
FROM_IMAGE=$(shell grep FROM Dockerfile | cut -d " " -f 2)

build:
	docker build -t $(IMAGE) .
	docker tag $(IMAGE):latest $(IMAGE):$(VERSION)

run:
	docker run -d \
	-p 61613:61613 \
	-p 61623:61623 \
	--name cortex-apollo \
	$(IMAGE):$(VERSION)

clean:
	docker rm -f cortex-apollo || \
	docker rm -f apollo

update:
	docker pull $(FROM_IMAGE)
	$(MAKE) build
