.PHONY: build run

IMAGE=audrey-chat
VERSION=1.0.0
FROM_IMAGE=$(shell grep FROM Dockerfile | cut -d " " -f 2)
CONTAINER=$(IMAGE)
CWD=$(shell pwd)

build:
	docker build -t $(IMAGE) .
	docker tag $(IMAGE):latest $(IMAGE):$(VERSION)

run:
	docker run -d \
	-p 80:80 \
	--name $(CONTAINER) \
	$(IMAGE):$(VERSION)

dev:
	docker run -d \
	-p 80:80 \
	--name $(CONTAINER) \
	-v $(CWD):/var/www/html/audrey-chat \
	$(IMAGE):$(VERSION)

clean:
	docker rm -f $(CONTAINER)

update:
	docker pull $(FROM_IMAGE)
	$(MAKE) build
