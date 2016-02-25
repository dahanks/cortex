.PHONY: build run

IMAGE=wetware-py
FROM_IMAGE=$(shell grep FROM Dockerfile | cut -d " " -f 2)
VERSION=$(shell grep version setup.py | cut -d "'" -f 2)
CWD=$(shell pwd)

build:
	docker build -t $(IMAGE) .
	docker tag -f $(IMAGE):latest $(IMAGE):$(VERSION)

run:
	docker run -it \
	--name $(IMAGE) \
	$(IMAGE):$(VERSION)

update:
	docker pull $(FROM_IMAGE)
	$(MAKE) build
