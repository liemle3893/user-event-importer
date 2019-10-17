.PHONY: build test

test:
	go test github.com/liemle3893/user-event-importer/...

build:
	go build github.com/liemle3893/user-event-importer/...