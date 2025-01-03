clean:
	go clean -testcache

build:
	go build -v ./...

test:
	go test -tags test -v ./...

lint:
	golangci-lint run

all: clean build lint test