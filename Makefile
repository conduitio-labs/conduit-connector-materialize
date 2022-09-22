.PHONY: build test lint

VERSION=$(shell git describe --tags --dirty --always)

build:
	go build -ldflags "-X 'github.com/conduitio-labs/conduit-connector-materialize.version=${VERSION}'" -o conduit-connector-materialize cmd/connector/main.go

test:
	docker compose -f test/docker-compose.yml up --quiet-pull -d --wait
	go test $(GOTEST_FLAGS) ./...; ret=$$?; \
		docker-compose -f test/docker-compose.yml down; \
		exit $$ret

lint:
	golangci-lint run
