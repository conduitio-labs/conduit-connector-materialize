.PHONY: build test lint

build:
	go build -o conduit-connector-materialize cmd/materialize/main.go

test:
	go test $(GOTEST_FLAGS) -race ./...

lint:
	golangci-lint run