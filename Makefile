# Makefile
.PHONY: all build test clean lint tidy install-tools

STATICCHECK_VERSION := v0.6.1
GOLANGCI_LINT_VERSION := v2.0.2

all: build

build: tidy
	go build

test:
	go test -v ./...

lint:
	staticcheck ./...
	golangci-lint run --timeout=10m

tidy:
	go mod tidy

install-tools:
	go install honnef.co/go/tools/cmd/staticcheck@$(STATICCHECK_VERSION)
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

clean:
	rm -f iceberg-terraform
