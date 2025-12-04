# Makefile
.PHONY: all build test clean

all: build

build:
	go mod tidy
	go build

test:
	go test -v -count=1 ./...
