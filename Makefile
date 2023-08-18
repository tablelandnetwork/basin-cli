# Lint
lint:
	go run github.com/golangci/golangci-lint/cmd/golangci-lint@v1.51.0 run
.PHONY: lint

# Build
build:
	go build -o basin cmd/basin/*
.PHONY: build

# Test
test: 
	go test ./... -short -race
.PHONY: test

generate:
	capnp compile -I ../go-capnp/std -ogo pkg/capnp/tx.capnp
.PHONY: generate

# Helpers for running
replicate:
	go run $(shell ls -1 cmd/basin/*.go | grep -v _test.go) replicate 
.PHONY: replicate

setup:
	go run $(shell ls -1 cmd/basin/*.go | grep -v _test.go) setup 
.PHONY: setup 