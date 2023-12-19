#!/bin/sh

go run $( ls -1 cmd/vaults/*.go | grep -v _test.go) $@