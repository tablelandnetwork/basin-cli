#!/bin/sh

go run $( ls -1 cmd/basin/*.go | grep -v _test.go) $@