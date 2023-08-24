#!/bin/sh

if [ -z "${PORT}" ]; then
    echo "PORT isn't set"
    exit 1
fi

PORT=${PORT} go run examples/provider/server.go > log