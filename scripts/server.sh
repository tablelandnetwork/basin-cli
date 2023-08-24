#!/bin/sh

if [ -z "${PORT}" ]; then
    echo "PORT isn't set"
    exit 1
fi

if [ -z "${ETH_ADDRESS}" ]; then
    echo "ETH_ADDRESS isn't set"
    exit 1
fi

PORT=${PORT} ETH_ADDRESS=${ETH_ADDRESS} go run examples/provider/server.go > log