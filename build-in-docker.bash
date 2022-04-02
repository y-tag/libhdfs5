#!/bin/bash

set -euxo pipefail

IMAGE_NAME=libhdfs5-build

cd $(dirname $0)

docker build -t $IMAGE_NAME .

docker run -it --rm \
    -u $(id -u):$(id -g) \
    -e GO111MODULE=on \
    -e GOCACHE=/tmp/go-cache \
    -e GOMODCACHE=/tmp/gomod-cache \
    -e GOLANGCI_LINT_CACHE=/tmp/golangci-lint-cache \
    -v $(pwd):/libhdfs5 \
    -w /libhdfs5 \
    $IMAGE_NAME \
	bash -c "make clean && make"

docker rmi $IMAGE_NAME
