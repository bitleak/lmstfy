#!/bin/bash
set -e -x

go test $(go list ./... | grep -v client) -v -covermode=count -coverprofile=coverage.out -p 1
