#!/bin/bash
set -e -x
export SPANNER_EMULATOR_HOST=localhost:9010
go test $(go list ./... | grep -v client) -race -v -covermode=atomic -coverprofile=coverage.out -p 1
