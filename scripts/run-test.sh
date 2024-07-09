#!/bin/bash
set -e -x
go test $(go list ./... | grep -v client) -race -v -covermode=atomic -coverprofile=coverage.out -p 1
