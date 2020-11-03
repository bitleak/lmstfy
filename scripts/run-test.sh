#!/bin/bash
set -e -x

#go list ./... | grep -v client | xargs -n 1 go test -v

go list ./...|grep -v client|awk '{printf $1" "}'|xargs go test -v -covermode=count -coverprofile=coverage.out 

