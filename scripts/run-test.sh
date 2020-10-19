#!/bin/bash
set -e -x

go list ./... | grep -v client | xargs -n 1 go test -v -count=1
