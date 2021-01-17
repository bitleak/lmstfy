#!/bin/bash
set -e

docker build -t bitleak/lmstfy:$RELEASE_VERSION .
docker tag bitleak/lmstfy:$RELEASE_VERSION bitleak/lmstfy:latest

docker push bitleak/lmstfy:latest
docker push bitleak/lmstfy:$RELEASE_VERSION