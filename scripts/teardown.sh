#!/usr/bin/env bash

cd scripts/redis && docker-compose -p lmstfy-test down -v --remove-orphans && cd ../..
