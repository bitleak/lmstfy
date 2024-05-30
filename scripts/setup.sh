#!/usr/bin/env bash

if [[ -n $(docker ps -q -f "name=lmstfy-test") ]];then
	cd scripts/redis && docker-compose -p lmstfy-test down -v --remove-orphans && cd ../..
fi

cd scripts/redis && docker-compose -p lmstfy-test up -d  --remove-orphans && cd ../..