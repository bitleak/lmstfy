PROGRAM=lmstfy-server

PKG_FILES=`go list ./... | sed -e 's=github.com/bitleak/lmstfy/=./='`

CCCOLOR="\033[37;1m"
MAKECOLOR="\033[32;1m"
ENDCOLOR="\033[0m"

all: $(PROGRAM)

.PHONY: all

$(PROGRAM):
	@sh build.sh
	@echo ""
	@printf $(MAKECOLOR)"Hint: It's a good idea to run 'make test' ;)"$(ENDCOLOR)
	@echo ""

test:
	- cd scripts/redis && docker-compose up -d && cd ../..
	@LMSTFY_TEST_CONFIG=`pwd`/scripts/test-conf.toml sh scripts/run-test.sh 
	@cp coverage.out build/
	- cd scripts/redis && docker-compose down && cd ../..

coverage:
	- $(HOME)/gopath/bin/goveralls -coverprofile=build/coverage.out -service=travis-ci -repotoken $(COVERAGE_TOKEN)
	@rm -rf build/coverage.out

lint:
	@rm -rf lint.log
	@printf $(CCCOLOR)"Checking format...\n"$(ENDCOLOR)
	@go list ./... | sed -e 's=github.com/bitleak/lmstfy/=./=' | xargs -n 1 gofmt -d -s 2>&1 | tee lint.log
	@printf $(CCCOLOR)"Checking vet...\n"$(ENDCOLOR)
	@go list ./... | sed -e 's=github.com/bitleak/lmstfy/=./=' | xargs -n 1 go vet 2>&1 | tee lint.log
	@[ ! -s lint.log ]
