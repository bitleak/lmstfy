PROGRAM=lmstfy-server

PKG_FILES=`go list ./... | sed -e 's=github.com/bitleak/lmstfy/=./='`

CCCOLOR="\033[37;1m"
MAKECOLOR="\033[32;1m"
ENDCOLOR="\033[0m"

all: $(PROGRAM)

.PHONY: all

$(PROGRAM):
	@bash build.sh
	@echo ""
	@printf $(MAKECOLOR)"Hint: It's a good idea to run 'make test' ;)"$(ENDCOLOR)
	@echo ""

test:
	- cd scripts/spanner && docker-compose up --force-recreate -d && cd ../..
	@sh scripts/run-test.sh
	- cd scripts/spanner && docker-compose down && cd ../..

lint:
	@rm -rf lint.log
	@printf $(CCCOLOR)"Checking format...\n"$(ENDCOLOR)
	@go list ./... | sed -e 's=github.com/bitleak/lmstfy/=./=' | xargs -n 1 gofmt -d -s 2>&1 | tee -a lint.log
	@[ ! -s lint.log ]
	@printf $(CCCOLOR)"Checking vet...\n"$(ENDCOLOR)
	@go list ./... | sed -e 's=github.com/bitleak/lmstfy/=./=' | xargs -n 1 go vet
