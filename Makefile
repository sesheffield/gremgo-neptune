SHELL=bash
.DEFAULT_GOAL:= all
.PHONY: all
all: vet test

.PHONY: test
test:
	go test -v -race -cover ./...

.PHONY: audit
audit:
	go list -json -m all | nancy sleuth

.PHONY: build
build:
	go build ./...

.PHONY: test-bench
test-bench:
	@go test -v -bench=. -race

.PHONY: gremlin
gremlin:
	@docker build -t gremgo-neptune/gremlin-server -f ./Dockerfile.gremlin .
	@docker run -p 8182:8182 -t gremgo-neptune/gremlin-server
