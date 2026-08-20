ROOT := $(abspath $(dir $(lastword $(MAKEFILE_LIST)))/..)
GO   ?= go

.PHONY: build test fmt fmt-check vet

build:
	cd $(ROOT) && $(GO) build -o bin/scheduler ./scheduler/cmd

# Tests the same package set `vet` checks: a target that stops at ./scheduler/...
# leaves everything under shared/ untested by the only command CI runs.
test:
	cd $(ROOT) && $(GO) test ./scheduler/... ./shared/... ./api/...

fmt:
	cd $(ROOT) && gofmt -w $$(find scheduler shared api -name '*.go' -type f)

fmt-check:
	@out=$$(cd $(ROOT) && gofmt -l $$(find scheduler shared api -name '*.go' -type f)); \
	 test -z "$$out" || (printf 'Files need formatting:\n%s\n' "$$out"; exit 1)

vet:
	cd $(ROOT) && $(GO) vet ./scheduler/... ./shared/... ./api/...
