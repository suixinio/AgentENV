ROOT := $(abspath $(dir $(lastword $(MAKEFILE_LIST)))/..)
GO   ?= go

.PHONY: build test fmt fmt-check vet

build:
	cd $(ROOT) && $(GO) build -o bin/scheduler ./scheduler/cmd

# Tests the same package set `vet` checks: a target that stops at ./scheduler/...
# leaves everything under shared/ untested by the only command CI runs.
# 🔴 -count=1, always. Go caches a package's test result and reports it as a
# plain `ok … (cached)`, which is indistinguishable from a run — so a suite
# whose whole point is proving that PostgreSQL and Redis were reachable this
# time can report success without opening a connection. The environment those
# tests read is not part of the cache key either, so `make test` with the DSN
# set replays the result of the run that skipped them.
test:
	cd $(ROOT) && $(GO) test -count=1 ./scheduler/... ./shared/... ./api/...

fmt:
	cd $(ROOT) && gofmt -w $$(find scheduler shared api -name '*.go' -type f)

fmt-check:
	@out=$$(cd $(ROOT) && gofmt -l $$(find scheduler shared api -name '*.go' -type f)); \
	 test -z "$$out" || (printf 'Files need formatting:\n%s\n' "$$out"; exit 1)

vet:
	cd $(ROOT) && $(GO) vet ./scheduler/... ./shared/... ./api/...
