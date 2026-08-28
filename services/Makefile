PROTOC    ?= protoc
GO        ?= go
GOBIN_DIR := $(shell $(GO) env GOPATH)/bin
PROTO_SRC := api/proto/scheduler.proto api/proto/apiproxy/apiproxy.proto

.PHONY: services proto tidy build test test-with-postgres report-skipped-suites fmt fmt-check vet run-gateway

proto:
	PATH="$(GOBIN_DIR):$$PATH" $(PROTOC) \
		--go_out=paths=source_relative:. \
		--go-grpc_out=paths=source_relative:. \
		$(PROTO_SRC)

tidy:
	$(GO) mod tidy

build:
	$(MAKE) -C gateway build

test:
	$(MAKE) -C gateway test
	@$(MAKE) --no-print-directory report-skipped-suites

# 🔴 What a skipped suite looks like is `ok`.
#
# Without `redis-server` on PATH, the RedisBindingStore/routing-reader tests
# call t.Skip, and `go test` in its default mode prints neither the skip nor a
# count — the package line is the same `ok  agentenv/services/…` it prints when
# they ran. So the run above can report a clean pass over a suite that opened
# no connection, and the only way to tell is to know which variable was set.
# This says so, out loud, after the output somebody is actually looking at.
#
# It does not fail: running the module's tests without redis-server is a
# supported workflow and this target is the one that supports it.
# `test-with-postgres` provides it, so nothing is printed there.
#
# 🔴 No PostgreSQL-gated suite is listed here any more. `scheduler/internal/registry`
# and `scheduler/internal/catalog` — the only packages in this module that ever
# touched a real database — were deleted with `services/scheduler` (阶段四's Go
# scheduler decommission); `services/gateway` and `services/shared` open no SQL
# connection. If a future package in this module needs PostgreSQL again, add its
# skip-detection here rather than assuming this list is exhaustive.
report-skipped-suites:
	@skipped=''; \
	 if [ -z "$$REDIS_SERVER_BIN" ] && ! command -v redis-server >/dev/null 2>&1; then \
	   skipped="$$skipped  - no redis-server on PATH and REDIS_SERVER_BIN unset: the RedisBindingStore and\n    routing-reader suites skipped — the only coverage the HA deployment's binding\n    store has.\n"; \
	 fi; \
	 if [ -n "$$skipped" ]; then \
	   printf '\n\033[1;33mNOT EVERYTHING ABOVE RAN.\033[0m Reported as passes, but skipped:\n%b' "$$skipped"; \
	   printf 'Run \033[1mmake -C services test-with-postgres\033[0m for the coverage CI has.\n\n'; \
	 fi

# 🔴 Despite the name, this no longer touches PostgreSQL. It used to start a
# throwaway PostgreSQL container and set SCHEDULER_REGISTRY_TEST_DSN /
# SCHEDULER_REGISTRY_TEST_REQUIRED for `scheduler/internal/registry`'s and
# `scheduler/internal/catalog`'s suites — the only packages in this module that
# ever opened a real database connection. Both were deleted with
# `services/scheduler` (阶段四's Go scheduler decommission; confirmed via a
# repository-wide grep that nothing in this module still reads either
# variable), so there is nothing left in this module for a database to gate.
# The name stays because it is what CLAUDE.md, services/README.md and this
# module's docs already call it, and this target's remaining, real job —
# turning a missing `redis-server` into a failure instead of a green skip, for
# the RedisBindingStore/routing-reader suites `make test` alone would silently
# skip — is exactly the same job `report-skipped-suites` above warns about.
#
# 🔴 The -count=1 that makes this target mean anything lives in the two
# sub-Makefiles, so it covers `make test` in CI as well. Without it a second run
# answers `ok … (cached)` for every package — the environment below is not part
# of the cache key, so the result being replayed can be the one from the run
# that skipped everything.
#
# 🔴 Both redis variables, not only the binary. The check below proves
# redis-server was on PATH when make ran; SCHEDULER_REDIS_TEST_REQUIRED turns a
# skip inside the suite into a failure. That is what covers the HA binding
# tests, and they are the only ones that catch an incarnation implemented for
# the in-memory store and forgotten for Redis — a defect that never shows up as
# a red test locally, only as fencing that silently does nothing in the one
# deployment that runs Redis.
test-with-postgres:
	@command -v redis-server >/dev/null 2>&1 || { \
		echo "redis-server not found: install it, or the RedisBindingStore tests silently skip"; \
		exit 1; }
	SCHEDULER_REDIS_TEST_REQUIRED=1 \
	REDIS_SERVER_BIN="$$(command -v redis-server)" \
	$(MAKE) test

fmt:
	$(MAKE) -C gateway fmt

fmt-check:
	$(MAKE) -C gateway fmt-check

vet:
	$(MAKE) -C gateway vet

run-gateway:
	$(GO) run ./gateway/cmd -config ./config/local.json
