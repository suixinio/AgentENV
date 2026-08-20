PROTOC    ?= protoc
GO        ?= go
GOBIN_DIR := $(shell $(GO) env GOPATH)/bin
PROTO_SRC := api/proto/scheduler.proto

# Throwaway PostgreSQL for `test-with-postgres`. The port matches the one the
# registry suite's own documentation uses, and is overridable so the target can
# run alongside a database somebody is already holding that port with.
DOCKER              ?= docker
REGISTRY_TEST_PG    ?= agentenv-registry-test-pg
REGISTRY_TEST_PORT  ?= 15499
REGISTRY_TEST_DSN   ?= postgres://postgres:verify@127.0.0.1:$(REGISTRY_TEST_PORT)/aenv_registry

.PHONY: services proto tidy build test test-with-postgres fmt fmt-check vet run-scheduler run-gateway

proto:
	PATH="$(GOBIN_DIR):$$PATH" $(PROTOC) \
		--go_out=paths=source_relative:. \
		--go-grpc_out=paths=source_relative:. \
		$(PROTO_SRC)

tidy:
	$(GO) mod tidy

build:
	$(MAKE) -C gateway build
	$(MAKE) -C scheduler build

test:
	$(MAKE) -C gateway test
	$(MAKE) -C scheduler test

# `make test` on its own runs the registry suite with no database, and every
# test in it that touches SQL then skips — which reports as a pass. This target
# is the coverage CI has: a throwaway PostgreSQL, plus the two variables that
# turn a missing dependency into a failure instead of a file of green skips.
#
# SCHEDULER_REGISTRY_TEST_REQUIRED does that for the DSN. REDIS_SERVER_BIN and
# SCHEDULER_REDIS_TEST_REQUIRED do it for the binding-store tests, which
# otherwise skip when `redis-server` is not on PATH.
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
	$(DOCKER) run -d --rm --name $(REGISTRY_TEST_PG) \
		-e POSTGRES_PASSWORD=verify -e POSTGRES_DB=aenv_registry \
		-p $(REGISTRY_TEST_PORT):5432 postgres:16-alpine >/dev/null
	@for i in $$(seq 1 60); do \
		$(DOCKER) exec $(REGISTRY_TEST_PG) pg_isready -U postgres >/dev/null 2>&1 && break; \
		sleep 1; \
	done; \
	SCHEDULER_REGISTRY_TEST_DSN="$(REGISTRY_TEST_DSN)" \
	SCHEDULER_REGISTRY_TEST_REQUIRED=1 \
	SCHEDULER_REDIS_TEST_REQUIRED=1 \
	REDIS_SERVER_BIN="$$(command -v redis-server)" \
	$(MAKE) test; \
	status=$$?; \
	$(DOCKER) rm -f $(REGISTRY_TEST_PG) >/dev/null; \
	exit $$status

fmt:
	$(MAKE) -C gateway fmt
	$(MAKE) -C scheduler fmt

fmt-check:
	$(MAKE) -C gateway fmt-check
	$(MAKE) -C scheduler fmt-check

vet:
	$(MAKE) -C gateway vet
	$(MAKE) -C scheduler vet

run-scheduler:
	$(GO) run ./scheduler/cmd -config ./config/local.json

run-gateway:
	$(GO) run ./gateway/cmd -config ./config/local.json
