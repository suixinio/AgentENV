PROTOC    ?= protoc
GO        ?= go
GOBIN_DIR := $(shell $(GO) env GOPATH)/bin
PROTO_SRC := api/proto/scheduler.proto api/proto/apiproxy/apiproxy.proto

.PHONY: services proto tidy build test test-with-redis report-skipped-suites fmt fmt-check vet run-gateway

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
# Without `redis-server` on PATH, the routing-reader tests call t.Skip, and
# `go test` in its default mode prints neither the skip nor a count — the
# package line is the same `ok  agentenv/services/…` it prints when they ran.
# So the run above can report a clean pass over a suite that opened no
# connection, and the only way to tell is to know which variable was set.
# This says so, out loud, after the output somebody is actually looking at.
#
# It does not fail: running the module's tests without redis-server is a
# supported workflow and this target is the one that supports it.
# `test-with-redis` provides it, so nothing is printed there.
#
# 🔴 No PostgreSQL-gated suite is listed here, and none is coming back by
# accident. `scheduler/internal/registry` and `scheduler/internal/catalog` —
# the only packages in this module that ever touched a real database — were
# deleted with `services/scheduler` (阶段四's Go scheduler decommission);
# nothing left under `services/` imports database/sql, pgx or lib/pq. If a
# future package in this module needs PostgreSQL again, add its skip-detection
# here rather than assuming this list is exhaustive.
report-skipped-suites:
	@skipped=''; \
	 if [ -z "$$REDIS_SERVER_BIN" ] && ! command -v redis-server >/dev/null 2>&1; then \
	   skipped="$$skipped  - no redis-server on PATH and REDIS_SERVER_BIN unset: the routing-reader suite\n    skipped — the only thing in this module that reads the real Redis routing\n    projection every HA deployment's data plane routes on.\n"; \
	 fi; \
	 if [ -n "$$skipped" ]; then \
	   printf '\n\033[1;33mNOT EVERYTHING ABOVE RAN.\033[0m Reported as passes, but skipped:\n%b' "$$skipped"; \
	   printf 'Run \033[1mmake -C services test-with-redis\033[0m for the coverage CI has.\n\n'; \
	 fi

# 🔴 This target was called `test-with-postgres` and started no PostgreSQL.
#
# It once did: it ran a throwaway container and set SCHEDULER_REGISTRY_TEST_DSN
# / SCHEDULER_REGISTRY_TEST_REQUIRED for `scheduler/internal/registry`'s and
# `scheduler/internal/catalog`'s suites, the only packages in this module that
# ever opened a database connection. Both were deleted with `services/scheduler`
# (阶段四's Go scheduler decommission), and the recipe below is everything that
# was left: two Redis variables and a `command -v redis-server`. The name
# outlived the database by a whole decommission, and README/CLAUDE.md went on
# telling people it "starts a throwaway PostgreSQL in Docker" — which meant an
# operator debugging a red run had a container to go looking for that this
# target has never started. Renamed rather than re-documented, and with no
# compatibility alias: `make -C services test-with-postgres` now fails with
# "No rule to make target", which is the loud version of finding out.
#
# What it does, and the only thing it ever still did: turn a missing
# `redis-server` into a failure instead of a green skip, for the routing-reader
# suite `make test` alone would silently skip — exactly the job
# `report-skipped-suites` above only warns about.
#
# 🔴 The -count=1 that makes this target mean anything lives in the two
# sub-Makefiles, so it covers `make test` in CI as well. Without it a second run
# answers `ok … (cached)` for every package — the environment below is not part
# of the cache key, so the result being replayed can be the one from the run
# that skipped everything.
#
# 🔴 Both variables, not only the binary. The check below proves redis-server
# was on PATH when make ran; AENV_REDIS_TEST_REQUIRED turns a skip inside the
# suite into a failure. That is what covers the routing projection every HA
# deployment's data plane reads — and a key-format change made on the Rust
# side (`src/binding_store/record.rs`, the only writer of those keys) and
# forgotten here passes every other test in this module, on any machine.
#
# 🔴 AENV_REDIS_TEST_REQUIRED is deliberately the same name the Rust side's
# harness reads (`make test-with-redis` at the repository root, `.github/
# workflows/ci.yml`). One variable covers both halves of a contract that only
# exists because two languages read the same Redis keys; two names for it would
# let a run set one and silently skip the other half.
test-with-redis:
	@command -v redis-server >/dev/null 2>&1 || { \
		echo "redis-server not found: install it, or the routing-reader tests silently skip"; \
		exit 1; }
	AENV_REDIS_TEST_REQUIRED=1 \
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
