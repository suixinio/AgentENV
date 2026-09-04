SHELL := /usr/bin/env bash
.SHELLFLAGS := -eu -o pipefail -c
.DELETE_ON_ERROR:

# Tools
CARGO ?= cargo
DOCKER ?= docker
DOCKER_COMPOSE ?= docker compose
DEPLOY_COMPOSE_FILE ?= deploy/docker-compose.yml
APT_MIRROR_BASE ?=
# Commit baked into aenv-node/aenv-api at build time (see build.rs and
# deploy/docker/Dockerfile.aenv-{node,api}); `.git` is excluded from the
# Docker build context, so this has to be passed in as a build-arg rather
# than resolved inside the container. Falls back to "unknown" rather than
# failing the build when git resolution isn't possible either.
AENV_GIT_COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
KUBECTL ?= kubectl
K8S_NAMESPACE ?= agentenv-system
K8S_RUNTIME_IMAGE ?= agentenv-runtime:latest
K8S_API_IMAGE ?= agentenv-api:latest
K8S_GATEWAY_IMAGE ?= agentenv-gateway:latest
K8S_EGRESS_IMAGE ?= agentenv-egress:latest
K3S_CTR ?= sudo k3s ctr

# aenv home path.
AENV_HOME_PATH ?= /var/lib/aenv
export AENV_HOME_PATH

# aenv CLI install location. Override with AENV_INSTALL_PREFIX=~/.local for a
# user-local install; AENV_INSTALL_PREFIX=/usr/local requires sudo.
AENV_INSTALL_PREFIX ?= /usr/local
AENV_INSTALL_DIR := $(AENV_INSTALL_PREFIX)/bin
# Runner used to execute the `install` / `rm` commands that write to
# $(AENV_INSTALL_DIR). Defaults to sudo because the default prefix is
# /usr/local; override to empty (AENV_INSTALL_SUDO=) for a user-local prefix.
AENV_INSTALL_SUDO ?= sudo

# Script entrypoints
TEST_SCRIPTS_DIR := ./scripts/tests

# Runner for tests that require AENV's network and namespace capabilities.
CARGO_HOST_TARGET_ENV = $(shell $(CARGO) -vV | sed -n 's/^host: //p' | tr '[:lower:]-' '[:upper:]_')
CAPABILITY_RUNNER = CARGO_TARGET_$(CARGO_HOST_TARGET_ENV)_RUNNER="$(CURDIR)/scripts/run-with-capabilities.sh"
AENV_TEST_STATE_ID ?= $(if $(GITHUB_RUN_ID),$(GITHUB_RUN_ID)-$(GITHUB_RUN_ATTEMPT),local-$$(id -u))
AENV_TEST_STATE_DIR ?= /tmp/aenv-test-$(AENV_TEST_STATE_ID)
AENV_TEST_DEPS_PATH ?= $(if $(AENV_DEPS_PATH),$(AENV_DEPS_PATH),/var/lib/aenv/deps)
CAPABILITY_TEST_ENV := AENV_HOME_PATH="$(AENV_TEST_STATE_DIR)/home" AENV_RUNTIME_PATH="$(AENV_TEST_STATE_DIR)/run" AENV_DEPS_PATH="$(AENV_TEST_DEPS_PATH)"
UVM_UBLK_DAEMON_INSTALL_PATH := $(AENV_HOME_PATH)/ublk/uvm-ublk-daemon
DEBUG_PROFILE_DIR := $${CARGO_TARGET_DIR:-$$(pwd)/target}/debug

# Build profile: debug for dev/test targets, release for explicit release and
# benchmark targets.
PROFILE ?= debug
CARGO_PROFILE_FLAG = $(if $(filter release,$(PROFILE)),--release,)
TARGET_PROFILE_DIR = $${CARGO_TARGET_DIR:-$$(pwd)/target}/$(PROFILE)

.PHONY: all build release \
	build-server build-server-release \
	build-snapshot-image \
	build-aenv build-aenv-release install-aenv uninstall-aenv \
	build-ublk install-ublk \
	fmt clippy check-crate-boundaries \
	mutants coverage \
	test test-unit test-integration test-with-redis test-with-postgres prepare-agent-test-state test-agent test-agent-integration test-envd test-ublk \
	test-e2e-compose test-e2e-k8s test-e2e-compose-split test-e2e-k8s-split test-e2e-all \
	bench bench-snapshot bench-ublk bench-orchestrator-store bench-placement-shadow \
	ci-deps ci-deps-protoc \
	firecracker-client envd-http-client agentenv-server custom-extension-client start-server start-server-release \
	services gateway \
	deploy-build deploy-up deploy-up-no-build deploy-down deploy-logs deploy-ps \
	k8s-build k8s-redeploy k8s-load-dev k8s-refresh-dev \
	k8s-render k8s-apply k8s-delete \
	k8s-render-dev k8s-apply-dev k8s-delete-dev \
	docs docs-serve

all: build

build:
	$(CARGO) build

release:
	$(CARGO) build --release

build-server:
	$(CARGO) build -p aenv-node -p aenv-api --bin aenv-node --bin aenv-api

build-server-release:
	$(CARGO) build --release -p aenv-node -p aenv-api --bin aenv-node --bin aenv-api

build-snapshot-image:
	$(CARGO) build -p aenv-api --bin aenv-snapshot-image

build-aenv:
	$(CARGO) build -p aenv

build-aenv-release:
	$(CARGO) build --release -p aenv

install-aenv: build-aenv-release
	$(AENV_INSTALL_SUDO) install -d "$(AENV_INSTALL_DIR)"
	$(AENV_INSTALL_SUDO) install -m 0755 "$${CARGO_TARGET_DIR:-$$(pwd)/target}/release/aenv" "$(AENV_INSTALL_DIR)/aenv"
	@echo "Installed aenv to $(AENV_INSTALL_DIR)/aenv"

uninstall-aenv:
	$(AENV_INSTALL_SUDO) rm -f "$(AENV_INSTALL_DIR)/aenv"
	@echo "Removed $(AENV_INSTALL_DIR)/aenv"

fmt:
	$(CARGO) fmt --all -- --check

clippy:
	$(CARGO) clippy --workspace --all-targets --all-features -- -D warnings

# The crate split, as a fact about the dependency graph rather than a comment.
#
# 🔴 This is what replaced `only_a_role_that_runs_sandboxes_builds_the_byte_half`,
# a source scan that asserted the byte half had exactly one call site inside a
# runtime role gate. The gate is gone: the byte half (overlaybd, ublk) lives in
# `aenv-node` and the deciding half (`sqlx`) in `aenv-api`, and neither crate
# depends on the other. A regression is now a `Cargo.toml` edit, and this is
# what fails on it.
#
# `-e normal` on purpose: `aenv-core` dev-depends on nothing, but `aenv-node`
# and `aenv-api` both dev-depend on `aenv-core`'s `test-support` feature, and a
# dev edge is not something a shipped binary links.
check-crate-boundaries:
	@fail=0; \
	api_tree=$$($(CARGO) tree -p aenv-api -e normal) || { echo "cargo tree -p aenv-api failed"; exit 1; }; \
	node_tree=$$($(CARGO) tree -p aenv-node -e normal) || { echo "cargo tree -p aenv-node failed"; exit 1; }; \
	if printf '%s\n' "$$api_tree" | grep -E 'overlaybd|uvm-ublk|uvm-ublk-daemon|storage-util'; then \
	  echo "aenv-api links the byte half; the api half runs no sandboxes and must not."; \
	  fail=1; \
	fi; \
	if printf '%s\n' "$$node_tree" | grep -E 'sqlx|deadpool-postgres'; then \
	  echo "aenv-node links a PostgreSQL driver; a node must never hold database credentials."; \
	  fail=1; \
	fi; \
	egress_tree=$$($(CARGO) tree -p aenv-egress -e normal --all-features --prefix none) || { echo "cargo tree -p aenv-egress failed"; exit 1; }; \
	if printf '%s\n' "$$egress_tree" | sort -u | grep -E '^(sqlx|deadpool-postgres|overlaybd|uvm-ublk|uvm-ublk-daemon|storage-util|aenv-core|rustls|tokio-rustls|hyper-rustls|rcgen) v[0-9]'; then \
	  echo "aenv-egress links a database, the byte half, aenv-core or a second TLS stack; the broker contract is a leaf on openssl only."; \
	  fail=1; \
	fi; \
	for feature in core resolver remote tls; do \
	  $(CARGO) check -q -p aenv-egress --no-default-features --features $$feature >/dev/null 2>&1 || { \
	    echo "aenv-egress does not build with only its \"$$feature\" feature. Every build in the tree turns on either one feature or all of them, so a reference from one optional module into another compiles everywhere and fails for whoever enables just the one."; \
	    fail=1; }; \
	done; \
	node_features=$$($(CARGO) tree -p aenv-node -e features --prefix none) || { echo "cargo tree -p aenv-node -e features failed"; exit 1; }; \
	if printf '%s\n' "$$node_features" | grep -E '^aenv-egress feature "tls"'; then \
	  echo "aenv-node enables aenv-egress's tls feature; the node relays bytes and terminates no TLS."; \
	  fail=1; \
	fi; \
	workspace_tree=$$($(CARGO) tree --workspace -e normal,build,dev --prefix none) || { echo "cargo tree --workspace failed"; exit 1; }; \
	if printf '%s\n' "$$workspace_tree" | sort -u | grep -E '^(rocksdb|librocksdb-sys) v[0-9]'; then \
	  echo "the workspace links an embedded database; node-local metadata is derived from the"; \
	  echo "cache layout or written as JSON records, and nothing here needs one."; \
	  fail=1; \
	fi; \
	if [ $$fail -eq 0 ]; then echo "crate boundaries hold: aenv-api has no byte half, aenv-node has no database and no broker TLS, aenv-egress is a leaf whose features each stand alone, the workspace has no embedded database"; fi; \
	exit $$fail

mutants:
	$(CARGO) adev mutants

coverage:
	$(MAKE) install-ublk PROFILE=debug
	PATH="$${CARGO_TARGET_DIR:-$$(pwd)/target}/debug:$$PATH" \
	$(CARGO) adev coverage

test: test-agent test-envd test-ublk

# 🔴 `--lib --bins`, not `--lib`. A test target that names `--lib` only is
# naming the crate's library and nothing else, and the difference was not
# theoretical: the two server binaries hold tests that read their own source
# text, and this target filtered every one of them out. The run printed
# `running 0 tests ... 1487 passed; 1491 filtered out` and reported ok.
#
# What that hid, then and now: `the_shutdown_bounds_are_still_wired` and
# `async_main_actually_refuses_a_configured_pg_dsn`
# (`crates/aenv-node/src/bin/aenv-node.rs`) scan that file's own source for
# calls nothing else in the suite exercises — blocking work still outstanding
# at exit hanging the process past `terminationGracePeriodSeconds`, and a `[pg].dsn`
# reaching a machine that runs user code. Both live in a `src/bin/` file
# because what they read is that file; moving them into a library would only
# make them read a sibling. The target selection was the thing that was wrong,
# not their address.
#
# 🔴 `-p aenv -p adev` for the same reason, one step further along: both are
# bin-only crates, so `--lib` could not have reached them however the package
# list was written, and no other target in this file or in `.github/workflows/`
# names either one. 68 tests that ran nowhere — `make clippy` compiles them
# (`--workspace --all-targets`), so they were type-checked on every PR and
# executed on none. Among them: `aenv download` not overwriting an existing
# file without `--force`, and `aenv upload`'s directory walk refusing to
# follow symlinks out of the tree.
#
# The package list reaches `aenv-egress` with its default features (`core`),
# which is what `aenv-node` links and deliberately carries no TLS stack, no
# credential resolver and no `http` handler. The broker binary's own code — the
# leaf signer, the resolver client, the header injection — lives behind `bin`, so
# it needs the second invocation to be executed at all rather than only
# type-checked by `make clippy`'s `--all-features`.
test-unit:
	$(CARGO) test -p aenv-core -p aenv-api -p aenv-node -p aenv-egress -p envd -p linux-cap -p aenv -p adev --lib --bins
	$(CARGO) test -p aenv-egress --features bin --lib --bins
	$(CAPABILITY_TEST_ENV) $(CAPABILITY_RUNNER) $(CARGO) test -p aenv-core -p aenv-api -p aenv-node --lib --bins -- --ignored
	$(CAPABILITY_TEST_ENV) $(CAPABILITY_RUNNER) $(CARGO) test -p uvm-ublk -p uvm-ublk-daemon --lib --bins
	bash scripts/tests/verify-capability-runner.sh
	bash scripts/tests/verify-install-service.sh

# The metadata store's Redis suite, against a real redis-server.
#
# 🔴 Two disciplines borrowed from services/Makefile, both of them put there by
# a real incident:
#   * prove the dependency is present before running, so a missing one is a
#     failure rather than a green run over skipped tests;
#   * say out loud what the run skipped. A make target in this repository once
#     silently skipped 152 tests and reported ok, and two of them were the only
#     tests that would have caught a `KEEPTTL` being dropped.
#
# Covers every Redis-backed suite in this crate: orchestrator::store (api's
# own sandbox-metadata store), binding_store::redis (the routing/binding
# store, task's own "D3" -- see that module's own KEEPTTL fix, the same
# defect class this target exists to keep catching), and node_registry::redis
# (the shared-roster fix's cross-replica observed-state store). A filter
# naming only some of these would let a newer suite silently skip the same
# way the incident above did for the first one.
REDIS_TEST_LOG ?= target/redis-store-tests.log

test-with-redis:
	@command -v "$${REDIS_SERVER_BIN:-redis-server}" >/dev/null 2>&1 || { \
	  echo "redis-server not found: the metadata store tests would skip instead of running."; \
	  echo "Install redis-server, or point REDIS_SERVER_BIN at one."; \
	  exit 1; }
	@mkdir -p $(dir $(REDIS_TEST_LOG))
	@AENV_REDIS_TEST_REQUIRED=1 $(CARGO) test -p aenv-core --lib -- --nocapture \
	  orchestrator::store:: binding_store::redis:: node_registry::redis:: \
	  redis_test_server:: \
	  > $(REDIS_TEST_LOG) 2>&1; status=$$?; \
	  cat $(REDIS_TEST_LOG); \
	  if grep -q 'SKIPPED\[redis\]' $(REDIS_TEST_LOG); then \
	    echo; echo "this run skipped store tests:"; \
	    grep 'SKIPPED\[redis\]' $(REDIS_TEST_LOG); \
	    exit 1; \
	  fi; \
	  if [ $$status -eq 0 ]; then echo; echo "no store tests were skipped"; fi; \
	  exit $$status

# The `src/pg/` suite (per-process pool + advisory-lock election), against a
# real postgres server. Mirrors test-with-redis above: `src/pg/harness.rs`
# borrows the same skip/required convention deliberately, down to the
# `SKIPPED[...]` marker, and without a make target enforcing it, a machine
# without postgres installed silently downgrades every real-server test in
# that suite to a skip and still reports ok — the exact failure mode this
# repository has already been bitten by once, for Redis.
#
# Debian/Ubuntu's postgresql package puts `initdb`/`postgres` under
# /usr/lib/postgresql/<version>/bin/, which is not on PATH; this probe uses
# the same lookup order as `src/pg/harness.rs::find_bin` (INITDB_BIN/
# POSTGRES_BIN override, then PATH, then that versioned layout) so the
# preflight check and the suite itself never disagree about availability.
PG_TEST_LOG ?= target/pg-store-tests.log

test-with-postgres:
	@initdb_bin="$${INITDB_BIN:-$$(command -v initdb 2>/dev/null || ls -1 /usr/lib/postgresql/*/bin/initdb 2>/dev/null | sort -V | tail -1)}"; \
	postgres_bin="$${POSTGRES_BIN:-$$(command -v postgres 2>/dev/null || ls -1 /usr/lib/postgresql/*/bin/postgres 2>/dev/null | sort -V | tail -1)}"; \
	if [ -z "$$initdb_bin" ] || [ ! -x "$$initdb_bin" ] || [ -z "$$postgres_bin" ] || [ ! -x "$$postgres_bin" ]; then \
	  echo "initdb/postgres not found: the pg tests would skip instead of running."; \
	  echo "Install postgresql (e.g. apt install postgresql), or point INITDB_BIN/POSTGRES_BIN at them."; \
	  echo "Debian/Ubuntu puts them under /usr/lib/postgresql/<version>/bin/, off PATH."; \
	  exit 1; \
	fi
	@mkdir -p $(dir $(PG_TEST_LOG))
	@AENV_PG_TEST_REQUIRED=1 $(CARGO) test -p aenv-core -p aenv-api --lib pg:: -- --nocapture \
	  > $(PG_TEST_LOG) 2>&1; status=$$?; \
	  cat $(PG_TEST_LOG); \
	  if grep -q 'SKIPPED\[postgres\]' $(PG_TEST_LOG); then \
	    echo; echo "this run skipped pg tests:"; \
	    grep 'SKIPPED\[postgres\]' $(PG_TEST_LOG); \
	    exit 1; \
	  fi; \
	  if [ $$status -eq 0 ]; then echo; echo "no pg tests were skipped"; fi; \
	  exit $$status

test-integration: test-agent-integration test-envd test-ublk

prepare-agent-test-state:
	$(CAPABILITY_TEST_ENV) $(CARGO) run -p aenv-node --bin aenv-node -- --setup-only

test-agent: prepare-agent-test-state
	$(MAKE) build-ublk PROFILE=debug
	export PATH="$(DEBUG_PROFILE_DIR):$$PATH"; \
	export AENV_UBLK_DAEMON_BINARY_PATH="$(DEBUG_PROFILE_DIR)/uvm-ublk-daemon"; \
	$(CAPABILITY_TEST_ENV) $(CAPABILITY_RUNNER) $(CARGO) test -p aenv-core -p aenv-api -p aenv-node; \
	$(CAPABILITY_TEST_ENV) $(CAPABILITY_RUNNER) $(CARGO) test -p aenv-core -p aenv-api -p aenv-node --lib -- --ignored

# Two cargo invocations over the same test binaries, because the egress tests
# need a different node config than every other integration test and the
# config is read from the process environment. `integration/egress.rs` refuses
# any mode but `embedded` (a `Result::Err` out of a `#[tokio::test]` is a
# failure, not a skip) while `config/default.toml` ships `disabled`; the
# overlay below flips the mode for that invocation only. The `--skip egress::`
# on the first one is what keeps the overlay from becoming the config the
# other tests run under.
test-agent-integration: prepare-agent-test-state
	$(MAKE) build-ublk PROFILE=debug
	PATH="$(DEBUG_PROFILE_DIR):$$PATH" \
	AENV_UBLK_DAEMON_BINARY_PATH="$(DEBUG_PROFILE_DIR)/uvm-ublk-daemon" \
	$(CAPABILITY_TEST_ENV) $(CAPABILITY_RUNNER) $(CARGO) test -p aenv-node \
		--test integration \
		--test orchestrator_integration \
		-- --skip egress::
	PATH="$(DEBUG_PROFILE_DIR):$$PATH" \
	AENV_UBLK_DAEMON_BINARY_PATH="$(DEBUG_PROFILE_DIR)/uvm-ublk-daemon" \
	AENV_CONFIG_OVERLAY_PATH="$(CURDIR)/tests/fixtures/egress-embedded-overlay.toml" \
	$(CAPABILITY_TEST_ENV) $(CAPABILITY_RUNNER) $(CARGO) test -p aenv-node \
		--test integration \
		egress::
	PATH="$(DEBUG_PROFILE_DIR):$$PATH" \
	AENV_UBLK_DAEMON_BINARY_PATH="$(DEBUG_PROFILE_DIR)/uvm-ublk-daemon" \
	$(CAPABILITY_TEST_ENV) $(CAPABILITY_RUNNER) $(CARGO) test -p agentenv-e2e-tests --test snapshot_oss_e2e_test -- --ignored

build-ublk:
	$(CARGO) build $(CARGO_PROFILE_FLAG) -p uvm-ublk -p uvm-ublk-daemon

install-ublk: build-ublk
	$(AENV_INSTALL_SUDO) mkdir -p "$$(dirname "$(UVM_UBLK_DAEMON_INSTALL_PATH)")"
	$(AENV_INSTALL_SUDO) cp "$(TARGET_PROFILE_DIR)/uvm-ublk-daemon" "$(UVM_UBLK_DAEMON_INSTALL_PATH)"
	$(AENV_INSTALL_SUDO) chmod 0755 "$(UVM_UBLK_DAEMON_INSTALL_PATH)"

test-envd:
	bash $(TEST_SCRIPTS_DIR)/test_envd.sh

test-ublk:
	$(CAPABILITY_TEST_ENV) $(CAPABILITY_RUNNER) $(CARGO) test -p uvm-ublk -p overlaybd -p uvm-ublk-daemon
	$(CAPABILITY_TEST_ENV) $(CAPABILITY_RUNNER) $(CARGO) test -p overlaybd --test oss_backend_minio -- --ignored

bench:
	$(MAKE) install-ublk PROFILE=release
	$(CAPABILITY_RUNNER) $(CARGO) bench -p agentenv-benchmarks

bench-snapshot:
	$(MAKE) install-ublk PROFILE=release
	$(CAPABILITY_RUNNER) $(CARGO) bench -p agentenv-benchmarks --bench snapshot

bench-ublk:
	$(MAKE) install-ublk PROFILE=release
	$(CAPABILITY_RUNNER) $(CARGO) bench -p agentenv-benchmarks --bench ublk_overlaybd

bench-orchestrator-store:
	$(CARGO) bench -p agentenv-benchmarks --bench orchestrator_store

# What the placement shadow scorer costs per selection, at N = 2 / 100 / 1000.
# Needs no ublk and no root, unlike `make bench`.
bench-placement-shadow:
	$(CARGO) bench -p agentenv-benchmarks --bench placement_shadow

OCI_IMAGE ?=
bench-oci-conversion:
	$(if $(OCI_IMAGE),AGENTENV_BENCH_OCI_IMAGE="$(OCI_IMAGE)") $(CARGO) bench -p agentenv-benchmarks --bench oci_conversion_pipeline

ci-deps:
	$(MAKE) ci-deps-protoc

ci-deps-protoc:
	$(CARGO) adev codegen --ensure-deps-only

firecracker-client:
	$(CARGO) adev codegen firecracker

envd-http-client:
	$(CARGO) adev codegen envd

agentenv-server:
	$(CARGO) adev codegen server

custom-extension-client:
	$(CARGO) adev codegen custom-extension

test-e2e-compose:
	APT_MIRROR_BASE="$(APT_MIRROR_BASE)" E2E_MODE=compose bash $(TEST_SCRIPTS_DIR)/e2e/run_e2e.sh

test-e2e-k8s:
	APT_MIRROR_BASE="$(APT_MIRROR_BASE)" E2E_MODE=k8s bash $(TEST_SCRIPTS_DIR)/e2e/run_e2e.sh

# Split-address runs: REST at the api half, the data plane at the gateway.
test-e2e-compose-split:
	APT_MIRROR_BASE="$(APT_MIRROR_BASE)" E2E_MODE=compose E2E_SPLIT_ADDRESSES=1 bash $(TEST_SCRIPTS_DIR)/e2e/run_e2e.sh

test-e2e-k8s-split:
	APT_MIRROR_BASE="$(APT_MIRROR_BASE)" E2E_MODE=k8s E2E_SPLIT_ADDRESSES=1 bash $(TEST_SCRIPTS_DIR)/e2e/run_e2e.sh

test-e2e-all: test-e2e-compose test-e2e-k8s

# 🔴 The node half only. There are two binaries, and a developer running one
# machine wants the one that boots VMs.
# Run `aenv-api` beside it when the cluster half is wanted too.
start-server:
	$(MAKE) install-ublk PROFILE=debug
	$(CAPABILITY_RUNNER) $(CARGO) run -p aenv-node --bin aenv-node

start-server-release:
	$(MAKE) install-ublk PROFILE=release
	$(CAPABILITY_RUNNER) $(CARGO) run --release -p aenv-node --bin aenv-node

deploy-up:
	APT_MIRROR_BASE="$(APT_MIRROR_BASE)" $(DOCKER_COMPOSE) -f $(DEPLOY_COMPOSE_FILE) up --build -d

deploy-build:
	APT_MIRROR_BASE="$(APT_MIRROR_BASE)" $(DOCKER_COMPOSE) -f $(DEPLOY_COMPOSE_FILE) build

deploy-up-no-build:
	$(DOCKER_COMPOSE) -f $(DEPLOY_COMPOSE_FILE) up -d

deploy-down:
	$(DOCKER_COMPOSE) -f $(DEPLOY_COMPOSE_FILE) down --remove-orphans

deploy-logs:
	$(DOCKER_COMPOSE) -f $(DEPLOY_COMPOSE_FILE) logs -f

deploy-ps:
	$(DOCKER_COMPOSE) -f $(DEPLOY_COMPOSE_FILE) ps

k8s-build:
	$(DOCKER) build $(if $(APT_MIRROR_BASE),--build-arg APT_MIRROR_BASE="$(APT_MIRROR_BASE)",) --build-arg AENV_GIT_COMMIT="$(AENV_GIT_COMMIT)" -f deploy/docker/Dockerfile.aenv-node -t $(K8S_RUNTIME_IMAGE) .
	$(DOCKER) build $(if $(APT_MIRROR_BASE),--build-arg APT_MIRROR_BASE="$(APT_MIRROR_BASE)",) --build-arg AENV_GIT_COMMIT="$(AENV_GIT_COMMIT)" -f deploy/docker/Dockerfile.aenv-api -t $(K8S_API_IMAGE) .
	$(DOCKER) build -f deploy/docker/Dockerfile.gateway -t $(K8S_GATEWAY_IMAGE) .
	$(DOCKER) build $(if $(APT_MIRROR_BASE),--build-arg APT_MIRROR_BASE="$(APT_MIRROR_BASE)",) --build-arg AENV_GIT_COMMIT="$(AENV_GIT_COMMIT)" -f deploy/docker/Dockerfile.aenv-egress -t $(K8S_EGRESS_IMAGE) .

k8s-redeploy:
	$(KUBECTL) rollout restart deploy/agentenv-gateway -n $(K8S_NAMESPACE)
	$(KUBECTL) rollout restart ds/agentenv-node -n $(K8S_NAMESPACE)
	$(KUBECTL) rollout restart deploy/agentenv-api -n $(K8S_NAMESPACE)
	$(KUBECTL) rollout restart deploy/aenv-egress -n $(K8S_NAMESPACE)
	$(KUBECTL) rollout status deploy/agentenv-gateway -n $(K8S_NAMESPACE)
	$(KUBECTL) rollout status ds/agentenv-node -n $(K8S_NAMESPACE)
	$(KUBECTL) rollout status deploy/agentenv-api -n $(K8S_NAMESPACE)
	$(KUBECTL) rollout status deploy/aenv-egress -n $(K8S_NAMESPACE)

k8s-load-dev:
	$(DOCKER) save $(K8S_RUNTIME_IMAGE) | $(K3S_CTR) images import -
	$(DOCKER) save $(K8S_API_IMAGE) | $(K3S_CTR) images import -
	$(DOCKER) save $(K8S_GATEWAY_IMAGE) | $(K3S_CTR) images import -
	$(DOCKER) save $(K8S_EGRESS_IMAGE) | $(K3S_CTR) images import -

k8s-refresh-dev: k8s-build k8s-load-dev k8s-redeploy

k8s-render:
	bash deploy/k8s/run.sh render

k8s-apply:
	bash deploy/k8s/run.sh apply

k8s-delete:
	bash deploy/k8s/run.sh delete

k8s-render-dev:
	K8S_OVERLAY=local-dev bash deploy/k8s/run.sh render

k8s-apply-dev:
	K8S_OVERLAY=local-dev bash deploy/k8s/run.sh apply

k8s-delete-dev:
	K8S_OVERLAY=local-dev bash deploy/k8s/run.sh delete

services-%:
	$(MAKE) -C services $*

gateway-%:
	$(MAKE) -C services/gateway $*

docs/src/openapi.yml:
	ln -sf ../../src/api/openapi.yml docs/src/openapi.yml

docs: docs/src/openapi.yml
	mdbook build docs

docs-serve: docs/src/openapi.yml
	mdbook serve docs --open
