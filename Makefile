SHELL := /usr/bin/env bash
.SHELLFLAGS := -eu -o pipefail -c
.DELETE_ON_ERROR:

# Tools
CARGO ?= cargo
DOCKER ?= docker
DOCKER_COMPOSE ?= docker compose
DEPLOY_COMPOSE_FILE ?= deploy/docker-compose.yml
APT_MIRROR_BASE ?=
KUBECTL ?= kubectl
K8S_NAMESPACE ?= agentenv-system
K8S_RUNTIME_IMAGE ?= agentenv-runtime:latest
K8S_GATEWAY_IMAGE ?= agentenv-gateway:latest
K8S_SCHEDULER_IMAGE ?= agentenv-scheduler:latest
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
	fmt clippy \
	mutants coverage \
	test test-unit test-integration test-with-redis test-with-postgres test-snapshot-catalog prepare-agent-test-state test-agent test-agent-integration test-envd test-ublk \
	test-e2e test-e2e-compose test-e2e-k8s test-e2e-all \
	bench bench-snapshot bench-ublk bench-orchestrator-store \
	ci-deps ci-deps-protoc \
	firecracker-client envd-http-client agentenv-server custom-extension-client start-server start-server-release \
	services gateway scheduler \
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
	$(CARGO) build -p agentenv --bin server

build-server-release:
	$(CARGO) build --release -p agentenv --bin server

build-snapshot-image:
	$(CARGO) build -p agentenv --bin aenv-snapshot-image

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

mutants:
	$(CARGO) adev mutants

coverage:
	$(MAKE) install-ublk PROFILE=debug
	PATH="$${CARGO_TARGET_DIR:-$$(pwd)/target}/debug:$$PATH" \
	$(CARGO) adev coverage

test: test-agent test-envd test-ublk

# 🔴 `--lib --bins`, not `--lib`. A test target that names `--lib` only is
# naming the crate's library and nothing else, and the difference was not
# theoretical: `src/bin/server.rs` holds the tests that guard the `--role`
# split, and this target filtered every one of them out. The run printed
# `running 0 tests ... 1487 passed; 1491 filtered out` and reported ok.
#
# What that hid: `only_the_split_roles_bind_a_second_listener` asserts that
# `assemble_all` never reaches for `spawn_grpc_surface`, because `--role all`
# is the rollback target and is defined as the pre-split process verbatim.
# A `spawn_grpc_surface` mention planted inside `assemble_all` passed
# `make test-unit` green. Those tests read `src/bin/server.rs`'s own source
# text, so moving them into the library would only make them read a sibling
# file — the target selection is the thing that was wrong, not their address.
#
# 🔴 `-p aenv -p adev` for the same reason, one step further along: both are
# bin-only crates, so `--lib` could not have reached them however the package
# list was written, and no other target in this file or in `.github/workflows/`
# names either one. 68 tests that ran nowhere — `make clippy` compiles them
# (`--workspace --all-targets`), so they were type-checked on every PR and
# executed on none. Among them: `aenv download` not overwriting an existing
# file without `--force`, and `aenv upload`'s directory walk refusing to
# follow symlinks out of the tree.
test-unit:
	$(CARGO) test -p agentenv -p envd -p linux-cap -p aenv -p adev --lib --bins
	$(CAPABILITY_TEST_ENV) $(CAPABILITY_RUNNER) $(CARGO) test -p agentenv --lib --bins -- --ignored
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
	@AENV_REDIS_TEST_REQUIRED=1 $(CARGO) test -p agentenv --lib -- --nocapture \
	  orchestrator::store:: binding_store::redis:: node_registry::redis:: \
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
	@AENV_PG_TEST_REQUIRED=1 $(CARGO) test -p agentenv --lib pg:: -- --nocapture \
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

# Throwaway PostgreSQL and scheduler for `test-snapshot-catalog`.
CATALOG_TEST_PG      ?= agentenv-catalog-test-pg
CATALOG_TEST_PG_PORT ?= 15501
CATALOG_TEST_GRPC    ?= 127.0.0.1:19090
CATALOG_TEST_CLUSTER ?= 0198f0a1-0000-7000-8000-0000000c0ffe
CATALOG_TEST_DIR     ?= $(CURDIR)/target/catalog-test

# The central snapshot catalog client, against a real SnapshotCatalog server.
#
# 🔴 Without this, `cargo test --test snapshot_catalog` reports `ok` over a file
# in which every test returned before it did anything. Which refusal a duplicate
# alias produces, whether a `building` row is visible to a caller that never
# mentioned allow_any_status, whether a bytea round-trips a CommittedSnapshot —
# all of them are properties of the *server*, and a stub written from the same
# reading of the proto as the client agrees with the client by construction.
#
# AENV_SNAPSHOT_CATALOG_TEST_REQUIRED turns a missing endpoint into a failure
# rather than back into green skips, for the same reason
# SCHEDULER_REGISTRY_TEST_REQUIRED does on the Go side.
#
# 🔴 Two waits, not one, and the second one fails rather than gives up. The gRPC
# listener comes up *before* the catalog migration has run — the migration is a
# retrying goroutine behind it — so a run that only waited for the socket raced
# it. Waiting on the tables themselves is the check with no gap in it: the thing
# the tests need is the thing being waited for.
#
# Giving up quietly was worse than not waiting. A scheduler left listening by an
# earlier run makes this one exit with "address already in use", the tests then
# talk to the *old* process against a database it never migrated, and the
# result is thirty tests failing with `relation "snapshots" does not exist` —
# which reads as a broken change rather than as a stale process.
test-snapshot-catalog:
	@command -v docker >/dev/null 2>&1 || { echo "docker not found: the catalog tests need a throwaway PostgreSQL"; exit 1; }
	@command -v go >/dev/null 2>&1 || { echo "go not found: the catalog tests need the scheduler built from services/"; exit 1; }
	mkdir -p $(CATALOG_TEST_DIR)
	cd services && go build -o $(CATALOG_TEST_DIR)/scheduler ./scheduler/cmd
	printf '%s\n' '{"log_level":"info","log_format":"json","scheduler":{"grpc_listen_addr":"$(CATALOG_TEST_GRPC)","metrics_listen_addr":"127.0.0.1:19101","strategy":"round_robin","report_ttl":"30s","binding_ttl":"30s","warmup_timeout":"15s","redis_addr":"","nodes":[{"id":"test-node-a","endpoint":"http://127.0.0.1:8000"}]},"gateway":{"http_listen_addr":"127.0.0.1:18080","metrics_listen_addr":"127.0.0.1:19102","scheduler_addr":"$(CATALOG_TEST_GRPC)","request_timeout":"90s","forward_response_size":4194304}}' > $(CATALOG_TEST_DIR)/config.json
	docker run -d --rm --name $(CATALOG_TEST_PG) \
		-e POSTGRES_PASSWORD=verify -e POSTGRES_DB=aenv_registry \
		-p $(CATALOG_TEST_PG_PORT):5432 postgres:16-alpine >/dev/null
	@for i in $$(seq 1 60); do \
		docker exec $(CATALOG_TEST_PG) pg_isready -U postgres >/dev/null 2>&1 && break; \
		sleep 1; \
	done; \
	SCHEDULER_REGISTRY_DSN="postgres://postgres:verify@127.0.0.1:$(CATALOG_TEST_PG_PORT)/aenv_registry" \
	SCHEDULER_REGISTRY_CLUSTER_ID="$(CATALOG_TEST_CLUSTER)" \
	SCHEDULER_REGISTRY_WRITE_ENABLED=true \
	$(CATALOG_TEST_DIR)/scheduler -config $(CATALOG_TEST_DIR)/config.json > $(CATALOG_TEST_DIR)/scheduler.log 2>&1 & \
	scheduler_pid=$$!; \
	for i in $$(seq 1 60); do \
		grep -q "scheduler gRPC server listening" $(CATALOG_TEST_DIR)/scheduler.log 2>/dev/null && break; \
		sleep 1; \
	done; \
	migrated=0; \
	for i in $$(seq 1 60); do \
		docker exec $(CATALOG_TEST_PG) psql -U postgres -d aenv_registry -tAc \
			"select to_regclass('public.snapshots') is not null and to_regclass('public.builds') is not null" \
			2>/dev/null | grep -q '^t$$' && { migrated=1; break; }; \
		sleep 1; \
	done; \
	if [ "$$migrated" != "1" ]; then \
		echo "the catalog migration never ran; see $(CATALOG_TEST_DIR)/scheduler.log"; \
		echo "(a scheduler left listening on $(CATALOG_TEST_GRPC) by an earlier run makes this one exit with 'address already in use')"; \
		kill $$scheduler_pid 2>/dev/null; \
		docker rm -f $(CATALOG_TEST_PG) >/dev/null 2>&1; \
		exit 1; \
	fi; \
	AENV_SNAPSHOT_CATALOG_TEST_ENDPOINT="http://$(CATALOG_TEST_GRPC)" \
	AENV_SNAPSHOT_CATALOG_TEST_CLUSTER_ID="$(CATALOG_TEST_CLUSTER)" \
	AENV_SNAPSHOT_CATALOG_TEST_REQUIRED=1 \
	$(CARGO) test -p agentenv --test snapshot_catalog; \
	status=$$?; \
	kill $$scheduler_pid 2>/dev/null; \
	docker rm -f $(CATALOG_TEST_PG) >/dev/null 2>&1; \
	exit $$status

prepare-agent-test-state:
	$(CAPABILITY_TEST_ENV) $(CARGO) run --bin server -- --setup-only

test-agent: prepare-agent-test-state
	$(MAKE) build-ublk PROFILE=debug
	export PATH="$(DEBUG_PROFILE_DIR):$$PATH"; \
	export AENV_UBLK_DAEMON_BINARY_PATH="$(DEBUG_PROFILE_DIR)/uvm-ublk-daemon"; \
	$(CAPABILITY_TEST_ENV) $(CAPABILITY_RUNNER) $(CARGO) test -p agentenv; \
	$(CAPABILITY_TEST_ENV) $(CAPABILITY_RUNNER) $(CARGO) test -p agentenv --lib -- --ignored

test-agent-integration: prepare-agent-test-state
	$(MAKE) build-ublk PROFILE=debug
	PATH="$(DEBUG_PROFILE_DIR):$$PATH" \
	AENV_UBLK_DAEMON_BINARY_PATH="$(DEBUG_PROFILE_DIR)/uvm-ublk-daemon" \
	$(CAPABILITY_TEST_ENV) $(CAPABILITY_RUNNER) $(CARGO) test -p agentenv \
		--test integration \
		--test orchestrator_integration
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

test-e2e:
	$(MAKE) install-ublk PROFILE=debug
	bash $(TEST_SCRIPTS_DIR)/e2e/run_e2e.sh

test-e2e-compose:
	APT_MIRROR_BASE="$(APT_MIRROR_BASE)" E2E_MODE=compose bash $(TEST_SCRIPTS_DIR)/e2e/run_e2e.sh

test-e2e-k8s:
	APT_MIRROR_BASE="$(APT_MIRROR_BASE)" E2E_MODE=k8s bash $(TEST_SCRIPTS_DIR)/e2e/run_e2e.sh

test-e2e-all: test-e2e test-e2e-compose test-e2e-k8s

start-server:
	$(MAKE) install-ublk PROFILE=debug
	$(CAPABILITY_RUNNER) $(CARGO) run --bin server

start-server-release:
	$(MAKE) install-ublk PROFILE=release
	$(CAPABILITY_RUNNER) $(CARGO) run --release --bin server

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
	$(DOCKER) build $(if $(APT_MIRROR_BASE),--build-arg APT_MIRROR_BASE="$(APT_MIRROR_BASE)",) -f deploy/docker/Dockerfile.agentenv -t $(K8S_RUNTIME_IMAGE) .
	$(DOCKER) build -f deploy/docker/Dockerfile.gateway -t $(K8S_GATEWAY_IMAGE) .
	$(DOCKER) build -f deploy/docker/Dockerfile.scheduler -t $(K8S_SCHEDULER_IMAGE) .

k8s-redeploy:
	$(KUBECTL) rollout restart deploy/agentenv-gateway -n $(K8S_NAMESPACE)
	$(KUBECTL) rollout restart deploy/agentenv-scheduler -n $(K8S_NAMESPACE)
	$(KUBECTL) rollout restart ds/agentenv-node -n $(K8S_NAMESPACE)
	$(KUBECTL) rollout restart deploy/agentenv-api -n $(K8S_NAMESPACE)
	$(KUBECTL) rollout status deploy/agentenv-gateway -n $(K8S_NAMESPACE)
	$(KUBECTL) rollout status deploy/agentenv-scheduler -n $(K8S_NAMESPACE)
	$(KUBECTL) rollout status ds/agentenv-node -n $(K8S_NAMESPACE)
	$(KUBECTL) rollout status deploy/agentenv-api -n $(K8S_NAMESPACE)

k8s-load-dev:
	$(DOCKER) save $(K8S_RUNTIME_IMAGE) | $(K3S_CTR) images import -
	$(DOCKER) save $(K8S_GATEWAY_IMAGE) | $(K3S_CTR) images import -
	$(DOCKER) save $(K8S_SCHEDULER_IMAGE) | $(K3S_CTR) images import -

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

scheduler-%:
	$(MAKE) -C services/scheduler $*

docs/src/openapi.yml:
	ln -sf ../../src/api/openapi.yml docs/src/openapi.yml

docs: docs/src/openapi.yml
	mdbook build docs

docs-serve: docs/src/openapi.yml
	mdbook serve docs --open
