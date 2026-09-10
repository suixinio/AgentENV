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
	build-ublk install-ublk build-egress \
	fmt clippy check-crate-boundaries \
	mutants coverage \
	test test-unit test-integration test-with-redis test-with-postgres prepare-agent-test-state test-agent test-agent-integration test-envd test-ublk test-nbd test-uffd test-ublk-daemon-nbd \
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
	# --all-features turns test-support on everywhere and hides a test target
	# that forgot to ask for it; the default-feature check does not.
	$(CARGO) check --workspace --all-targets

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
#
# `cargo tree` cannot see inside `aenv-core`, so the module half of the same
# rule is a path test: these run only on a machine that runs sandboxes, and a
# copy of any of them under `src/` is the union crate re-forming.
#
# Each entry is `<path that must stay out of aenv-core>:<the half that owns it
# now>`. Both file shapes of the left-hand module are one rule -- `src/p2p` and
# `src/p2p.rs`, `src/api/proxy.rs` and `src/api/proxy/` -- so bringing it back
# as the other shape is not a way through. The second half is the positive
# control this check would otherwise
# have none of: an absence test passes forever on a path nobody ever writes, so
# a typo in an entry, or a module renamed out from under one, is caught here
# rather than by nothing.
CORE_EXILED_PATHS := \
	src/api/proxy.rs:crates/aenv-node/src/api/proxy.rs \
	src/observability/reporter.rs:crates/aenv-node/src/observability/reporter.rs \
	src/sandbox/network/iptables_util.rs:crates/aenv-node/src/sandbox/network/iptables_util.rs \
	src/sandbox/network/policy_apply.rs:crates/aenv-node/src/sandbox/network/policy_apply.rs \
	src/orchestrator/launch_claim.rs:crates/aenv-node/src/orchestrator/launch_claim.rs \
	src/orchestrator/launch_plan.rs:crates/aenv-node/src/orchestrator/launch_plan.rs \
	src/orchestrator/proxy.rs:crates/aenv-node/src/orchestrator/proxy.rs \
	src/orchestrator/service.rs:crates/aenv-node/src/orchestrator/service.rs \
	src/orchestrator/store/in_memory.rs:crates/aenv-node/src/orchestrator/store/in_memory.rs \
	src/p2p:crates/aenv-node/src/p2p \
	src/record_dir.rs:crates/aenv-node/src/record_dir.rs \
	src/sandbox/envd.rs:crates/aenv-node/src/sandbox/envd.rs \
	src/sandbox/process.rs:crates/aenv-node/src/sandbox/process.rs

# The mirror image: modules only the deciding half runs. A copy of any of them
# under `src/` puts the whole user-facing surface, the cluster's placement and
# its routing store back into every node binary, which is what the role gate
# used to hide.
API_EXILED_PATHS := \
	src/api/impls:crates/aenv-api/src/api/impls \
	src/api/grpc:crates/aenv-api/src/api/grpc \
	src/api/server.rs:crates/aenv-api/src/api/server.rs \
	src/binding_store:crates/aenv-api/src/binding_store \
	src/node_client:crates/aenv-api/src/node_client \
	src/node_registry:crates/aenv-api/src/node_registry \
	src/orchestrator/runtime_routing.rs:crates/aenv-api/src/orchestrator/runtime_routing.rs \
	src/orchestrator/store/redis:crates/aenv-api/src/orchestrator/store/redis \
	src/secrets:crates/aenv-api/src/secrets

# The fourth arm, and the one the two path lists cannot state: `orchestrator`
# is a module name all three crates have. `aenv-core` keeps the model both
# halves read; each half declares its own module over it, with the state
# machine on one side and the Redis store on the other. A half that drops its
# own declaration and re-exports core's instead puts the union crate back
# without moving a file, because every path a caller writes still resolves.
# Each entry is `<half>:<module it owns>`.
HALF_OWNED_MODULES := aenv-node:orchestrator aenv-api:orchestrator

# The third half of the same rule: what each half re-exports from `aenv-core`.
# Read from `crates/<half>/src/lib.rs` with line comments stripped: every
# `pub use aenv_core::`, braced list or single name, alias or nested in an
# inline module, against that file's own top-level `pub mod` list. A name
# `aenv-core` does not have is a module published from the wrong crate. A name
# the half also declares is one path with two owners -- rustc refuses that pair
# only when both sit in the same module, so an inline-module re-export is a
# shape it compiles and this arm does not.
# The dependency-graph rules' patterns, each named once so its exclusion and the
# control proving the pattern still matches something read the same spelling.
# Every tree is `--prefix none | sort -u`, so a pattern anchored at `^` names a
# package rather than a position in a drawn tree, and a name that merely
# contains the word -- `deadpool-redis`, `tokio-postgres` -- is caught rather
# than let past. That reading holds for the exclusion only: on the control side
# a substring is a way through, so any crate whose name happens to carry
# `redis` or `postgres` would satisfy a control the real driver has stopped
# satisfying.
DEP_BYTE_HALF := ^(overlaybd|uvm-nbd|uvm-uffd|uvm-ublk|uvm-ublk-daemon|storage-util) v[0-9]
# The two driver rules are spelled one alternative at a time so that each name
# the api half really carries gets a control of its own: a control over the
# whole alternation is satisfied by any single live name while every other name
# in it rots unnoticed.
DEP_POSTGRES_NAMED := ^[A-Za-z0-9_-]*postgres[A-Za-z0-9_-]* v[0-9]
DEP_POSTGRES_SQLX := ^sqlx([_-][a-z0-9]+)? v[0-9]
# No half carries these, so for them the rule is a closed blacklist with no
# control to prove it, the way DEP_EMBEDDED_DB is for its whole list.
DEP_POSTGRES_ABSENT := ^(diesel([_-][a-z0-9]+)?|sea-orm) v[0-9]
DEP_POSTGRES := $(DEP_POSTGRES_NAMED)|$(DEP_POSTGRES_SQLX)|$(DEP_POSTGRES_ABSENT)
DEP_REDIS_NAMED := ^[A-Za-z0-9_-]*redis[A-Za-z0-9_-]* v[0-9]
DEP_REDIS_ABSENT := ^(fred|rustis) v[0-9]
DEP_REDIS := $(DEP_REDIS_NAMED)|$(DEP_REDIS_ABSENT)
# `redb` is deliberately not on this list: `iroh-blobs` carries it as its own
# blob store, so it is in the tree for a reason that is not node metadata, and
# adding it here would make the rule false rather than stricter.
DEP_EMBEDDED_DB := ^(rocksdb|librocksdb-sys|sled|heed3?|libmdbx([_-][a-z0-9]+)?|persy|fjall) v[0-9]
DEP_P2P_OR_GUEST := ^(iroh([_-][a-z0-9]+)?|envd) v[0-9]
DEP_EGRESS_FORBIDDEN := ^(sqlx|deadpool-postgres|overlaybd|uvm-nbd|uvm-uffd|uvm-ublk|uvm-ublk-daemon|storage-util|aenv-core|rustls|tokio-rustls|hyper-rustls|rcgen) v[0-9]
DEP_EGRESS_TLS_FEATURE := ^aenv-egress feature "tls"
# `aenv-core`'s test seeds are gated on its `test-support` feature, which only
# the halves' dev-dependencies may turn on. `{p} {f}` prints one line per
# package with the features enabled on it, so this names the feature on the
# package rather than anywhere in a tree.
DEP_CORE_TEST_SUPPORT := ^aenv-core v[0-9].*[ ,]test-support([ ,]|$$)
# The same feature on either half, for the workspace-wide reading of the rule:
# cargo unifies features per invocation, so a sibling member's normal edge is
# what the halves' own subtrees cannot see.
DEP_TEST_SEEDS := ^aenv-(core|node) v[0-9].*[ ,]test-support([ ,]|$$)
# The control for the rule above: a feature tree that names no aenv-egress
# feature at all cannot be read for the one feature that is forbidden.
DEP_EGRESS_FEATURE_SHAPE := ^aenv-egress feature "(core|local|resolver|tls)"

check-crate-boundaries:
	@fail=0; \
	matches() { \
	  printf '%s\n' "$$2" | grep -E "$$1"; \
	}; \
	control() { \
	  if ! printf '%s\n' "$$3" | grep -qE "$$1"; then \
	    echo "the pattern excluding $$2 names nothing in $$4, where it must. An exclusion whose"; \
	    echo "pattern has stopped matching anything -- a crate renamed, a tree read the wrong way --"; \
	    echo "passes whatever the graph it guards holds."; \
	    fail=1; \
	  fi; \
	}; \
	api_tree=$$($(CARGO) tree -p aenv-api -e normal --prefix none | sort -u) || { echo "cargo tree -p aenv-api failed"; exit 1; }; \
	node_tree=$$($(CARGO) tree -p aenv-node -e normal --prefix none | sort -u) || { echo "cargo tree -p aenv-node failed"; exit 1; }; \
	if matches '$(DEP_BYTE_HALF)' "$$api_tree"; then \
	  echo "aenv-api links the byte half; the api half runs no sandboxes and must not."; \
	  fail=1; \
	fi; \
	control '$(DEP_BYTE_HALF)' "the byte half" "$$node_tree" "aenv-node's own tree"; \
	if matches '$(DEP_POSTGRES)' "$$node_tree"; then \
	  echo "aenv-node links a PostgreSQL driver; a node must never hold database credentials."; \
	  fail=1; \
	fi; \
	control '$(DEP_POSTGRES_NAMED)' "a driver whose name carries PostgreSQL" "$$api_tree" "aenv-api's own tree"; \
	control '$(DEP_POSTGRES_SQLX)' "sqlx" "$$api_tree" "aenv-api's own tree"; \
	if matches '$(DEP_REDIS)' "$$node_tree"; then \
	  echo "aenv-node links a Redis client. The metadata store every replica reads is the api"; \
	  echo "half's, and a node's own records live in its process; a node opens no Redis."; \
	  fail=1; \
	fi; \
	control '$(DEP_REDIS_NAMED)' "a client whose name carries Redis" "$$api_tree" "aenv-api's own tree"; \
	egress_tree=$$($(CARGO) tree -p aenv-egress -e normal --all-features --prefix none | sort -u) || { echo "cargo tree -p aenv-egress failed"; exit 1; }; \
	if matches '$(DEP_EGRESS_FORBIDDEN)' "$$egress_tree"; then \
	  echo "aenv-egress links a database, the byte half, aenv-core or a second TLS stack; the broker contract is a leaf on openssl only."; \
	  fail=1; \
	fi; \
	control '$(DEP_EGRESS_FORBIDDEN)' "a database, the byte half, aenv-core or a second TLS stack" "$$api_tree" "aenv-api's own tree"; \
	control '^openssl v[0-9]' "nothing (the tree itself)" "$$egress_tree" "aenv-egress's own tree"; \
	for feature in core resolver local tls; do \
	  $(CARGO) check -q -p aenv-egress --no-default-features --features $$feature >/dev/null 2>&1 || { \
	    echo "aenv-egress does not build with only its \"$$feature\" feature. Every build in the tree turns on either one feature or all of them, so a reference from one optional module into another compiles everywhere and fails for whoever enables just the one."; \
	    fail=1; }; \
	done; \
	node_features=$$($(CARGO) tree -p aenv-node -e features --prefix none | sort -u) || { echo "cargo tree -p aenv-node -e features failed"; exit 1; }; \
	if matches '$(DEP_EGRESS_TLS_FEATURE)' "$$node_features"; then \
	  echo "aenv-node enables aenv-egress's tls feature; the node relays bytes and terminates no TLS."; \
	  fail=1; \
	fi; \
	control '$(DEP_EGRESS_FEATURE_SHAPE)' "aenv-egress's tls feature" "$$node_features" "the feature tree that rule reads"; \
	workspace_tree=$$($(CARGO) tree --workspace -e normal,build,dev --prefix none | sort -u) || { echo "cargo tree --workspace failed"; exit 1; }; \
	if matches '$(DEP_EMBEDDED_DB)' "$$workspace_tree"; then \
	  echo "the workspace links an embedded database; node-local metadata is derived from the"; \
	  echo "cache layout or written as JSON records, and nothing here needs one."; \
	  fail=1; \
	fi; \
	control '$(DEP_POSTGRES)' "a PostgreSQL driver" "$$workspace_tree" "the workspace tree the embedded-database rule reads"; \
	if matches '$(DEP_P2P_OR_GUEST)' "$$api_tree"; then \
	  echo "aenv-api links the peer-to-peer transport or the guest agent client; the api half"; \
	  echo "moves no artifact bytes and speaks to no guest, so both are aenv-node's alone."; \
	  fail=1; \
	fi; \
	control '$(DEP_P2P_OR_GUEST)' "the peer-to-peer transport or the guest agent client" "$$node_tree" "aenv-node's own tree"; \
	for half in aenv-api aenv-node; do \
	  shipped=$$($(CARGO) tree -p $$half -e normal --prefix none -f '{p} {f}' | sort -u) || { echo "cargo tree -p $$half -e normal -f '{p} {f}' failed"; exit 1; }; \
	  tested=$$($(CARGO) tree -p $$half -e normal,dev --prefix none -f '{p} {f}' | sort -u) || { echo "cargo tree -p $$half -e normal,dev -f '{p} {f}' failed"; exit 1; }; \
	  if matches '$(DEP_CORE_TEST_SUPPORT)' "$$shipped"; then \
	    echo "$$half turns on aenv-core's test-support feature over a normal dependency edge."; \
	    echo "That feature is what gates the orchestration surface's three test seeds, and cargo"; \
	    echo "unifies features across one invocation: a normal edge puts the seeds into the"; \
	    echo "shipped binary's own aenv-core, not just into a test target's."; \
	    fail=1; \
	  fi; \
	  control '$(DEP_CORE_TEST_SUPPORT)' "aenv-core's test-support feature" "$$tested" "$$half's tree with its dev edges, where the feature is turned on"; \
	done; \
	workspace_shipped=$$($(CARGO) tree --workspace -e normal --prefix none -f '{p} {f}' | sort -u) || { echo "cargo tree --workspace -e normal -f '{p} {f}' failed"; exit 1; }; \
	workspace_tested=$$($(CARGO) tree --workspace -e normal,dev --prefix none -f '{p} {f}' | sort -u) || { echo "cargo tree --workspace -e normal,dev -f '{p} {f}' failed"; exit 1; }; \
	if matches '$(DEP_TEST_SEEDS)' "$$workspace_shipped"; then \
	  echo "a workspace member turns on aenv-core's or aenv-node's test-support feature over a"; \
	  echo "normal dependency edge. Feature unification is per invocation, not per package: a"; \
	  echo "sibling's normal edge hands the seeded build to every binary resolved beside it,"; \
	  echo "which is why this reads the whole workspace and not each half's own subtree."; \
	  fail=1; \
	fi; \
	control '$(DEP_TEST_SEEDS)' "aenv-core's and aenv-node's test-support feature" "$$workspace_tested" "the workspace tree with its dev edges, where the feature is turned on"; \
	for pair in $(CORE_EXILED_PATHS); do \
	  path=$${pair%%:*}; home=$${pair#*:}; stem=$${path%.rs}; \
	  back=""; \
	  for shape in "$$stem" "$$stem.rs"; do \
	    if [ -e "$$shape" ]; then back="$$shape"; fi; \
	  done; \
	  if [ -n "$$back" ]; then \
	    echo "$$back is under aenv-core again. It runs only where sandboxes run, so aenv-node"; \
	    echo "owns it; back here it is compiled into the api binary as well."; \
	    fail=1; \
	  fi; \
	  if [ ! -e "$$home" ]; then \
	    echo "$$home does not exist, so the rule keeping $$path out of aenv-core is watching a"; \
	    echo "path nothing owns and would pass whatever happens. Name where it lives, or drop the pair."; \
	    fail=1; \
	  fi; \
	  case "$$home" in */"$$path") ;; *) \
	    echo "$$home is not $$path under a half's crate: the two spellings must be the same path,"; \
	    echo "or a typo on the left is a rule about nothing and the control on the right cannot see it."; \
	    fail=1;; \
	  esac; \
	done; \
	for pair in $(API_EXILED_PATHS); do \
	  path=$${pair%%:*}; home=$${pair#*:}; stem=$${path%.rs}; \
	  back=""; \
	  for shape in "$$stem" "$$stem.rs"; do \
	    if [ -e "$$shape" ]; then back="$$shape"; fi; \
	  done; \
	  if [ -n "$$back" ]; then \
	    echo "$$back is under aenv-core again. Only the deciding half serves it, so aenv-api"; \
	    echo "owns it; back here it is compiled into every node binary as well."; \
	    fail=1; \
	  fi; \
	  if [ ! -e "$$home" ]; then \
	    echo "$$home does not exist, so the rule keeping $$path out of aenv-core is watching a"; \
	    echo "path nothing owns and would pass whatever happens. Name where it lives, or drop the pair."; \
	    fail=1; \
	  fi; \
	  case "$$home" in */"$$path") ;; *) \
	    echo "$$home is not $$path under a half's crate: the two spellings must be the same path,"; \
	    echo "or a typo on the left is a rule about nothing and the control on the right cannot see it."; \
	    fail=1;; \
	  esac; \
	done; \
	if grep -qE '^[[:space:]]+orchestrator: Arc<dyn NodeOrchestration>' crates/aenv-api/src/api/impls/mod.rs; then \
	  echo "ApiImpl holds the node orchestration surface. The REST layer asks for records,"; \
	  echo "placement and lifecycle transitions; the handle table, the proxy route table and"; \
	  echo "the launches in flight are a running process's, and no deciding half has them."; \
	  fail=1; \
	fi; \
	if ! grep -qE '^[[:space:]]+orchestrator: Arc<dyn SandboxOrchestration>,' crates/aenv-api/src/api/impls/mod.rs; then \
	  echo "crates/aenv-api/src/api/impls/mod.rs declares no orchestration field in the shape"; \
	  echo "this rule reads, so the rule above is watching nothing. Name the field, or drop it."; \
	  fail=1; \
	fi; \
	for half in aenv-api aenv-node; do \
	  flat=$$(sed 's|//.*||' crates/$$half/src/lib.rs | tr '\n' ' '); \
	  braced=$$(printf '%s' "$$flat" | grep -oE 'pub use aenv_core::\{[^}]*\}' | sed -e 's/.*{//' -e 's/}//' | tr -d ' \t' | tr ',' '\n' || true); \
	  named=$$(printf '%s' "$$flat" | grep -oE 'pub use aenv_core::[A-Za-z0-9_]+' | sed 's/.*:://' || true); \
	  reexports=$$(printf '%s\n%s\n' "$$braced" "$$named" | grep -v '^$$' | sort -u || true); \
	  if [ -z "$$reexports" ]; then \
	    echo "crates/$$half/src/lib.rs re-exports nothing from aenv_core; this check reads every"; \
	    echo "'pub use aenv_core::' in the file, braced list or single name, and a file with none"; \
	    echo "leaves it green whatever the halves publish."; \
	    fail=1; \
	  fi; \
	  own=$$(grep -E '^pub mod [a-z0-9_]+;$$' crates/$$half/src/lib.rs | sed 's/^pub mod //; s/;$$//' || true); \
	  if [ -z "$$own" ]; then \
	    echo "crates/$$half/src/lib.rs declares no 'pub mod' of its own; this check reads that"; \
	    echo "list to tell a re-exported path from one the half owns, and an empty list tells it nothing."; \
	    fail=1; \
	  fi; \
	  for module in $$reexports; do \
	    if [ ! -e "src/$$module.rs" ] && [ ! -d "src/$$module" ]; then \
	      echo "crates/$$half/src/lib.rs re-exports aenv_core::$$module, which is no module of"; \
	      echo "aenv-core: whichever half owns it is the only crate that may publish that path."; \
	      fail=1; \
	    fi; \
	    if printf '%s\n' "$$own" | grep -qx "$$module"; then \
	      echo "crates/$$half/src/lib.rs both re-exports aenv_core::$$module and declares its own"; \
	      echo "'pub mod $$module'. One path, one owner: publish core's or the half's, not both."; \
	      fail=1; \
	    fi; \
	  done; \
	done; \
	for pair in $(HALF_OWNED_MODULES); do \
	  half=$${pair%%:*}; module=$${pair#*:}; \
	  if ! grep -qE "^pub mod $$module;$$" crates/$$half/src/lib.rs; then \
	    echo "crates/$$half/src/lib.rs declares no 'pub mod $$module' of its own, so whatever it"; \
	    echo "publishes under that path is aenv-core's and this half owns none of it."; \
	    fail=1; \
	  fi; \
	  if [ ! -d "crates/$$half/src/$$module" ] && [ ! -e "crates/$$half/src/$$module.rs" ]; then \
	    echo "crates/$$half/src/$$module does not exist, so the rule above is watching a path"; \
	    echo "nothing owns. Name where the half's own $$module lives, or drop the pair."; \
	    fail=1; \
	  fi; \
	done; \
	if [ $$fail -eq 0 ]; then echo "crate boundaries hold: aenv-api has no byte half, no p2p transport and no guest agent, aenv-node has no database, no Redis client and no broker TLS, aenv-core holds neither half's runtime, aenv-egress is a leaf whose features each stand alone, the workspace has no embedded database"; fi; \
	exit $$fail

mutants:
	$(CARGO) adev mutants

coverage:
	$(MAKE) install-ublk PROFILE=debug
	PATH="$${CARGO_TARGET_DIR:-$$(pwd)/target}/debug:$$PATH" \
	$(CARGO) adev coverage

test: test-agent test-envd test-ublk test-uffd

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
# Covers every Redis-backed suite: orchestrator::store::redis (the api half's
# sandbox-metadata store), binding_store::redis (the routing/binding store,
# task's own "D3" -- see that module's own KEEPTTL fix, the same defect class
# this target exists to keep catching), and node_registry::redis (the
# shared-roster fix's cross-replica observed-state store). A filter naming
# only some of these would let a newer suite silently skip the same way the
# incident above did for the first one.
#
# `-p aenv-node` for the other half of one suite: the metadata-store contract
# runs against both backends, and the in-memory one lives with the node whose
# records it holds. A run of this target after a contract change has to
# exercise the pair.
REDIS_TEST_LOG ?= target/redis-store-tests.log

test-with-redis:
	@command -v "$${REDIS_SERVER_BIN:-redis-server}" >/dev/null 2>&1 || { \
	  echo "redis-server not found: the metadata store tests would skip instead of running."; \
	  echo "Install redis-server, or point REDIS_SERVER_BIN at one."; \
	  exit 1; }
	@mkdir -p $(dir $(REDIS_TEST_LOG))
	@AENV_REDIS_TEST_REQUIRED=1 $(CARGO) test -p aenv-core -p aenv-api -p aenv-node --lib -- --nocapture \
	  orchestrator::store:: binding_store::redis:: node_registry::redis:: \
	  redis_test_server:: node_client::redis_harness_tests:: \
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
# any mode but `local` (a `Result::Err` out of a `#[tokio::test]` is a
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
	$(MAKE) build-egress
	PATH="$(DEBUG_PROFILE_DIR):$$PATH" \
	AENV_UBLK_DAEMON_BINARY_PATH="$(DEBUG_PROFILE_DIR)/uvm-ublk-daemon" \
	AENV_EGRESS_BINARY_PATH="$(DEBUG_PROFILE_DIR)/aenv-egress" \
	AENV_EGRESS_BROKER_SOCKET_PATH="$(AENV_TEST_STATE_DIR)/run/aenv-egress/broker.sock" \
	AENV_CONFIG_OVERLAY_PATH="$(CURDIR)/tests/fixtures/egress-local-overlay.toml" \
	$(CAPABILITY_TEST_ENV) $(CAPABILITY_RUNNER) $(CARGO) test -p aenv-node \
		--test integration \
		egress::
	PATH="$(DEBUG_PROFILE_DIR):$$PATH" \
	AENV_UBLK_DAEMON_BINARY_PATH="$(DEBUG_PROFILE_DIR)/uvm-ublk-daemon" \
	$(CAPABILITY_TEST_ENV) $(CAPABILITY_RUNNER) $(CARGO) test -p agentenv-e2e-tests --test snapshot_oss_e2e_test -- --ignored

build-ublk:
	$(CARGO) build $(CARGO_PROFILE_FLAG) -p uvm-ublk -p uvm-ublk-daemon

# The broker binary the egress integration tests start as a child. `bin` is
# what carries the `http` handler, the resolver and the Unix listener.
build-egress:
	$(CARGO) build $(CARGO_PROFILE_FLAG) -p aenv-egress --features bin --bin aenv-egress

install-ublk: build-ublk
	$(AENV_INSTALL_SUDO) mkdir -p "$$(dirname "$(UVM_UBLK_DAEMON_INSTALL_PATH)")"
	$(AENV_INSTALL_SUDO) cp "$(TARGET_PROFILE_DIR)/uvm-ublk-daemon" "$(UVM_UBLK_DAEMON_INSTALL_PATH)"
	$(AENV_INSTALL_SUDO) chmod 0755 "$(UVM_UBLK_DAEMON_INSTALL_PATH)"

test-envd:
	bash $(TEST_SCRIPTS_DIR)/test_envd.sh

test-ublk:
	$(CAPABILITY_TEST_ENV) $(CAPABILITY_RUNNER) $(CARGO) test -p uvm-ublk -p overlaybd -p uvm-ublk-daemon
	$(CAPABILITY_TEST_ENV) $(CAPABILITY_RUNNER) $(CARGO) test -p overlaybd --test oss_backend_minio -- --ignored

# The netlink side needs CAP_SYS_ADMIN, which the capability runner grants, and
# /dev/nbd* must be group-accessible to the account it drops to, which
# `scripts/tests/setup-nbd-access.sh` (or `--setup-host`) arranges.
# `AENV_NBD_TEST_REQUIRED=1` turns either one missing into a failure instead of
# a SKIPPED[nbd] line. One device at a time: the tests assert on the kernel's
# view of the device they just created.
test-nbd:
	AENV_NBD_TEST_REQUIRED=1 $(CAPABILITY_TEST_ENV) $(CAPABILITY_RUNNER) $(CARGO) test -p uvm-nbd -- --test-threads=1

# The same daemon suite `test-ublk` runs, over the nbd transport instead. Same
# host requirements as `test-nbd`.
test-ublk-daemon-nbd:
	AENV_NBD_TEST_REQUIRED=1 AENV_DAEMON_TEST_TRANSPORT=nbd $(CAPABILITY_TEST_ENV) $(CAPABILITY_RUNNER) $(CARGO) test -p uvm-ublk-daemon -- --test-threads=1

# The uffd suite creates its own userfaultfd with UFFD_USER_MODE_ONLY, which
# an unprivileged account may do under vm.unprivileged_userfaultfd=0; no
# capability runner is needed.
test-uffd:
	AENV_UFFD_TEST_REQUIRED=1 $(CARGO) test -p uvm-uffd

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
