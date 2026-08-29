//! The node clean-up half of Stage A (`docs/proposals/_sd-phase4-stageA-node-inventory.md`):
//! an api-side, growing-in-place port of `services/scheduler/internal`'s node
//! inventory (`node_registry.go`, `kubernetes_discovery.go`, `filter.go`,
//! `strategy.go`, `warmup.go`, `cpu_template.go`).
//!
//! `aenv-api`'s `assemble_api` builds this module's registry unconditionally
//! and answers every placement question from it — see
//! [`crate::node_client::NativeNodePlacement`] (the placement-side consumer)
//! and [`grpc_service`] (the heartbeat-receiving plane that feeds it). It
//! used to be conditional on a now-deleted `[cluster].node_placement_source`
//! switch, with a `"scheduler"` alternative under which `assemble_api` built
//! none of this at all — no registry, no kube client, no gRPC service — but
//! that alternative dialled a Go scheduler process that is deleted from the
//! tree (see "Distributed Control Plane" in the repo's top-level
//! `CLAUDE.md`), so this tree is what every `aenv-api` replica runs now,
//! always, with no switch left to turn it off.
//!
//! # 🔴 P6-e: the Go test-parity count, precisely
//!
//! Stage A's port of `node_registry.go`/`kubernetes_discovery.go`/
//! `filter.go`/`strategy.go`/`warmup.go`/`cpu_template.go` was reported
//! during independent verification as "87/87" Go tests ported — that count
//! is wrong. Counting `^func Test` across the six Go source files
//! (`node_registry_test.go` 20, `node_registry_roster_test.go` 8,
//! `kubernetes_discovery_test.go` 19, `filter_test.go` 17, `strategy_test.go`
//! 2, `warmup_test.go` 7) gives **87** Go tests, matching the label — but 4
//! of those 87 have no Rust counterpart:
//!
//! - **3, deferred to Stage D**: `warmup_test.go`'s
//!   `TestLookupWithholdsNotFoundWhileCold`, `TestLookupReportsNotFoundOnceWarm`,
//!   and `TestQueryOnlyLookupIsNotGated` all drive `Service.LookupNode`
//!   against a `BindingStore`, which this codebase has not ported yet — see
//!   [`warmup`]'s own module doc. (The first of the three now has a
//!   non-byte-for-byte Rust counterpart anyway:
//!   `node_client::native_placement`'s
//!   `node_membership_withholds_gone_while_the_registry_is_cold`, added
//!   fixing the P1 bug that test's Go original is "the whole point" of —
//!   exercising `NativeNodePlacement`, the actual consumer this build wired
//!   the gate to, rather than the not-yet-ported `LookupNode`.)
//! - **1, absorbed as a deliberate duplicate**:
//!   `kubernetes_discovery_test.go`'s `TestLingeringNodeGetsNoScheduleStatusInObservedView`
//!   is already covered by `registry.rs`'s own
//!   `lingering_node_becomes_unhealthy_after_ttl` (its first assertion) —
//!   see the commit that ported `kubernetes_discovery_test.go`'s
//!   registry-focused cases.
//!
//! So the honest count is **83 matched 1:1**, not 87. Summing the Rust test
//! functions in the six corresponding files as they stood at the end of
//! Stage A (`registry.rs` 32, `kubernetes_discovery.rs` 19, `filter.rs` 18,
//! `strategy.rs` 4, `warmup.rs` 4, `cpu_template.rs` 16) gives **93** — so
//! **10 are net-new to the Rust port**, covering behavior the Go tests never
//! isolated on their own (roster normalization edge cases, alias/identity
//! collapse, and the like). None of this counts the tests added afterward,
//! in `node_client::native_placement`, `api::server`, or
//! `observability::reporter`, to guard the independent-review fixes
//! (`node_registry_grpc_service`'s own test count is separate for the same
//! reason: it has no Go file of its own, `Heartbeat`'s Go home being
//! `node_registry.go` itself) — those are a different accounting entirely,
//! for bugs this review found rather than for port parity.

pub mod cpu_template;
pub mod filter;
pub mod grpc_service;
pub mod kubernetes_discovery;
pub mod redis;
pub mod registry;
pub mod static_discovery;
pub mod strategy;
pub mod types;
pub mod warmup;
