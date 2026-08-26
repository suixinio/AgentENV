//! Task's own "D3": Redis-specific behavior a golden-string or in-memory
//! test cannot demonstrate — real `PTTL`/`KEEPTTL` semantics, and the
//! wire-format compatibility gateway's own Redis reader depends on.

use std::time::Duration;

use redis::AsyncCommands;

use super::harness::store_or_skip;
use crate::binding_store::{
    arbitration::ArbitrationMode, Binding, BindingStore, BindingStoreSettings,
};
use crate::node_registry::types::{Node, RosterEntry};

fn unix(secs: u64) -> std::time::SystemTime {
    std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(secs)
}

fn node(id: &str) -> Node {
    Node {
        id: id.to_string(),
        endpoint: format!("http://{id}"),
        pod_name: String::new(),
    }
}

fn authoritative_settings() -> BindingStoreSettings {
    BindingStoreSettings {
        binding_ttl: Duration::from_secs(30),
        arbitration: ArbitrationMode::Fenced,
        projection_authoritative: true,
    }
}

fn ephemeral_settings() -> BindingStoreSettings {
    BindingStoreSettings {
        binding_ttl: Duration::from_secs(30),
        arbitration: ArbitrationMode::Fenced,
        projection_authoritative: false,
    }
}

async fn pttl_ms(store: &super::RedisBindingStore, sandbox_id: &str) -> i64 {
    let mut conn = store.raw_connection();
    let key = format!("{}:sandbox:{sandbox_id}", store.config().key_prefix);
    conn.pttl(key).await.expect("PTTL")
}

/// The "阶段 1 第 5 点" fix's own reason to exist: under
/// `projection_authoritative = true`, a heartbeat refresh of the *same*
/// incarnation keeps whatever deadline the key already had (`KEEPTTL`)
/// rather than re-arming a fresh one.
#[tokio::test]
async fn authoritative_mode_keeps_the_deadline_on_a_same_incarnation_refresh() {
    let store = store_or_skip!(
        "authoritative_mode_keeps_the_deadline_on_a_same_incarnation_refresh",
        authoritative_settings()
    );

    store
        .reconcile_node(
            node("node-a"),
            vec![RosterEntry {
                sandbox_id: "sbx-1".to_string(),
                execution_id: "exec-1".to_string(),
                projection_ttl: Duration::from_secs(30),
            }],
            unix(0),
        )
        .await
        .unwrap();

    // Simulate time passing: knock the TTL down to something far smaller
    // than the full budget, directly against Redis (the store's own API
    // cannot do this — that is exactly why this test needs raw access).
    {
        let mut conn = store.raw_connection();
        let key = format!("{}:sandbox:sbx-1", store.config().key_prefix);
        let _: () = conn.pexpire(key, 1_000).await.unwrap();
    }
    let before = pttl_ms(&store, "sbx-1").await;
    assert!(
        before > 0 && before <= 1_000,
        "the manual PEXPIRE must have taken effect"
    );

    // A heartbeat refresh of the *same* incarnation, with a full 30s budget
    // on offer.
    store
        .reconcile_node(
            node("node-a"),
            vec![RosterEntry {
                sandbox_id: "sbx-1".to_string(),
                execution_id: "exec-1".to_string(),
                projection_ttl: Duration::from_secs(30),
            }],
            unix(1),
        )
        .await
        .unwrap();

    let after = pttl_ms(&store, "sbx-1").await;
    assert!(
        after <= before,
        "KEEPTTL must have preserved the shortened deadline, not reset it to the full 30s \
         budget (before={before}ms, after={after}ms)"
    );
}

/// The opposite of the above: under `projection_authoritative = false`
/// (rollback/ephemeral mode), every write always re-arms a fresh deadline,
/// even for the same incarnation — proving the mode actually changes
/// behavior, not just the script text (already proven by
/// `scripts::tests::reconcile_scripts_differ_by_projection_authoritative`).
#[tokio::test]
async fn ephemeral_mode_always_rearms_the_deadline() {
    let store = store_or_skip!(
        "ephemeral_mode_always_rearms_the_deadline",
        ephemeral_settings()
    );

    store
        .reconcile_node(
            node("node-a"),
            vec![RosterEntry {
                sandbox_id: "sbx-1".to_string(),
                execution_id: "exec-1".to_string(),
                projection_ttl: Duration::from_secs(30),
            }],
            unix(0),
        )
        .await
        .unwrap();
    {
        let mut conn = store.raw_connection();
        let key = format!("{}:sandbox:sbx-1", store.config().key_prefix);
        let _: () = conn.pexpire(key, 1_000).await.unwrap();
    }

    store
        .reconcile_node(
            node("node-a"),
            vec![RosterEntry {
                sandbox_id: "sbx-1".to_string(),
                execution_id: "exec-1".to_string(),
                projection_ttl: Duration::from_secs(30),
            }],
            unix(1),
        )
        .await
        .unwrap();

    let after = pttl_ms(&store, "sbx-1").await;
    assert!(
        after > 5_000,
        "ephemeral mode must always re-arm the full budget, not keep the shortened deadline \
         (after={after}ms)"
    );
}

/// `KEEPTTL` is never issued against a key with no TTL at all — the guard
/// this store's Lua checks `PTTL > 0` for. A record written with
/// `binding_ttl <= 0`-equivalent (here: the record path is always a real
/// TTL, so this test drives the edge case directly via a raw `PERSIST`)
/// must still end up with a deadline afterward, not be left permanently
/// TTL-less.
#[tokio::test]
async fn a_ttl_less_key_gets_a_deadline_rather_than_staying_ttl_less() {
    let store = store_or_skip!(
        "a_ttl_less_key_gets_a_deadline_rather_than_staying_ttl_less",
        authoritative_settings()
    );

    store
        .reconcile_node(
            node("node-a"),
            vec![RosterEntry {
                sandbox_id: "sbx-1".to_string(),
                execution_id: "exec-1".to_string(),
                projection_ttl: Duration::from_secs(30),
            }],
            unix(0),
        )
        .await
        .unwrap();
    {
        let mut conn = store.raw_connection();
        let key = format!("{}:sandbox:sbx-1", store.config().key_prefix);
        let _: () = conn.persist(key).await.unwrap();
    }
    assert_eq!(
        pttl_ms(&store, "sbx-1").await,
        -1,
        "PERSIST must have actually removed the TTL"
    );

    store
        .reconcile_node(
            node("node-a"),
            vec![RosterEntry {
                sandbox_id: "sbx-1".to_string(),
                execution_id: "exec-1".to_string(),
                projection_ttl: Duration::from_secs(30),
            }],
            unix(1),
        )
        .await
        .unwrap();

    let after = pttl_ms(&store, "sbx-1").await;
    assert!(
        after > 0,
        "a TTL-less key must get a real deadline on the next write, not stay TTL-less forever \
         (after={after})"
    );
}

/// Gateway's `services/shared/routing.Reader` reads this exact key/value
/// shape directly — this is the wire-compatibility golden check, run
/// against a real Redis rather than only asserted at the JSON-string level
/// (`record.rs`'s own tests).
#[tokio::test]
async fn a_recorded_binding_is_byte_compatible_with_gateways_own_reader_shape() {
    let store = store_or_skip!(
        "a_recorded_binding_is_byte_compatible_with_gateways_own_reader_shape",
        ephemeral_settings()
    );
    store
        .record(
            "sbx-1",
            Binding {
                node: node("node-a"),
                execution_id: "0198f5c0-1234-7abc-8def-000000000001".to_string(),
                projection_ttl: Duration::ZERO,
            },
            unix(0),
        )
        .await
        .unwrap();

    let mut conn = store.raw_connection();
    let key = format!("{}:sandbox:sbx-1", store.config().key_prefix);
    let raw: String = conn.get(&key).await.unwrap();
    assert_eq!(
        raw,
        r#"{"node":{"node_id":"node-a","endpoint":"http://node-a"},"execution_id":"0198f5c0-1234-7abc-8def-000000000001"}"#
    );

    let node_index_key = format!("{}:node:node-a", store.config().key_prefix);
    let members: Vec<String> = conn.smembers(&node_index_key).await.unwrap();
    assert_eq!(members, vec!["sbx-1".to_string()]);
}

mod contract {
    use super::super::RedisBindingStore;
    use crate::binding_store::BindingStoreSettings;

    async fn new_contract_store(test: &str) -> Option<RedisBindingStore> {
        super::store_or_skip_option(test, BindingStoreSettings::default()).await
    }

    async fn new_contract_store_with_mode(
        test: &str,
        mode: crate::binding_store::ArbitrationMode,
    ) -> Option<RedisBindingStore> {
        super::store_or_skip_option(
            test,
            BindingStoreSettings {
                arbitration: mode,
                ..BindingStoreSettings::default()
            },
        )
        .await
    }

    crate::binding_store::contract::binding_store_contract!();
    crate::binding_store::contract::binding_store_arbitration_contract!();
}

/// A non-macro variant of `store_or_skip!` for `mod contract`, which needs
/// `Option` rather than an early `return` (the contract macro's own
/// generated test body already does the `let Some(..) = .. else { return }`
/// dance one level up).
async fn store_or_skip_option(
    test: &str,
    settings: BindingStoreSettings,
) -> Option<super::RedisBindingStore> {
    super::harness::store_for(test, settings, |_| {}).await
}
