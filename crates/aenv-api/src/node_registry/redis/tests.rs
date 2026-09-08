use redis::AsyncCommands;

use super::harness::{raw, raw_machine, store_or_skip};
use super::{StoredMachineInfo, StoredObservedRecord, GC_GRACE_MULTIPLIER};

fn now_ms() -> i64 {
    super::super::registry::unix_millis(std::time::SystemTime::now())
}

/// Builds a record fresh enough to survive wall-clock garbage collection.
fn fresh(node_id: &str, report_ttl_secs: u64) -> StoredObservedRecord {
    sample(node_id, now_ms(), report_ttl_secs)
}

fn sample(node_id: &str, last_seen_unix_ms: i64, report_ttl_secs: u64) -> StoredObservedRecord {
    StoredObservedRecord {
        node_id: node_id.to_string(),
        endpoint: format!("http://{node_id}:8000"),
        cluster_id: "cluster-a".to_string(),
        service_instance_id: "instance-1".to_string(),
        version: "v1".to_string(),
        commit: "abc123".to_string(),
        machine_info: None,
        machine_digest: None,
        snapshot: None,
        last_seen_unix_ms,
        p2p_endpoint: None,
        report_ttl_secs,
        entries: Vec::new(),
    }
}

fn machine_info(cpu_config_json: &str) -> StoredMachineInfo {
    StoredMachineInfo {
        cpu_family: "6".to_string(),
        cpu_model: "154".to_string(),
        cpu_model_name: "test-cpu".to_string(),
        cpu_architecture: "x86_64".to_string(),
        cpu_config_json: cpu_config_json.to_string(),
    }
}

/// Builds a fresh record with machine information.
fn fresh_with_machine(
    node_id: &str,
    report_ttl_secs: u64,
    cpu_config_json: &str,
) -> StoredObservedRecord {
    StoredObservedRecord {
        machine_info: Some(machine_info(cpu_config_json)),
        ..fresh(node_id, report_ttl_secs)
    }
}

#[tokio::test]
async fn a_pushed_record_comes_back_from_a_pull() {
    let store = store_or_skip!("a_pushed_record_comes_back_from_a_pull");
    let record = fresh("node-a", 30);
    store.upsert("node-a", &record).await.unwrap();

    let pulled = store.pull_all().await.unwrap();
    let got = pulled.get("node-a").expect("node-a should be in the pull");
    assert_eq!(got.endpoint, record.endpoint);
    assert_eq!(got.cluster_id, record.cluster_id);
    assert_eq!(got.last_seen_unix_ms, record.last_seen_unix_ms);
}

#[tokio::test]
async fn a_removed_record_is_gone_from_the_next_pull_and_from_redis_itself() {
    let store = store_or_skip!("a_removed_record_is_gone_from_the_next_pull_and_from_redis_itself");
    store.upsert("node-a", &fresh("node-a", 30)).await.unwrap();
    store.remove("node-a").await.unwrap();

    let pulled = store.pull_all().await.unwrap();
    assert!(
        !pulled.contains_key("node-a"),
        "a removed node must not reappear in a pull"
    );

    let (mut conn, hash_key) = raw(&store);
    let exists: bool = conn.hexists(&hash_key, "node-a").await.unwrap();
    assert!(!exists, "HDEL must have actually removed the hash field");
}

#[tokio::test]
async fn pull_all_prunes_an_entry_stale_past_its_own_gc_grace_window() {
    let store = store_or_skip!("pull_all_prunes_an_entry_stale_past_its_own_gc_grace_window");
    let report_ttl_secs = 30u64;
    let grace_ms = (report_ttl_secs * GC_GRACE_MULTIPLIER * 1000) as i64;
    let now_ms = now_ms();

    let fresh_enough = now_ms - grace_ms + 5_000;
    store
        .upsert(
            "node-fresh",
            &sample("node-fresh", fresh_enough, report_ttl_secs),
        )
        .await
        .unwrap();
    let too_stale = now_ms - grace_ms - 5_000;
    store
        .upsert(
            "node-stale",
            &sample("node-stale", too_stale, report_ttl_secs),
        )
        .await
        .unwrap();

    let pulled = store.pull_all().await.unwrap();
    assert!(
        pulled.contains_key("node-fresh"),
        "a record inside its own GC grace window must survive a pull"
    );
    assert!(
        !pulled.contains_key("node-stale"),
        "a record past GC_GRACE_MULTIPLIER * report_ttl_secs must be pruned by a pull"
    );

    let (mut conn, hash_key) = raw(&store);
    let exists: bool = conn.hexists(&hash_key, "node-stale").await.unwrap();
    assert!(
        !exists,
        "pull_all's GC pass must actually HDEL the stale field, not just omit it from the \
         returned map"
    );
}

#[tokio::test]
async fn pull_all_skips_an_undecodable_field_without_failing_the_whole_pull() {
    let store =
        store_or_skip!("pull_all_skips_an_undecodable_field_without_failing_the_whole_pull");
    store
        .upsert("node-good", &fresh("node-good", 30))
        .await
        .unwrap();

    let (mut conn, hash_key) = raw(&store);
    let _: () = conn
        .hset(&hash_key, "node-garbage", "not valid json at all")
        .await
        .unwrap();

    let pulled = store
        .pull_all()
        .await
        .expect("one bad field must not fail the whole pull");
    assert!(pulled.contains_key("node-good"));
    assert!(!pulled.contains_key("node-garbage"));
}

#[tokio::test]
async fn a_machine_info_survives_the_hot_and_side_hash_split() {
    let store = store_or_skip!("a_machine_info_survives_the_hot_and_side_hash_split");
    let record = fresh_with_machine("node-a", 30, "cpuid-a");
    store.upsert("node-a", &record).await.unwrap();

    let pulled = store.pull_all().await.unwrap();
    let got = pulled.get("node-a").expect("node-a should be in the pull");
    assert_eq!(
        got.machine_info
            .as_ref()
            .map(|m| m.cpu_config_json.as_str()),
        Some("cpuid-a"),
        "pull_all must resolve machine_info back onto the record it hands out"
    );

    let (mut conn, hash_key) = raw(&store);
    let raw_hot: Option<String> = conn.hget(&hash_key, "node-a").await.unwrap();
    let decoded: StoredObservedRecord =
        serde_json::from_str(&raw_hot.expect("node-a must still be in the hot hash")).unwrap();
    assert!(
        decoded.machine_info.is_none(),
        "the hot hash must never carry machine_info directly -- see this module's own \
         splitting doc"
    );
    assert!(
        decoded.machine_digest.is_some(),
        "the hot hash's own copy must name a machine_digest so a puller knows there is \
         something to resolve"
    );
}

#[tokio::test]
async fn an_unchanged_machine_info_is_not_rewritten_but_a_changed_one_still_is() {
    let store =
        store_or_skip!("an_unchanged_machine_info_is_not_rewritten_but_a_changed_one_still_is");
    let (mut conn, machine_hash_key) = raw_machine(&store);

    store
        .upsert("node-a", &fresh_with_machine("node-a", 30, "cpuid-a"))
        .await
        .unwrap();

    // Sentinel reveals whether an unchanged upsert rewrites the side hash.
    let sentinel = "SENTINEL-not-actually-cpuid-a";
    let _: () = conn
        .hset(&machine_hash_key, "node-a", sentinel)
        .await
        .unwrap();

    store
        .upsert("node-a", &fresh_with_machine("node-a", 30, "cpuid-a"))
        .await
        .unwrap();
    let after_unchanged: Option<String> = conn.hget(&machine_hash_key, "node-a").await.unwrap();
    let after_unchanged = after_unchanged.expect("node-a must still be in the side hash");
    assert_eq!(
        after_unchanged, sentinel,
        "an upsert whose machine_info content is unchanged must not rewrite the side hash"
    );

    store
        .upsert("node-a", &fresh_with_machine("node-a", 30, "cpuid-a-v2"))
        .await
        .unwrap();
    let after_changed: Option<String> = conn.hget(&machine_hash_key, "node-a").await.unwrap();
    let after_changed = after_changed.expect("node-a must still be in the side hash");
    assert_ne!(
        after_changed, sentinel,
        "an upsert whose machine_info content actually changed must still rewrite the side hash"
    );
    let decoded: StoredMachineInfo = serde_json::from_str(&after_changed).unwrap();
    assert_eq!(decoded.cpu_config_json, "cpuid-a-v2");
}

#[tokio::test]
async fn pull_all_reads_a_pre_split_inline_record_from_an_old_peer() {
    let store = store_or_skip!("pull_all_reads_a_pre_split_inline_record_from_an_old_peer");
    let (mut conn, hash_key) = raw(&store);

    // Pre-split wire shape has inline machine info and no digest field.
    let old_format = serde_json::json!({
        "node_id": "node-old",
        "endpoint": "http://node-old:8000",
        "cluster_id": "cluster-a",
        "service_instance_id": "instance-1",
        "version": "v1",
        "commit": "abc123",
        "machine_info": {
            "cpu_family": "6",
            "cpu_model": "154",
            "cpu_model_name": "test-cpu",
            "cpu_architecture": "x86_64",
            "cpu_config_json": "cpuid-old-peer",
        },
        "snapshot": null,
        "last_seen_unix_ms": now_ms(),
        "p2p_endpoint": null,
        "report_ttl_secs": 30,
        "entries": [],
    });
    let _: () = conn
        .hset(&hash_key, "node-old", old_format.to_string())
        .await
        .unwrap();

    let pulled = store.pull_all().await.unwrap();
    let got = pulled
        .get("node-old")
        .expect("a pre-split record must still be readable by pull_all");
    assert_eq!(
        got.machine_info
            .as_ref()
            .map(|m| m.cpu_config_json.as_str()),
        Some("cpuid-old-peer"),
        "pull_all must accept an old-format record's inline machine_info as authoritative"
    );
}

#[tokio::test]
async fn removing_a_node_also_clears_its_side_hash_entry() {
    let store = store_or_skip!("removing_a_node_also_clears_its_side_hash_entry");
    store
        .upsert("node-a", &fresh_with_machine("node-a", 30, "cpuid-a"))
        .await
        .unwrap();
    let (mut conn, machine_hash_key) = raw_machine(&store);
    let existed: bool = conn.hexists(&machine_hash_key, "node-a").await.unwrap();
    assert!(
        existed,
        "the upsert above should have populated the side hash"
    );

    store.remove("node-a").await.unwrap();

    let exists: bool = conn.hexists(&machine_hash_key, "node-a").await.unwrap();
    assert!(
        !exists,
        "remove must HDEL the side-hash entry, not just the hot-hash one"
    );
}
