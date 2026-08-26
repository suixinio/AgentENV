use redis::AsyncCommands;

use super::harness::{raw, store_or_skip};
use super::{StoredObservedRecord, GC_GRACE_MULTIPLIER};

fn now_ms() -> i64 {
    super::super::registry::unix_millis(std::time::SystemTime::now())
}

/// A record freshly "heartbeated" right now — `pull_all`'s GC pass compares
/// `last_seen_unix_ms` against *real* wall-clock time, so a fixture meant to
/// survive a pull has to sit near it rather than at an arbitrary fixed
/// offset.
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
        snapshot: None,
        last_seen_unix_ms,
        p2p_endpoint: None,
        report_ttl_secs,
        entries: Vec::new(),
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

    // Just inside the grace window: survives.
    let fresh_enough = now_ms - grace_ms + 5_000;
    store
        .upsert(
            "node-fresh",
            &sample("node-fresh", fresh_enough, report_ttl_secs),
        )
        .await
        .unwrap();
    // Well past the grace window: pruned.
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
