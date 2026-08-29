//! The reads that resolve a snapshot — a Rust port of
//! `services/scheduler/internal/catalog/queries_resolved.go`.
//!
//! 🔴 Every one of these carries `status_group = 'ready'` when the caller
//! asks for it, same as Go's file-level rule: the statements that must *not*
//! carry it (the build-status read) belong to a different module for the
//! same reason Go keeps them in a different file — so neither set is edited
//! by accident while looking at the other. See `writes.rs`.

use sqlx::PgPool;
use uuid::Uuid;

use crate::snapshot::repository::interfaces::{
    CatalogReadScope, SnapshotCursor, SnapshotListFilter,
};
use crate::snapshot::repository::{RepositoryError, RepositoryResult};
use crate::snapshot::types::{SnapshotId, SnapshotRecord};

use super::convert::{decode_row, CatalogRow};

/// `status_group = 'ready'` — the whole of the rule, in one place, matching
/// `queries_resolved.go`'s `readyPredicate`.
const READY_PREDICATE: &str = "s.status_group = 'ready'";

/// Every column [`CatalogRow`] scans, in scan order — matches
/// `queries_resolved.go`'s `snapshotColumns` column-for-column, including the
/// `::text` casts on the two uuid columns (see that file's own comment: the
/// decoder must not depend on which uuid codec happens to be registered, and
/// an id compared against a keyset cursor must be compared in the same form
/// the cursor holds it).
const SNAPSHOT_COLUMNS: &str = "s.id::text                AS id,
       s.cluster_id::text        AS cluster_id,
       s.source_kind             AS source_kind,
       s.source_sandbox_id       AS source_sandbox_id,
       s.cpu_count               AS cpu_count,
       s.memory_mib              AS memory_mib,
       s.disk_size_mib           AS disk_size_mib,
       s.status                  AS status,
       s.status_group            AS status_group,
       a.alias                   AS alias,
       s.created_at_ms           AS created_at_ms,
       s.updated_at_ms           AS updated_at_ms,
       s.committed_payload       AS committed_payload,
       s.committed_schema        AS committed_schema,
       s.build_error             AS build_error,
       s.published               AS published,
       s.origin_node_id          AS origin_node_id";

/// At most one alias names this snapshot — safe to join rather than subquery
/// because `aliases_one_per_snapshot` (`0001_initial_schema.sql`) makes a
/// second alias row for the same snapshot a schema violation rather than a
/// possibility.
const ALIAS_JOIN: &str =
    "LEFT JOIN aliases a ON a.snapshot_id = s.id AND a.cluster_id = s.cluster_id";

fn scope_predicate(scope: CatalogReadScope) -> &'static str {
    match scope {
        CatalogReadScope::Resolvable => READY_PREDICATE,
        CatalogReadScope::AnyStatus => "TRUE",
    }
}

/// Reads one row by id or alias, at an explicitly chosen scope. Tries `id`
/// first and falls back to `alias`, matching `queries_resolved.go`'s own
/// comment on why: an alias is allowed to look exactly like a uuid, so the
/// shape of the string is a hint rather than an answer.
pub async fn get_scoped(
    pool: &PgPool,
    cluster_id: Uuid,
    id_or_alias: &str,
    scope: CatalogReadScope,
) -> RepositoryResult<Option<SnapshotRecord>> {
    let scope_sql = scope_predicate(scope);

    if let Ok(id) = SnapshotId::parse(id_or_alias) {
        let sql = format!(
            "SELECT {SNAPSHOT_COLUMNS}\n  FROM snapshots s\n  {ALIAS_JOIN}\n \
             WHERE s.cluster_id = $1 AND s.deleted_at_ms IS NULL AND s.id = $2 AND {scope_sql}"
        );
        let row: Option<CatalogRow> = sqlx::query_as(&sql)
            .bind(cluster_id)
            .bind(id.to_uuid())
            .fetch_optional(pool)
            .await
            .map_err(backend_error("get_snapshot by id"))?;
        if let Some(row) = row {
            return decode_row(row, cluster_id).map(Some);
        }
        // Fall through: an id-shaped string may still be somebody's alias.
    }

    let sql = format!(
        "SELECT {SNAPSHOT_COLUMNS}\n  FROM snapshots s\n  {ALIAS_JOIN}\n \
         WHERE s.cluster_id = $1 AND s.deleted_at_ms IS NULL AND a.alias = $2 AND {scope_sql}"
    );
    let row: Option<CatalogRow> = sqlx::query_as(&sql)
        .bind(cluster_id)
        .bind(id_or_alias)
        .fetch_optional(pool)
        .await
        .map_err(backend_error("get_snapshot by alias"))?;
    row.map(|row| decode_row(row, cluster_id)).transpose()
}

/// Resolves an alias to a snapshot id, at an explicitly chosen scope.
///
/// 🔴 No pin projection here (unlike Go's `resolveAliasSQL`, which also
/// selects `published`/`origin_node_id`) — the general `SnapshotCatalog`
/// trait this backs has no pin-aware resolution surface; see
/// `docs/proposals/_sd-phase4-open-questions-resolved.md` Q3 and the Stage B
/// report's "not done" list for what pin-aware resolution would need.
pub async fn resolve_alias_scoped(
    pool: &PgPool,
    cluster_id: Uuid,
    alias: &str,
    scope: CatalogReadScope,
) -> RepositoryResult<Option<SnapshotId>> {
    let scope_sql = scope_predicate(scope);
    let sql = format!(
        "SELECT s.id::text\n  FROM aliases a\n  JOIN snapshots s ON s.id = a.snapshot_id AND \
         s.cluster_id = a.cluster_id\n WHERE a.cluster_id = $1 AND a.alias = $2 AND \
         s.deleted_at_ms IS NULL AND {scope_sql}"
    );
    let id: Option<String> = sqlx::query_scalar(&sql)
        .bind(cluster_id)
        .bind(alias)
        .fetch_optional(pool)
        .await
        .map_err(backend_error("resolve_alias"))?;
    id.map(|id| {
        SnapshotId::parse(&id)
            .map_err(|_| malformed("resolve_alias", "an alias target that is not a uuid"))
    })
    .transpose()
}

/// The unbounded read: every row matching `filter`, at `scope`, ignoring
/// pagination — for the mirror's history backfill and the population
/// comparison, both of which are counting *everything*.
pub async fn list_scoped(
    pool: &PgPool,
    cluster_id: Uuid,
    filter: &SnapshotListFilter,
    scope: CatalogReadScope,
) -> RepositoryResult<Vec<SnapshotRecord>> {
    let mut binder = Binder::new(cluster_id);
    let sql = list_sql(&mut binder, filter, scope, None);
    let rows: Vec<CatalogRow> = binder
        .apply(sqlx::query_as(&sql))
        .fetch_all(pool)
        .await
        .map_err(backend_error("list_snapshots"))?;
    rows.into_iter()
        .map(|row| decode_row(row, cluster_id))
        .collect()
}

/// One keyset page, pushed all the way into the `WHERE`/`ORDER BY`/`LIMIT` —
/// the pushdown that is the whole point of this backend existing, unlike the
/// object-store backends' "list everything and slice".
pub async fn list_page_scoped(
    pool: &PgPool,
    cluster_id: Uuid,
    filter: &SnapshotListFilter,
    scope: CatalogReadScope,
) -> RepositoryResult<(Vec<SnapshotRecord>, Option<SnapshotCursor>)> {
    let limit = filter.effective_limit();
    if limit == 0 {
        return Ok((Vec::new(), None));
    }

    let mut binder = Binder::new(cluster_id);
    // 🔴 limit + 1: reading one row past the page is what says whether there
    // is another one, matching `listSnapshotsSQL`'s own comment — comparing
    // the page size to the limit instead ends every listing whose total is a
    // multiple of the limit one page early, silently.
    let sql = list_sql(&mut binder, filter, scope, Some(limit as i64 + 1));
    let rows: Vec<CatalogRow> = binder
        .apply(sqlx::query_as(&sql))
        .fetch_all(pool)
        .await
        .map_err(backend_error("list_snapshots"))?;

    let mut records: Vec<SnapshotRecord> = rows
        .into_iter()
        .map(|row| decode_row(row, cluster_id))
        .collect::<RepositoryResult<_>>()?;

    let next = if records.len() > limit as usize {
        records.truncate(limit as usize);
        records
            .last()
            .map(|record| SnapshotCursor::new(record.created_at_unix_ms, record.id.clone()))
    } else {
        None
    };
    Ok((records, next))
}

/// Accumulates bound parameters and hands back their `$n` placeholders, so a
/// filter that is present and one that is absent cannot renumber each other
/// — the same discipline `queries_resolved.go`'s `args` type exists for.
struct Binder {
    values: Vec<Value>,
}

enum Value {
    Text(String),
    TextArray(Vec<String>),
    Uuid(Uuid),
    UuidArray(Vec<Uuid>),
    I64(i64),
}

impl Binder {
    fn new(cluster_id: Uuid) -> Self {
        Self {
            values: vec![Value::Uuid(cluster_id)],
        }
    }

    fn add(&mut self, value: Value) -> String {
        self.values.push(value);
        format!("${}", self.values.len())
    }

    fn apply<'q>(
        &'q self,
        mut query: sqlx::query::QueryAs<
            'q,
            sqlx::Postgres,
            CatalogRow,
            sqlx::postgres::PgArguments,
        >,
    ) -> sqlx::query::QueryAs<'q, sqlx::Postgres, CatalogRow, sqlx::postgres::PgArguments> {
        for value in &self.values {
            query = match value {
                Value::Text(text) => query.bind(text),
                Value::TextArray(texts) => query.bind(texts),
                Value::Uuid(uuid) => query.bind(uuid),
                Value::UuidArray(uuids) => query.bind(uuids),
                Value::I64(n) => query.bind(n),
            };
        }
        query
    }
}

/// Builds one listing query. `limit` is `None` for the unbounded read
/// ([`list_scoped`]) and `Some(limit + 1)` for a keyset page
/// ([`list_page_scoped`]).
fn list_sql(
    binder: &mut Binder,
    filter: &SnapshotListFilter,
    scope: CatalogReadScope,
    limit: Option<i64>,
) -> String {
    let scope_sql = scope_predicate(scope);
    let mut sql = format!(
        "SELECT {SNAPSHOT_COLUMNS}\n  FROM snapshots s\n  {ALIAS_JOIN}\n \
         WHERE s.cluster_id = $1 AND s.deleted_at_ms IS NULL AND {scope_sql}"
    );

    append_filters(&mut sql, binder, filter);

    if let Some(cursor) = &filter.cursor {
        let cursor_id = binder.add(Value::Text(cursor.snapshot_id.to_string()));
        let cursor_ms = binder.add(Value::I64(cursor.created_at_unix_ms));
        // 🔴 Spelled exactly as `listSnapshotsSQL` does: the keyset
        // comparison swaps its operands so the two halves of the tuple sort
        // in opposite directions in one comparison, and both ids compare as
        // text — see that file's own comment on why the ORDER BY below may
        // still use the native column and stay consistent with this.
        sql.push_str(&format!(
            "\n   AND (s.created_at_ms, s.id::text) < ({cursor_ms}::bigint, {cursor_id}::text)"
        ));
    }

    sql.push_str("\n ORDER BY s.created_at_ms DESC, s.id ASC");
    if let Some(limit) = limit {
        let limit_placeholder = binder.add(Value::I64(limit));
        sql.push_str(&format!("\n LIMIT {limit_placeholder}"));
    }
    sql
}

/// 🔴 No branch for `published`/`origin_node_id`, and there must never be
/// one — see rule 5 of the origin-pinning block in
/// `0001_initial_schema.sql`: filtering a listing on them would tell a user a
/// snapshot does not exist when it can only be started on one machine.
fn append_filters(sql: &mut String, binder: &mut Binder, filter: &SnapshotListFilter) {
    if let Some(sources) = &filter.sources {
        if !sources.is_empty() {
            let kinds: Vec<String> = sources
                .iter()
                .map(|kind| source_kind_str(*kind).to_string())
                .collect();
            if kinds.len() == 1 {
                let placeholder = binder.add(Value::Text(kinds.into_iter().next().unwrap()));
                sql.push_str(&format!("\n   AND s.source_kind = {placeholder}"));
            } else {
                let placeholder = binder.add(Value::TextArray(kinds));
                sql.push_str(&format!("\n   AND s.source_kind = ANY({placeholder})"));
            }
        }
    }

    if let Some(prefix) = &filter.alias_prefix {
        let prefix = prefix.trim();
        if !prefix.is_empty() {
            let placeholder = binder.add(Value::Text(prefix.to_string()));
            sql.push_str(&format!("\n   AND starts_with(a.alias, {placeholder})"));
        }
    }

    if let Some(ids) = &filter.snapshot_ids {
        if !ids.is_empty() {
            let uuids: Vec<Uuid> = ids.iter().map(SnapshotId::to_uuid).collect();
            let placeholder = binder.add(Value::UuidArray(uuids));
            sql.push_str(&format!("\n   AND s.id = ANY({placeholder})"));
        }
    }

    if let Some(value) = &filter.snapshot_id_or_alias {
        let value = value.trim();
        if !value.is_empty() {
            if let Ok(id) = SnapshotId::parse(value) {
                let id_placeholder = binder.add(Value::Uuid(id.to_uuid()));
                let alias_placeholder = binder.add(Value::Text(value.to_string()));
                sql.push_str(&format!(
                    "\n   AND (s.id = {id_placeholder} OR a.alias = {alias_placeholder})"
                ));
            } else {
                let placeholder = binder.add(Value::Text(value.to_string()));
                sql.push_str(&format!("\n   AND a.alias = {placeholder}"));
            }
        }
    }

    if let Some(sandbox_id) = &filter.source_sandbox_id {
        let sandbox_id = sandbox_id.trim();
        if !sandbox_id.is_empty() {
            let placeholder = binder.add(Value::Text(sandbox_id.to_string()));
            sql.push_str(&format!("\n   AND s.source_sandbox_id = {placeholder}"));
        }
    }

    if let Some(statuses) = &filter.template_statuses {
        if !statuses.is_empty() {
            let statuses: Vec<String> = statuses
                .iter()
                .map(|status| build_status_str(*status).to_string())
                .collect();
            let placeholder = binder.add(Value::TextArray(statuses));
            sql.push_str(&format!("\n   AND s.status = ANY({placeholder})"));
        }
    }
}

fn source_kind_str(kind: crate::snapshot::types::SnapshotSourceKind) -> &'static str {
    use crate::snapshot::types::SnapshotSourceKind;
    match kind {
        SnapshotSourceKind::Template => "template",
        SnapshotSourceKind::Sandbox => "sandbox",
    }
}

fn build_status_str(status: crate::snapshot::types::TemplateBuildStatus) -> &'static str {
    use crate::snapshot::types::TemplateBuildStatus;
    match status {
        TemplateBuildStatus::Waiting => "waiting",
        TemplateBuildStatus::Building => "building",
        TemplateBuildStatus::Ready => "ready",
        TemplateBuildStatus::Error => "error",
    }
}

pub fn backend_error(operation: &'static str) -> impl Fn(sqlx::Error) -> RepositoryError {
    move |error| RepositoryError::backend(format!("snapshot catalog '{operation}' failed"), error)
}

fn malformed(operation: &'static str, reason: &'static str) -> RepositoryError {
    RepositoryError::Backend {
        message: format!("snapshot catalog '{operation}' answered off contract: {reason}"),
        source: None,
    }
}
