-- Stage B addition (not ported from services/scheduler/internal/catalog):
-- where "has the PostgreSQL read side ever caught up with object storage"
-- is recorded, so that fact is a cluster fact rather than a node-local one.
--
-- 🔴 This is the structural fix for the CrashLoopBackOff `admit_read_side`
-- used to cause. The comparison itself
-- (`crate::snapshot::repository::mirror::CatalogPopulations::compare`) still
-- runs — this table only changes where its *answer* is remembered. Before
-- this table, the answer lived in each `aenv-api` replica's own
-- `MirrorBacklog` (RocksDB under `$AENV_HOME/snapshot-catalog-mirror`), and
-- `$AENV_HOME` is an `emptyDir` on that role: every fresh replica read an
-- empty local store, concluded "nobody has confirmed this yet", and re-ran
-- the full comparison — which could fail and `bail!` out of startup. Once the
-- comparison has succeeded once, every replica after it can read this row
-- instead of repeating the comparison against live traffic.
--
-- 🔴 Keyed by cluster_id, matching every other table in this schema, on an
-- assumption this migration does not fully verify: that one PostgreSQL
-- database can serve more than one cluster_id at once. If that never
-- happens in practice, the column is harmless — there is exactly one row —
-- but it is kept rather than collapsed to a singleton so the table does not
-- have to be reshaped the day it turns out to matter.
CREATE TABLE IF NOT EXISTS catalog_migration_state (
    cluster_id           UUID    PRIMARY KEY,
    read_side_confirmed  BOOLEAN NOT NULL DEFAULT false,
    confirmed_at_ms       BIGINT NULL,
    -- Audit only, never read by admission logic — the row's existence and
    -- read_side_confirmed are the whole decision.
    confirmed_by_node_id  TEXT   NULL,
    CONSTRAINT catalog_migration_state_confirmed_axis
        CHECK (read_side_confirmed = (confirmed_at_ms IS NOT NULL))
);
