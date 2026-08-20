-- The three tables that hang off `snapshots`: what is only true of templates,
-- the build queue, and the alias index.
--
-- Order matters and is fixed by the foreign keys — all three point at
-- `snapshots`, so `snapshots` exists first (migration 0001) and these follow.

-- ─────────────────────────────────────────────────────────────────────────────
-- templates
--
-- A template and a snapshot are one entity here, distinguished by
-- SnapshotSource, rather than the two tables e2b keeps. That shape is kept: the
-- row lives in `snapshots` and this table carries only what is true of a
-- template and not of a snapshot.
--
-- 🔴 Two things about this table are unsettled and belong to whoever writes the
-- queries, not to the schema:
--
--   1. As specified it has no column `snapshots` does not already have. Until
--      something template-only lands here, a template row in this table is
--      pure duplication.
--   2. It therefore carries a *second* deleted_at_ms. Two soft-delete flags for
--      one entity can disagree, and the reader that consults the wrong one
--      shows a deleted template. Whoever writes the delete statement must write
--      both in the same transaction, and whoever writes the reads must pick one
--      as authoritative and say so. `snapshots.deleted_at_ms` is the better
--      candidate: it is the one the listing index already filters on.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS templates (
    id            UUID   PRIMARY KEY,
    cluster_id    UUID   NOT NULL,
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    deleted_at_ms BIGINT NULL,
    CONSTRAINT templates_id_fk FOREIGN KEY (id)
        REFERENCES snapshots (id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS templates_cluster_live_idx
    ON templates (cluster_id, created_at_ms DESC, id)
    WHERE deleted_at_ms IS NULL;

-- The read view does not expose deleted_at_ms, so a caller reading through it
-- has no way to forget the filter — and no way to write a query that reads
-- deleted rows by accident.
CREATE OR REPLACE VIEW active_templates AS
SELECT id, cluster_id, created_at_ms, updated_at_ms
  FROM templates
 WHERE deleted_at_ms IS NULL;

-- ─────────────────────────────────────────────────────────────────────────────
-- builds
--
-- Today the API forces build id and template id equal. The table keeps them as
-- two columns anyway: the identity is going to be split, and splitting a table
-- afterwards costs far more than carrying two columns that currently match.
--
-- The template id points at `snapshots`, not at `templates`, and deliberately:
-- the row that a build belongs to is the snapshots row, which is created before
-- anything writes to `templates`.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS builds (
    id              UUID   PRIMARY KEY,
    template_id     UUID   NOT NULL,
    cluster_id      UUID   NOT NULL,

    status          TEXT   NOT NULL
                           CHECK (status IN ('waiting', 'building', 'ready', 'error')),
    status_group    TEXT   NOT NULL
                           CHECK (status_group IN ('pending', 'in_progress', 'ready', 'failed')),

    -- Which machine is running this build.
    node_id         TEXT   NULL,

    -- 🔴 What the reaper stands on, and it is not optional in practice.
    --
    -- The partial unique index below makes one stuck build block that template
    -- forever, so the index and the reaper are one change: shipping the index
    -- without the reaper turns today's leak — a build record stranded at
    -- `building` when the process died — into an outage for that template.
    -- The reaper only acts on rows that carry a heartbeat, so a build admitted
    -- without one is exactly the row nothing can clean up.
    heartbeat_at_ms BIGINT NULL,

    created_at_ms   BIGINT NOT NULL,
    started_at_ms   BIGINT NULL,
    finished_at_ms  BIGINT NULL,
    -- A TemplateBuildErrorReason. Not interpreted here; see snapshots.build_error.
    error_reason    JSONB  NULL,

    CONSTRAINT builds_template_fk FOREIGN KEY (template_id)
        REFERENCES snapshots (id) ON DELETE CASCADE,
    -- The two timestamps state the lifecycle rather than tracking it: a build
    -- that has left `waiting` has started, and a build in a terminal group has
    -- finished. Written as equivalences so neither direction can drift.
    CONSTRAINT builds_started_axis
        CHECK ((status = 'waiting') = (started_at_ms IS NULL)),
    CONSTRAINT builds_finished_axis
        CHECK ((status_group IN ('ready', 'failed')) = (finished_at_ms IS NOT NULL))
);

DROP TRIGGER IF EXISTS builds_status_group_trg ON builds;
CREATE TRIGGER builds_status_group_trg
    BEFORE INSERT OR UPDATE ON builds
    FOR EACH ROW EXECUTE FUNCTION catalog_status_group_trg();

-- 🔴 One live build per template, enforced rather than checked.
--
-- e2b has to query for concurrent builds because it allows several tags per
-- template; we have no tag dimension, so the exclusion can be an index. That is
-- strictly stronger than the query — it also closes the read-modify-write in
-- the object-store backend, where two concurrent POSTs both saw `waiting` and
-- both started a build VM.
--
-- 🔴 It is also what makes heartbeat_at_ms mandatory. See the column.
CREATE UNIQUE INDEX IF NOT EXISTS builds_one_active_per_template
    ON builds (template_id)
    WHERE status_group IN ('pending', 'in_progress');

-- Counting live builds for the cluster-wide admission cap, and the reaper's
-- scan. Partial, so its size tracks what is running rather than what has ever
-- run.
CREATE INDEX IF NOT EXISTS builds_active_idx
    ON builds (cluster_id, heartbeat_at_ms)
    WHERE status_group IN ('pending', 'in_progress');

-- ─────────────────────────────────────────────────────────────────────────────
-- aliases
--
-- The primary key is the uniqueness rule, so binding an alias becomes one
-- INSERT … ON CONFLICT and the 93 lines of read-modify-write-reread in the
-- object-store backend — which describes itself as "weaker than a true CAS" —
-- have nothing left to do.
--
-- 🔴 ON DELETE CASCADE is deliberate and differs from today's behaviour, where
-- a delete only removes an alias that still happens to point at the id being
-- removed. That defensiveness exists because there is no foreign key to lean
-- on. With one, "an alias pointing at a row that is not there" stops being a
-- state, and the two stale-alias sweeps on the read path have to be deleted
-- when reads move here: against a table with this constraint they can only
-- delete legitimate aliases.
--
-- 🔴 CASCADE fires on a *hard* delete, and the catalog's delete is soft. So the
-- soft delete has to remove the alias rows itself, in the same transaction.
-- The foreign key is not the mechanism there; it is the backstop for the GC
-- that eventually removes the row for real.
--
-- No namespace column and no NULLS NOT DISTINCT: that is e2b's multi-tenant
-- naming, and cluster_id is the whole of the scope we have.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS aliases (
    cluster_id    UUID   NOT NULL,
    alias         TEXT   NOT NULL,
    snapshot_id   UUID   NOT NULL,
    created_at_ms BIGINT NOT NULL,
    PRIMARY KEY (cluster_id, alias),
    CONSTRAINT aliases_snapshot_fk FOREIGN KEY (snapshot_id)
        REFERENCES snapshots (id) ON DELETE CASCADE
);

-- 🔴 Unique, not a plain index, and that is a departure worth stating.
--
-- SnapshotRecord.alias is an Option, not a collection: a snapshot has at most
-- one alias. Left unenforced, a second alias row for one snapshot makes the
-- listing query — which joins this table to project the alias — return that
-- snapshot twice inside one page, so a page of ten contains nine snapshots and
-- the "no duplicates" property of keyset pagination is quietly false.
--
-- The cost lands on whoever rebinds an alias: moving an alias to a different
-- snapshot is an UPDATE of the existing row, and giving a snapshot a second
-- alias is refused by this index rather than by a check in the caller. A
-- violation here is not the alias conflict the API reports — that one is the
-- primary key — and must not be reported as one.
--
-- Leading with snapshot_id also serves the cascade the foreign key above needs,
-- so this is one index doing both jobs.
CREATE UNIQUE INDEX IF NOT EXISTS aliases_one_per_snapshot
    ON aliases (snapshot_id);
