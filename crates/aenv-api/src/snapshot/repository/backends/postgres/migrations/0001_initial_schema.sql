-- The whole snapshot catalog schema, in one file.
--
-- 🔴 This is a squash, not a first draft. Five files used to live here — the
-- three `services/scheduler` shipped (`snapshots`, then the three tables that
-- hang off it, then the `disk_size_mib` rule moving from insert time to
-- `ready`) plus a pair that created `catalog_migration_state` and dropped it
-- again. They are collapsed into the end state of applying all five, because
-- the database they were incrementally migrating was never released: there is
-- no deployment holding rows this file has to preserve, and no Go migrator
-- left to agree with about version numbers. A cluster is created from empty.
--
-- Two things the squash deliberately drops rather than reproduces:
--
--   * `snapshots.publishing_execution_id`. Written as literal NULL by the only
--     INSERT that named it and read by nothing, ever — a column reserved for a
--     fencing predicate that was never added. `StagedSnapshot.execution_id`,
--     the value that was supposed to feed it, is deleted with it.
--   * `catalog_migration_state`. Created by the old 0004 and dropped by the old
--     0005; the read-side confirmation gate it recorded for went with the
--     object-storage catalog. A create followed by a drop is not a schema.
--
-- 🔴 Order in this file is fixed by dependencies, not taste. Both trigger
-- functions come first because two tables attach triggers to
-- `catalog_status_group_trg`; `snapshots` comes before `templates`, `builds`
-- and `aliases` because all three carry a foreign key into it.
--
-- 🔴 Time is BIGINT milliseconds, not timestamptz, throughout the catalog.
-- SnapshotRecord.created_at_unix_ms is an i64 of milliseconds and the public
-- pagination cursor is rendered from it; a round trip through timestamptz's
-- microseconds and back out as an RFC3339 string moves page boundaries. The
-- price is honest and paid on purpose: no now() defaults, no date functions,
-- and every operator query divides by 1000.
--
-- Every statement is idempotent (IF NOT EXISTS / OR REPLACE / DROP-then-CREATE)
-- so the file can be re-run against a database that already has part of it.

-- ─────────────────────────────────────────────────────────────────────────────
-- Trigger functions
-- ─────────────────────────────────────────────────────────────────────────────

-- status_group is derived, always, on both tables that carry it. Inlined into
-- one shared function rather than written twice, but the body is spelled with
-- literals only: a trigger body resolves unqualified names through the
-- *session's* search_path at execution time, so calling out to a second helper
-- function would make the mapping depend on a setting the caller controls.
CREATE OR REPLACE FUNCTION catalog_status_group_trg() RETURNS TRIGGER AS $$
BEGIN
    NEW.status_group := CASE NEW.status
        WHEN 'waiting'  THEN 'pending'
        WHEN 'building' THEN 'in_progress'
        WHEN 'ready'    THEN 'ready'
        ELSE 'failed'
    END;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- updated_at_ms follows the caller when the caller states it, and the clock
-- when it does not.
--
-- 🔴 It does not simply overwrite, and the reason outlives the double-write
-- phase it was first written for: a caller that has computed the exact instant
-- a row changed — the staging clock read that `SnapshotRepository::stage` takes
-- once and uses for every store — must be able to write it. A server-side stamp
-- would silently move every such value by the latency of the statement that
-- carried it.
CREATE OR REPLACE FUNCTION snapshots_touch_updated_at_trg() RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.updated_at_ms IS NULL THEN
            NEW.updated_at_ms := (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT;
        END IF;
    ELSIF NEW.updated_at_ms IS NOT DISTINCT FROM OLD.updated_at_ms THEN
        NEW.updated_at_ms := (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ─────────────────────────────────────────────────────────────────────────────
-- snapshots
--
-- The catalog's main table, and the only one the other three point at.
--
-- It holds what a SnapshotRecord is: identity, source, resources, lifecycle
-- state, and one opaque blob. The blob is a CommittedSnapshot exactly as the
-- node serialised it and nothing here reads it — every column beside it exists
-- so that a query can index, filter, order or fence without unpacking it.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS snapshots (
    id                      UUID    PRIMARY KEY,
    cluster_id              UUID    NOT NULL,

    -- The discriminant of SnapshotSource, plus the one field only its Sandbox
    -- arm has. Stated as an equivalence rather than as two independent columns,
    -- so a template row cannot quietly carry a sandbox id and a sandbox row
    -- cannot quietly lose one.
    source_kind             TEXT    NOT NULL
                                    CHECK (source_kind IN ('template', 'sandbox')),
    source_sandbox_id       TEXT    NULL,
    CONSTRAINT snapshots_source_axis
        CHECK ((source_kind = 'sandbox') = (source_sandbox_id IS NOT NULL)),

    -- SandboxResources. INTEGER rather than BIGINT because the Rust side is
    -- u32.
    --
    -- 🔴 cpu_count and memory_mib carry a positivity CHECK at insert time and
    -- disk_size_mib deliberately does not — see the two named disk-size rules
    -- below. Both of the first two are known at create (the create request
    -- carries them, or the node's config default fills them in), so a 0 in
    -- either is a real error and the earliest possible refusal is the right one
    -- for a real error.
    cpu_count               INTEGER NOT NULL CHECK (cpu_count  > 0),
    memory_mib              INTEGER NOT NULL CHECK (memory_mib > 0),
    disk_size_mib           INTEGER NOT NULL,

    status                  TEXT    NOT NULL
                                    CHECK (status IN ('waiting', 'building', 'ready', 'error')),
    -- Four states folded into four groups. Derived from `status` by a trigger
    -- below, never written by a caller.
    --
    -- A plain column and not GENERATED: a generated column cannot be used in a
    -- partial index predicate on the versions we have to support, and the
    -- partial indexes are the entire reason this column exists.
    status_group            TEXT    NOT NULL
                                    CHECK (status_group IN ('pending', 'in_progress', 'ready', 'failed')),

    -- ── origin pinning ────────────────────────────────────────────────────
    --
    -- 🔴 These two columns are a block, and the block is designed to be
    -- dropped. When a publish can no longer fail permanently, the removal is
    -- one file: drop the partial index below, drop the named constraint, drop
    -- the two columns. DROP COLUMN is metadata-only in PostgreSQL, so that
    -- costs nothing — provided the columns never got tangled into anything
    -- else. Six rules keep them untangled and each one is load-bearing:
    --
    --   1. Neither appears in a PRIMARY KEY, a UNIQUE constraint or a foreign
    --      key. Otherwise dropping a column rebuilds constraints and the
    --      indexes behind them.
    --   2. 🔴 `published` is never merged into `status_group`. They answer
    --      different questions. `status_group = 'ready'` asks "did the capture
    --      finish — is there a snapshot that can run?", and a publish that
    --      failed answers yes: the snapshot is complete and its origin node can
    --      start it. `published` asks "did the bytes reach shared storage —
    --      can anybody else start it?", and that one answers no. Fold them and
    --      the drop turns into a backfill.
    --   3. They appear in exactly one index, snapshots_unpublished_idx, which
    --      is dropped by the same file. Scattering them through composite
    --      indexes would make the drop rebuild each one.
    --   4. The constraint tying them together is named, so the drop can be
    --      written with DROP CONSTRAINT IF EXISTS and stay idempotent. An
    --      anonymous CHECK gets a server-generated name nobody can predict.
    --   5. 🔴 Neither ever appears in a WHERE clause outside that one index
    --      predicate. Resolving queries project them and hand them to a single
    --      function that decides pinning. Filtering on them would mean the drop
    --      has to edit every query — and, worse, would hide a snapshot the user
    --      can still resume on its origin node behind "no such snapshot".
    --      `reads.rs`'s `append_filters` states the same rule from the query
    --      side.
    --   6. origin_node_id stays NULLable forever. A later SET NOT NULL has to
    --      be unwound before the column can go, and the intermediate state
    --      rejects writes.
    published               BOOLEAN NOT NULL DEFAULT true,
    origin_node_id          TEXT    NULL,
    CONSTRAINT snapshots_origin_axis
        CHECK (published OR origin_node_id IS NOT NULL),

    -- When the sandbox this snapshot came from first started. NULL for
    -- templates.
    sandbox_started_at_ms   BIGINT  NULL,

    created_at_ms           BIGINT  NOT NULL,
    updated_at_ms           BIGINT  NOT NULL,
    -- Soft delete. Hard deletion would take an alias, and possibly an image
    -- reference somebody exported to a registry, with it and leave nothing to
    -- trace. Readers filter on this; the view over `templates` hides it from
    -- callers entirely so they cannot forget to.
    deleted_at_ms           BIGINT  NULL,

    -- 🔴 A CommittedSnapshot as the node serialised it. Nothing in this process
    -- deserialises it or branches on its contents. The version travels beside
    -- it so the node can refuse an encoding it does not know; the pair is
    -- all-or-nothing.
    committed_payload       BYTEA   NULL,
    committed_schema        INTEGER NULL,
    CONSTRAINT snapshots_committed_axis
        CHECK ((committed_payload IS NULL) = (committed_schema IS NULL)),
    -- The one rule that keeps a half-published snapshot from being started:
    -- `ready` is exactly the state in which a payload exists.
    CONSTRAINT snapshots_ready_is_committed
        CHECK (status <> 'ready' OR committed_payload IS NOT NULL),

    -- A TemplateBuildErrorReason. JSONB so an operator can read it; still not
    -- interpreted here — the type has a hand-written deserialiser with a legacy
    -- string form, and duplicating that logic in SQL is how the two drift.
    build_error             JSONB   NULL,
    CONSTRAINT snapshots_error_axis
        CHECK (status <> 'error' OR build_error IS NOT NULL),

    -- ── disk_size_mib, at the point where a launch becomes possible ───────
    --
    -- 🔴 0 is not a missing value in this column, it is *the* value for a
    -- snapshot whose disk size is not known yet, and that convention comes from
    -- the node, not from here. A v3 template genuinely has no disk size at
    -- create time: the E2B-compatible TemplateBuildRequestV3 carries name,
    -- tags, cpuCount and memoryMB and has no disk field at all, so there is
    -- nothing a node could send instead. The real figure is the produced
    -- rootfs's virtual size and it exists only once a build has produced a
    -- rootfs — src/template/builder.rs fills resources.disk_size_mib from
    -- build_execution.manifest.rootfs.virtual_size, and src/template/
    -- build_spec.rs states the convention in as many words:
    -- `disk_size_mib: 0, // disk size is determined after build.`. The other
    -- end reads it the same way round — the Firecracker factory takes 0 as "no
    -- explicit rootfs size, use the image's natural size".
    --
    -- 🔴 So there is no `disk_size_mib > 0` column CHECK, and adding one back
    -- would refuse every v3 template create on the cluster with InvalidArgument
    -- before a build could ever produce the number it wanted. The launch rule
    -- is gated on `ready` instead, exactly the way the payload rule above
    -- already is: a `waiting` or `building` row is not launchable and can never
    -- become one by accident, because every resolving query filters on
    -- status_group = 'ready' (that predicate is what snapshots_list_idx is
    -- built on).
    --
    -- The floor is what the dropped positivity CHECK used to provide for free.
    -- The column is INTEGER, which is signed, and the store casts a uint32 to
    -- int32 on the way in — so "not known yet" is 0 and everything below it is
    -- a value that arrived corrupted rather than one that means anything.
    CONSTRAINT snapshots_disk_size_floor
        CHECK (disk_size_mib >= 0),
    CONSTRAINT snapshots_ready_has_disk_size
        CHECK (status <> 'ready' OR disk_size_mib > 0)
);

-- DROP then CREATE rather than CREATE OR REPLACE TRIGGER: the replacing form
-- needs PostgreSQL 14, and this pair is idempotent everywhere.
DROP TRIGGER IF EXISTS snapshots_status_group_trg ON snapshots;
CREATE TRIGGER snapshots_status_group_trg
    BEFORE INSERT OR UPDATE ON snapshots
    FOR EACH ROW EXECUTE FUNCTION catalog_status_group_trg();

DROP TRIGGER IF EXISTS snapshots_updated_at_trg ON snapshots;
CREATE TRIGGER snapshots_updated_at_trg
    BEFORE INSERT OR UPDATE ON snapshots
    FOR EACH ROW EXECUTE FUNCTION snapshots_touch_updated_at_trg();

-- The keyset listing index. Its column order and its directions are the
-- ORDER BY of the listing query, spelled the same way round; a query that
-- disagrees with it falls back to sorting the table.
--
-- 🔴 status_group = 'ready' is in the *predicate*, not the key. That turns "the
-- resolving queries must all carry the ready predicate" from a rule somebody
-- has to remember into one that answers slowly when it is broken — a query
-- missing the predicate drops out of this index and shows up in timings.
CREATE INDEX IF NOT EXISTS snapshots_list_idx
    ON snapshots (cluster_id, source_kind, created_at_ms DESC, id)
    WHERE deleted_at_ms IS NULL AND status_group = 'ready';

-- Serves `GET /snapshots?sandboxID=…`.
CREATE INDEX IF NOT EXISTS snapshots_source_sandbox_idx
    ON snapshots (cluster_id, source_sandbox_id)
    WHERE source_sandbox_id IS NOT NULL AND deleted_at_ms IS NULL;

-- 🔴 The only index the origin block appears in — see rule 3 above. It answers
-- "what is stranded on which node", which is both the operator's view and the
-- measurement that decides when these two columns can go.
--
-- 🔴 deleted_at_ms IS NULL is in the predicate for the second of those jobs.
-- The rule that retires the origin block is "count(*) of unpublished rows
-- stayed at zero for thirty days"; an index that keeps soft-deleted rows makes
-- that count include snapshots the user deleted months ago, so it never reaches
-- zero and the columns are never dropped. The two indexes above it both filter
-- the same way, and there is no query that wants a stranded row that no longer
-- exists.
CREATE INDEX IF NOT EXISTS snapshots_unpublished_idx
    ON snapshots (cluster_id, origin_node_id)
    WHERE NOT published AND deleted_at_ms IS NULL;

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
-- 🔴 The template id points at `snapshots`, not at `templates`, and
-- deliberately: the row that a build belongs to is the snapshots row, which is
-- created before anything writes to `templates`.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS builds (
    id              UUID   PRIMARY KEY,
    template_id     UUID   NOT NULL,
    cluster_id      UUID   NOT NULL,

    status          TEXT   NOT NULL
                           CHECK (status IN ('waiting', 'building', 'ready', 'error')),
    status_group    TEXT   NOT NULL
                           CHECK (status_group IN ('pending', 'in_progress', 'ready', 'failed')),

    -- The process that administers this build's lease: it is the one that
    -- called StartBuild, and it is the only one allowed to RenewBuildLease
    -- (see `renew_build_lease`'s `node_id = $2`) or let the row go stale.
    --
    -- 🔴 Not necessarily the machine that runs the build sandbox. When a
    -- worker node builds its own template (the historical, still-common
    -- case) the two are the same node and this column has always answered
    -- both questions at once. Since template builds can be dispatched to a
    -- node from an API replica (AgentENV's node_client/build.rs), the two can
    -- differ: the API replica is what admits the build and heartbeats it —
    -- because it is the process running the loop that must stop the build if
    -- the lease is lost — while a different node is what actually runs
    -- Firecracker. Changing this column to record the executor instead would
    -- break the heartbeat: RenewBuildLease is sent by the same process that
    -- called StartBuild, so the two would stop matching the first time an
    -- admitting replica differs from the node it dispatched to, and the
    -- reaper would free a build that is still legitimately running. "Which
    -- machine is actually running this build" is answered by the admitting
    -- process's own dispatch log (info-level, `node_client/build.rs`: logged
    -- when the build is sent to a node and again when that node returns the
    -- staged result) rather than by this column.
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
-- 🔴 It is also what makes heartbeat_at_ms mandatory. See the column, and
-- `reaper.rs`, which is the something that ends a stranded build.
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
-- 🔴 ON DELETE CASCADE is deliberate and differs from the object-store
-- behaviour, where a delete only removes an alias that still happens to point
-- at the id being removed. That defensiveness existed because there was no
-- foreign key to lean on. With one, "an alias pointing at a row that is not
-- there" stops being a state, and a stale-alias sweep on the read path can only
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
