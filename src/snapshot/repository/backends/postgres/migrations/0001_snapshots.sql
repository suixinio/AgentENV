-- The catalog's main table, and the only one the other three point at.
--
-- It holds what a SnapshotRecord is: identity, source, resources, lifecycle
-- state, and one opaque blob. The blob is a CommittedSnapshot exactly as the
-- node serialised it and nothing here reads it — every column beside it exists
-- so that a query can index, filter, order or fence without unpacking it.
--
-- 🔴 Time is BIGINT milliseconds, not timestamptz, throughout the catalog.
-- SnapshotRecord.created_at_unix_ms is an i64 of milliseconds and the public
-- pagination cursor is rendered from it; a round trip through timestamptz's
-- microseconds and back out as an RFC3339 string moves page boundaries. The
-- price is honest and paid on purpose: no now() defaults, no date functions,
-- and every operator query divides by 1000.

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
    -- u32; the CHECKs are the same ones a launch would apply later, applied
    -- where a bad row cannot be written in the first place.
    cpu_count               INTEGER NOT NULL CHECK (cpu_count     > 0),
    memory_mib              INTEGER NOT NULL CHECK (memory_mib    > 0),
    disk_size_mib           INTEGER NOT NULL CHECK (disk_size_mib > 0),

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
    --      Go function that decides pinning. Filtering on them would mean the
    --      drop has to edit every query — and, worse, would hide a snapshot the
    --      user can still resume on its origin node behind "no such snapshot".
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
    -- deserialises it, defines a Go type for it, or branches on its contents.
    -- The version travels beside it so the node can refuse an encoding it does
    -- not know; the pair is all-or-nothing.
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
    -- string form, and duplicating that logic in Go is how the two drift.
    build_error             JSONB   NULL,
    CONSTRAINT snapshots_error_axis
        CHECK (status <> 'error' OR build_error IS NOT NULL),

    -- 🔴 Written from the first day, checked from a later one. When more than
    -- one process may commit, the commit statement grows a predicate on this
    -- column so a superseded incarnation cannot flip a row that was reopened
    -- under a newer one. Adding the column then instead would mean backfilling
    -- it onto rows already in flight, which is the one moment it cannot be
    -- done correctly.
    publishing_execution_id UUID    NULL
);

-- status_group is derived, always, on both tables that carry it. Inlined rather
-- than delegated to a shared SQL function on purpose: a trigger body resolves
-- unqualified names through the *session's* search_path at execution time, so a
-- helper function would make the mapping depend on a setting the caller
-- controls.
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
-- 🔴 It does not simply overwrite. During the double-write phase the same
-- record exists here and in object storage, and the check that they agree is
-- per row, not per count. A server-side stamp would make every mirrored row
-- differ from its twin by the latency of one RPC, and the check would fail on
-- rows that are in fact identical.
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
-- 🔴 deleted_at_ms IS NULL is in the predicate for the second of those jobs, and
-- the spec this file was written from (§4.2) omits it — a mistake, not a
-- choice. The rule that retires the origin block is "count(*) of unpublished
-- rows stayed at zero for thirty days"; an index that keeps soft-deleted rows
-- makes that count include snapshots the user deleted months ago, so it never
-- reaches zero and the columns are never dropped. The two indexes above it both
-- filter the same way, and there is no query that wants a stranded row that no
-- longer exists.
CREATE INDEX IF NOT EXISTS snapshots_unpublished_idx
    ON snapshots (cluster_id, origin_node_id)
    WHERE NOT published AND deleted_at_ms IS NULL;
