-- ─────────────────────────────────────────────────────────────────────────────
-- One live pause row per sandbox.
--
-- A pause writes a snapshot row and a resume reads the newest ready one. With
-- nothing stopping a second live row, the "newest" part was the whole rule:
-- a delete had to walk every row of the sandbox, and a delete that failed
-- halfway left rows behind that bring the sandbox back as resumable. This file
-- turns the rule into a constraint the database holds.
--
-- `is_pause` exists because SQL cannot tell a pause from a checkpoint. Both are
-- source_kind='sandbox' rows of the same sandbox, and the only thing that
-- separates them is a key inside `committed_payload`, which nothing here is
-- allowed to interpret. A checkpoint is a template of a running sandbox and a
-- sandbox may have many; only pauses are unique per sandbox.
-- ─────────────────────────────────────────────────────────────────────────────

-- No default, deliberately: NULL on an INSERT, and an unchanged value on an
-- UPDATE, is the sentinel the trigger below reads as "the writer did not say".
-- NOT NULL is set after the backfill, so the trigger is what keeps the column
-- total; a deployment that drops the trigger fails such a write loudly instead
-- of recording a pause as something else.
ALTER TABLE snapshots
    ADD COLUMN IF NOT EXISTS is_pause BOOLEAN;

-- Stated as an implication, not an equivalence: a sandbox-source row may be a
-- checkpoint instead, and a template row can never be a pause.
ALTER TABLE snapshots
    DROP CONSTRAINT IF EXISTS snapshots_pause_axis;
ALTER TABLE snapshots
    ADD CONSTRAINT snapshots_pause_axis
        CHECK (NOT is_pause OR source_kind = 'sandbox');

-- ── reading the payload ──────────────────────────────────────────────────────
--
-- The payload is a CommittedSnapshot serialised by serde_json, and its
-- `paused_sandbox` field is skipped when absent, so the key is present exactly
-- on a pause. This function is the only way the schema and the read path look
-- inside the blob, and both look for that key's presence only.
--
-- Wrapped so that a payload this build cannot decode makes one row a non-pause
-- and one row unmatched, rather than raising: `convert_from` raises on invalid
-- UTF-8 and the ::jsonb cast raises on anything that is not JSON. Both are
-- unreachable for a payload this catalog wrote; neither is worth a permanently
-- unstartable deployment, nor a 500 on every list that filters on metadata, if
-- one turns out to be reachable. Kept, not dropped: `reads.rs` calls it.
--
-- The wrapping is what it costs. A plpgsql EXCEPTION block opens a
-- subtransaction, which a parallel worker cannot do, so the function is
-- PARALLEL UNSAFE and cannot be marked SAFE: a listing whose predicate calls it
-- gets no parallel plan, on top of one call and one subtransaction per row that
-- reaches it. `reads.rs` keeps the call behind the `is_pause` column for that
-- reason.
CREATE OR REPLACE FUNCTION catalog_try_jsonb(payload BYTEA)
RETURNS JSONB AS $$
BEGIN
    RETURN convert_from(payload, 'UTF8')::jsonb;
EXCEPTION WHEN others THEN
    RETURN NULL;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

UPDATE snapshots
   SET is_pause = (
           source_kind = 'sandbox'
           AND COALESCE(catalog_try_jsonb(committed_payload) ? 'paused_sandbox', false)
       )
 WHERE is_pause IS NULL;

ALTER TABLE snapshots
    ALTER COLUMN is_pause SET NOT NULL;

-- ── the writer that does not know the column ─────────────────────────────────
--
-- A rolling upgrade runs this migration from the first new replica while the
-- previous build is still serving, and that build still commits pauses. Its
-- INSERT of the building row and its UPDATE that makes the row ready both name
-- every column but this one, so the sentinel decides for it: a pause it commits
-- is a pause the unique index holds, the pause listing returns, a resume finds
-- and `delete_sandbox_pauses` reclaims.
--
-- The classification is spelled with built-ins only. A trigger body resolves
-- unqualified names through the session's search_path at execution time, so
-- calling a helper here would make the answer depend on a setting the writer
-- controls.
--
-- Retirement: this trigger exists for the mixed window and for nothing else.
-- Once no replica older than one-pause-per-sandbox can run again, it can be
-- dropped; `services/README.md` carries that as an operator step.
CREATE OR REPLACE FUNCTION catalog_snapshots_pause_axis_trg() RETURNS TRIGGER AS $$
BEGIN
    -- An UPDATE that leaves the payload alone cannot reach a different answer,
    -- and the column is total, so the stored one stands. This is what keeps
    -- `set_origin_node_id`, `retire_previous_pauses` and `delete_sandbox_pauses`
    -- off the parse below, and it is what confines the classification, and the
    -- refusal it can raise, to a write that brings a payload.
    IF TG_OP = 'UPDATE'
       AND NEW.committed_payload IS NOT DISTINCT FROM OLD.committed_payload THEN
        RETURN NEW;
    END IF;

    IF TG_OP = 'INSERT' THEN
        IF NEW.is_pause IS NOT NULL THEN
            RETURN NEW;
        END IF;
    ELSIF NEW.is_pause IS DISTINCT FROM OLD.is_pause THEN
        RETURN NEW;
    END IF;

    -- A template row is never a pause and its payload is not this axis's
    -- business. AND is not obliged to short-circuit, so the kind decides before
    -- the payload is touched.
    IF NEW.source_kind <> 'sandbox' THEN
        NEW.is_pause := false;
        RETURN NEW;
    END IF;

    BEGIN
        NEW.is_pause := COALESCE(
            convert_from(NEW.committed_payload, 'UTF8')::jsonb ? 'paused_sandbox',
            false
        );
    EXCEPTION WHEN others THEN
        -- Refusing the write is the only outcome that leaves nothing behind.
        -- `false` would hide the row from the unique index, the pause listing,
        -- the resume read and `delete_sandbox_pauses` at once, and no column
        -- value tells it apart from a checkpoint afterwards; NULL is not
        -- available at all, because the column is total and the reclaim would
        -- have to return rows `decode_row` cannot decode. The backfill above
        -- cannot do this -- a migration that raises is a deployment that cannot
        -- start -- but a trigger that raises costs one write, and the payload is
        -- written by the commit path of the build that sends it.
        RAISE EXCEPTION
            'snapshots.committed_payload of sandbox row % is not decodable JSON', NEW.id
            USING ERRCODE = 'data_exception', DETAIL = SQLERRM;
    END;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS snapshots_pause_axis_trg ON snapshots;
CREATE TRIGGER snapshots_pause_axis_trg
    BEFORE INSERT OR UPDATE ON snapshots
    FOR EACH ROW EXECUTE FUNCTION catalog_snapshots_pause_axis_trg();

-- ── de-duplication ───────────────────────────────────────────────────────────
--
-- Every database that ever paused a sandbox twice already violates the index
-- below, so this migration cannot refuse duplicates: refusing would brick the
-- upgrade of exactly the deployments the constraint is for. It resolves them
-- the way the read it replaces did — the newest ready row wins — and retires
-- the rest with the catalog's own soft delete, which is what a user-initiated
-- delete of those rows would have done. Their bytes are left alone: a pause's
-- memory layers stack on the layers of the pause before it, so the surviving
-- row still needs them, and the sandbox's own deletion is what reclaims the
-- chain.
--
-- Ordering matches `latest_paused_snapshot`: ready first, then newest
-- created_at_ms, then the smaller id, so this file and that read cannot pick
-- different survivors. One statement, so a retired row and its alias go
-- together or neither does.
WITH ranked AS (
    SELECT id,
           row_number() OVER (
               PARTITION BY cluster_id, source_sandbox_id
               ORDER BY (status_group = 'ready') DESC, created_at_ms DESC, id ASC
           ) AS rank
      FROM snapshots
     WHERE is_pause
       AND deleted_at_ms IS NULL
), superseded AS (
    UPDATE snapshots s
       SET deleted_at_ms = (EXTRACT(EPOCH FROM now()) * 1000)::BIGINT,
           updated_at_ms = (EXTRACT(EPOCH FROM now()) * 1000)::BIGINT
      FROM ranked
     WHERE s.id = ranked.id
       AND ranked.rank > 1
    RETURNING s.id AS id, s.cluster_id AS cluster_id
)
DELETE FROM aliases a
 USING superseded
 WHERE a.snapshot_id = superseded.id
   AND a.cluster_id = superseded.cluster_id;

-- ── the constraint ───────────────────────────────────────────────────────────
--
-- A pause commit now upserts against this: it retires the sandbox's previous
-- pause row in the same transaction that makes the new one ready, and two
-- concurrent pauses of one sandbox cannot both survive.
CREATE UNIQUE INDEX IF NOT EXISTS snapshots_one_pause_per_sandbox
    ON snapshots (cluster_id, source_sandbox_id)
    WHERE is_pause AND deleted_at_ms IS NULL;
