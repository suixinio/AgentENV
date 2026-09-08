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

ALTER TABLE snapshots
    ADD COLUMN IF NOT EXISTS is_pause BOOLEAN NOT NULL DEFAULT false;

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
CREATE OR REPLACE FUNCTION catalog_try_jsonb(payload BYTEA)
RETURNS JSONB AS $$
BEGIN
    RETURN convert_from(payload, 'UTF8')::jsonb;
EXCEPTION WHEN others THEN
    RETURN NULL;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

UPDATE snapshots
   SET is_pause = true
 WHERE source_kind = 'sandbox'
   AND COALESCE(catalog_try_jsonb(committed_payload) ? 'paused_sandbox', false);

-- ── de-duplication ───────────────────────────────────────────────────────────
--
-- 🔴 Every database that ever paused a sandbox twice already violates the index
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
