-- disk_size_mib moves from "every row" to "every row somebody can launch".
--
-- 🔴 0 is not a missing value in this column, it is *the* value for a snapshot
-- whose disk size is not known yet, and that convention comes from the node,
-- not from here. A v3 template genuinely has no disk size at create time: the
-- E2B-compatible TemplateBuildRequestV3 carries name, tags, cpuCount and
-- memoryMB and has no disk field at all, so there is nothing a node could send
-- instead. The real figure is the produced rootfs's virtual size and it exists
-- only once a build has produced a rootfs — src/template/builder.rs fills
-- resources.disk_size_mib from build_execution.manifest.rootfs.virtual_size,
-- and src/template/build_spec.rs states the convention in as many words:
-- `disk_size_mib: 0, // disk size is determined after build.`. The other end
-- reads it the same way round — src/sandbox/firecracker/factory.rs takes 0 as
-- "no explicit rootfs size, use the image's natural size".
--
-- 0001 justified the column CHECKs as "the same ones a launch would apply
-- later, applied where a bad row cannot be written in the first place". That
-- reasoning is right and is kept; what was wrong is *where* it was applied. A
-- launch happens to a `ready` row. A `waiting` or `building` row is not one and
-- can never become one by accident: every resolving query filters on
-- status_group = 'ready' (that predicate is what snapshots_list_idx is built
-- on), so a row whose disk size is still 0 is invisible to everything that
-- could start it. Applied at insert time the CHECK protected no launch — it
-- refused the insert, and so refused every v3 template create on the cluster
-- with InvalidArgument before a build could ever produce the number it wanted.
--
-- So the rule is gated on `ready`, exactly the way the payload rule beside it
-- already is (snapshots_ready_is_committed). Both then hold at the same moment
-- and have the same single writer: commit_snapshot is the only statement in
-- this package that produces a `ready` row.
--
-- 🔴 cpu_count and memory_mib keep their column CHECKs and are not touched.
-- Both are known at create — the create request carries them, or the node's
-- config default fills them in — so a 0 in either is a real error, and the
-- earliest possible refusal is the right one for a real error.
--
-- 🔴 A plain ADD CONSTRAINT rather than the ADD NOT VALID / VALIDATE dance,
-- deliberately. The CHECK being dropped here is strictly stronger than both
-- rules replacing it, so every row already in the table satisfies them and the
-- validating scan cannot fail; and the applier runs this file inside one
-- transaction anyway, so splitting the step would not shorten a lock, only
-- make the file look as though it did.

-- The name PostgreSQL generated for the column CHECK in 0001. Column-level
-- CHECKs are named <table>_<column>_check, which was confirmed against
-- postgres:16-alpine by applying 0001 and reading pg_constraint rather than
-- assumed. IF EXISTS so the file stays re-runnable.
ALTER TABLE snapshots DROP CONSTRAINT IF EXISTS snapshots_disk_size_mib_check;

-- 🔴 And the sweep below, for the one case a fixed name cannot cover: the
-- generated name is only that name while it is free. Had it been taken, the
-- server would have appended a digit — snapshots_disk_size_mib_check1 — and the
-- DROP above would have succeeded having done nothing, leaving the very
-- constraint this file exists to remove behind a green migration. Anything left
-- on this table that mentions disk_size_mib and is not one of the two rules
-- added below is that constraint under another name.
DO $$
DECLARE
    leftover text;
BEGIN
    FOR leftover IN
        SELECT c.conname
          FROM pg_constraint c
         WHERE c.conrelid = 'snapshots'::regclass
           AND c.contype = 'c'
           AND c.conname NOT IN ('snapshots_disk_size_floor', 'snapshots_ready_has_disk_size')
           AND pg_get_constraintdef(c.oid) LIKE '%disk_size_mib%'
    LOOP
        EXECUTE format('ALTER TABLE snapshots DROP CONSTRAINT %I', leftover);
    END LOOP;
END
$$;

-- The floor. The column is INTEGER, which is signed, and the store casts a
-- uint32 to int32 on the way in — so "not known yet" is 0 and everything below
-- it is a value that arrived corrupted rather than one that means anything.
-- Dropping the positivity CHECK without this would leave the column able to
-- hold a negative size for the first time since the table was created.
ALTER TABLE snapshots DROP CONSTRAINT IF EXISTS snapshots_disk_size_floor;
ALTER TABLE snapshots
    ADD CONSTRAINT snapshots_disk_size_floor
        CHECK (disk_size_mib >= 0);

-- The launch rule, at the point where a launch becomes possible.
ALTER TABLE snapshots DROP CONSTRAINT IF EXISTS snapshots_ready_has_disk_size;
ALTER TABLE snapshots
    ADD CONSTRAINT snapshots_ready_has_disk_size
        CHECK (status <> 'ready' OR disk_size_mib > 0);
