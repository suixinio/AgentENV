package catalog

// This file holds the writes, and the two reads that must *not* carry the ready
// predicate.
//
// 🔴 The separation from queries_resolved.go is the point of having two files.
// `get_build` exists precisely to look at a build that is running or has
// failed, and the reaper exists to act on rows no resolving query can see;
// adding `status_group = 'ready'` to either would make the build-status
// endpoint answer "no such build" for every build that has not finished, which
// is every build a caller is actually asking about.

// nowMs is the database's own clock, in the milliseconds this schema stores.
//
// 🔴 Not a convenience. The build heartbeat is the one axis in this file where
// two clocks are compared against each other: a builder says it is alive and,
// some minutes later, the reaper decides it is not. If the "alive" end is
// stamped by the node and the "not alive" end by whoever runs the reaping pass,
// then the difference between them is the difference between two machines'
// clocks as much as it is elapsed time — and a node running a few minutes slow
// has every one of its builds reaped while they are still running, repeatedly,
// with no error anywhere to say why. The registry made this exact move for the
// same reason and wrote it down; see beginPauseSQL's note on paused_at.
//
// clock_timestamp() rather than now(): now() is the transaction's start time,
// and a reaping pass that opened its transaction before a heartbeat landed
// would judge that heartbeat against an instant that predates it.
const nowMs = `(EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT`

// ─────────────────────────────────────────────────────────────────────────────
// Transaction A — the row before the bytes
// ─────────────────────────────────────────────────────────────────────────────

// insertSnapshotSQL opens a catalog row.
//
// status_group is supplied and immediately overwritten: the trigger derives it
// from `status` on every insert and update, and it is written here only so the
// NOT NULL column has a value on the way in. Nothing reads what this statement
// puts there.
//
// created_at_ms is written to updated_at_ms as well. The updated_at trigger
// stamps the server clock only when a caller leaves the value alone, so a row
// and its object-store mirror start out carrying the same instant.
const insertSnapshotSQL = `
INSERT INTO snapshots (
    id, cluster_id, source_kind, source_sandbox_id,
    cpu_count, memory_mib, disk_size_mib,
    status, status_group,
    published, origin_node_id,
    sandbox_started_at_ms, created_at_ms, updated_at_ms,
    publishing_execution_id
) VALUES (
    $1::uuid, $2::uuid, $3, $4,
    $5, $6, $7,
    $8, 'pending',
    $9, $10,
    $11, $12, $12,
    $13::uuid
)
ON CONFLICT (id) DO NOTHING`

// insertTemplateSQL records the template-only half of a template row.
//
// 🔴 `snapshots.deleted_at_ms` is the authoritative soft delete and this table's
// copy follows it. Two flags for one entity can disagree, and a reader
// consulting the wrong one shows a deleted template; the listing index already
// filters on the snapshots one, so that is the one that decides. Every writer
// of either must write both, in one transaction — which is why the delete below
// does.
const insertTemplateSQL = `
INSERT INTO templates (id, cluster_id, created_at_ms, updated_at_ms)
VALUES ($1::uuid, $2::uuid, $3, $3)
ON CONFLICT (id) DO NOTHING`

// releaseOtherAliasesSQL drops any alias this snapshot holds under a different
// name, so binding a new one is a rename rather than a second row.
//
// 🔴 `aliases_one_per_snapshot` refuses a second alias for one snapshot, and
// that refusal is *not* the alias conflict the API reports — that one is the
// primary key, and it means somebody else holds the name. Reporting one as the
// other would tell a user their own rename collided with a stranger. Removing
// the old row first means the index can only ever fire on a genuine race.
const releaseOtherAliasesSQL = `
DELETE FROM aliases
 WHERE cluster_id = $1::uuid AND snapshot_id = $2::uuid AND alias <> $3`

// bindAliasSQL claims a name, or declines to.
//
// One statement replaces the 93 lines of read-modify-write-reread in the
// object-store backend that describe themselves as "weaker than a true CAS".
// A zero-row result means the name is held; who holds it is a separate read,
// because the answer decides whether this is idempotence or a conflict.
const bindAliasSQL = `
INSERT INTO aliases (cluster_id, alias, snapshot_id, created_at_ms)
VALUES ($1::uuid, $2, $3::uuid, $4)
ON CONFLICT (cluster_id, alias) DO NOTHING`

const aliasHolderSQL = `
SELECT snapshot_id::text FROM aliases WHERE cluster_id = $1::uuid AND alias = $2`

// ─────────────────────────────────────────────────────────────────────────────
// Transaction B — the flip
// ─────────────────────────────────────────────────────────────────────────────

// commitSnapshotSQL is the only statement in this package that produces a
// `ready` row.
//
// 🔴 `AND status = 'building'` is load-bearing, not defensive. It is where
// execution fencing lands on the catalog side: a commit from an incarnation the
// cluster has already replaced finds a row that was reopened under the newer
// one and matches nothing. It is also what makes a crash between the bytes and
// this statement harmless — the row stays `in_progress`, and every resolving
// query already refuses to see it, which is the same trick e2b uses to leave
// its orphans lying where they fall instead of cleaning them up.
//
// origin_node_id is COALESCEd rather than overwritten so a commit that names no
// node keeps the hint the begin recorded. Clearing it is not an operation any
// caller has: an unpublished row must name its origin, and a published one's
// hint is worth more than a blank.
//
// publishing_execution_id is written and not compared. The predicate that will
// compare it belongs to the phase with several writers; the column is here now
// so that adding it then is not a backfill onto rows already in flight.
const commitSnapshotSQL = `
UPDATE snapshots
   SET status                  = 'ready',
       committed_payload       = $3,
       committed_schema        = $4,
       published               = $5,
       origin_node_id          = COALESCE($6, origin_node_id),
       updated_at_ms           = $7,
       cpu_count               = COALESCE($8::integer, cpu_count),
       memory_mib              = COALESCE($9::integer, memory_mib),
       disk_size_mib           = COALESCE($10::integer, disk_size_mib),
       publishing_execution_id = COALESCE($11::uuid, publishing_execution_id)
 WHERE id = $1::uuid
   AND cluster_id = $2::uuid
   AND deleted_at_ms IS NULL
   AND status = 'building'`

// ─────────────────────────────────────────────────────────────────────────────
// Transaction C — the failure
// ─────────────────────────────────────────────────────────────────────────────

// failSnapshotSQL moves a row to `error`.
//
// 🔴 `status <> 'ready'` and not an exact predicate, so that a caller retrying
// after a lost response succeeds rather than being told the row moved. What it
// refuses is the one transition that would be a lie: a committed snapshot,
// which has a payload and can be started, being recorded as a failure.
//
// 🔴 This is not where a failed *publish* goes. A publish that produced bytes
// the node still holds ends `ready` with published=false — the snapshot is
// complete and its origin can start it — and saying otherwise would tell a user
// a snapshot they can still resume does not exist.
const failSnapshotSQL = `
UPDATE snapshots
   SET status        = 'error',
       build_error   = $3::jsonb,
       updated_at_ms = $4
 WHERE id = $1::uuid
   AND cluster_id = $2::uuid
   AND deleted_at_ms IS NULL
   AND status <> 'ready'`

// failActiveBuildSQL ends whatever build was holding this template.
//
// Without it a failed build leaves a `pending`/`in_progress` row that
// `builds_one_active_per_template` turns into a template nobody can build
// again — the same trap the reaper exists for, reached by a different road.
// endActiveBuildOfDeletedTemplateSQL ends whatever build a deleted template was
// holding.
//
// 🔴 `builds_template_fk` cascades on a *hard* delete, and a snapshot delete
// here is soft — so a template deleted mid-build left its `builds` row in
// `pending`/`in_progress`, where `countActiveBuildsSQL` goes on counting it
// against the cluster ceiling and `builds_one_active_per_template` goes on
// holding a template that no longer exists. Nothing ever released it but the
// heartbeat reaper, a TTL later, and only because the builder had stopped
// renewing; a builder still running would hold the slot indefinitely.
//
// Separate from failActiveBuildSQL because the reason differs and the reason is
// what an operator reads. The template is gone, which is not a build failure.
const endActiveBuildOfDeletedTemplateSQL = `
UPDATE builds
   SET status         = 'error',
       finished_at_ms = $3,
       error_reason   = $4::jsonb
 WHERE template_id = $1::uuid
   AND cluster_id = $2::uuid
   AND status_group IN ('pending', 'in_progress')
RETURNING id::text`

// The reason endActiveBuildOfDeletedTemplateSQL writes.
const deletedTemplateBuildError = `{"message":"the template was deleted while this build was running","step":null}`

const failActiveBuildSQL = `
UPDATE builds
   SET status         = 'error',
       finished_at_ms = $3,
       error_reason   = $4::jsonb
 WHERE template_id = $1::uuid
   AND cluster_id = $2::uuid
   AND status_group IN ('pending', 'in_progress')
RETURNING id::text`

// finishActiveBuildSQL takes the build off the queue when its snapshot commits.
//
// 🔴 The other half of admission, and leaving it out is not a leak but an
// outage on a timer. `builds_one_active_per_template` and the cluster ceiling
// both count `pending`/`in_progress` rows, and nothing else moves a build out
// of that group on the success path — so every build that *worked* would go on
// occupying a slot for ever, and the twenty-first build in the cluster's life
// would be refused with the queue full and stay refused.
//
// Unconditional rather than a flag the caller sets, unlike failActiveBuildSQL.
// There is no commit that should leave a build in flight, so a caller able to
// say "leave it" is only a caller able to forget; and the statement costs one
// index probe on a commit that has no build, which every pause is.
const finishActiveBuildSQL = `
UPDATE builds
   SET status         = 'ready',
       finished_at_ms = $3,
       error_reason   = NULL
 WHERE template_id = $1::uuid
   AND cluster_id = $2::uuid
   AND status_group IN ('pending', 'in_progress')
RETURNING id::text`

// ─────────────────────────────────────────────────────────────────────────────
// Delete
// ─────────────────────────────────────────────────────────────────────────────

// softDeleteSnapshotSQL retires a row without losing it.
//
// A hard delete would take the alias with it and, through it, possibly an image
// reference somebody exported to a registry — with nothing left to trace it
// back to. The row stays; only the readers stop seeing it.
const softDeleteSnapshotSQL = `
UPDATE snapshots
   SET deleted_at_ms = $3, updated_at_ms = $3
 WHERE id = $1::uuid AND cluster_id = $2::uuid AND deleted_at_ms IS NULL`

// dropAliasesOfSnapshotSQL frees the name a deleted snapshot was holding.
//
// 🔴 The foreign key's ON DELETE CASCADE does not do this: cascade fires on a
// hard delete and this one is soft. Leaving the row would keep a name reserved
// by a snapshot no read can reach, and nothing would ever release it.
const dropAliasesOfSnapshotSQL = `
DELETE FROM aliases WHERE cluster_id = $1::uuid AND snapshot_id = $2::uuid`

const softDeleteTemplateSQL = `
UPDATE templates
   SET deleted_at_ms = $3, updated_at_ms = $3
 WHERE id = $1::uuid AND cluster_id = $2::uuid AND deleted_at_ms IS NULL`

// ─────────────────────────────────────────────────────────────────────────────
// Build admission
// ─────────────────────────────────────────────────────────────────────────────

// buildAdmissionKey is the advisory lock every admission takes.
//
// 🔴 A lock rather than "insert, then count". Under READ COMMITTED two
// concurrent admissions cannot see each other's uncommitted rows, so counting
// after inserting lets both through and the cap is not a cap. Counting under a
// transaction-scoped advisory lock is the cheap way to make the count mean
// something; at our volumes the serialisation costs nothing worth measuring.
//
// One constant, not one per cluster: two clusters sharing a database would
// serialise against each other, which is conservative rather than wrong, and a
// key derived from the cluster id would be a hash collision away from two
// clusters silently sharing one budget.
const buildAdmissionKey int64 = 3405691582

const takeBuildAdmissionLockSQL = `SELECT pg_advisory_xact_lock($1)`

// countActiveBuildsSQL is the cluster-wide ceiling's evidence. It reads through
// `builds_active_idx`, which is partial on the same predicate, so its cost
// tracks what is running rather than what has ever run.
const countActiveBuildsSQL = `
SELECT count(*) FROM builds
 WHERE cluster_id = $1::uuid AND status_group IN ('pending', 'in_progress')`

// activeBuildForTemplateSQL names the build already holding a template.
//
// Read before inserting rather than after catching the unique violation: an
// aborted transaction cannot be queried, so learning *which* build holds the
// template afterwards would need a savepoint around every insert. The unique
// index remains the enforcement — this read only makes the refusal informative.
const activeBuildForTemplateSQL = `
SELECT id::text FROM builds
 WHERE cluster_id = $1::uuid AND template_id = $2::uuid
   AND status_group IN ('pending', 'in_progress')
 LIMIT 1`

// markSnapshotBuildingSQL moves the template row under the new build.
//
// 🔴 `status IN ('waiting', 'error')` and deliberately both. A template nobody
// has built yet is `waiting`; one whose last build failed — or was reaped — is
// `error`, and a retry of that is the ordinary case rather than an exception.
// `building` is refused because that is the exclusion this whole mechanism
// exists for, and `ready` because a published template is rebuilt under a new
// id rather than in place.
const markSnapshotBuildingSQL = `
UPDATE snapshots
   SET status = 'building', updated_at_ms = $3, build_error = NULL
 WHERE id = $1::uuid
   AND cluster_id = $2::uuid
   AND deleted_at_ms IS NULL
   AND status IN ('waiting', 'error')`

// insertBuildSQL admits the build.
//
// heartbeat_at_ms is stamped by this statement and not by a later one: the
// reaper only acts on rows carrying a heartbeat, so a build admitted without
// one is the single row nothing can ever clean up — and it would hold the
// template forever behind the partial unique index. Stamped here, that row
// cannot be written at all rather than being refused by a check somebody could
// remove.
//
// 🔴 The stamp is the database's, not the admitting node's. See nowMs.
const insertBuildSQL = `
INSERT INTO builds (
    id, template_id, cluster_id,
    status, status_group,
    node_id, heartbeat_at_ms,
    created_at_ms, started_at_ms
) VALUES (
    $1::uuid, $2::uuid, $3::uuid,
    'building', 'in_progress',
    $4, ` + nowMs + `,
    $5, $5
)`

// renewBuildLeaseSQL is one heartbeat.
//
// The node id is in the predicate: a process that is not the one running this
// build must not be able to keep it alive, which is what would happen after the
// reaper freed the template and somebody else took it.
//
// 🔴 What is recorded is when this process heard from the builder, not when the
// builder says it spoke. See nowMs: the reaper compares this value against a
// clock, and it has to be the same one.
const renewBuildLeaseSQL = `
UPDATE builds
   SET heartbeat_at_ms = ` + nowMs + `
 WHERE id = $3::uuid
   AND cluster_id = $1::uuid
   AND node_id = $2
   AND status_group IN ('pending', 'in_progress')`

// getBuildSQL reads one build row.
//
// 🔴 No ready predicate, on purpose — see the note at the top of this file.
const getBuildSQL = `
SELECT id::text, template_id::text, cluster_id::text,
       status, status_group, node_id,
       heartbeat_at_ms, created_at_ms, started_at_ms, finished_at_ms,
       error_reason
  FROM builds
 WHERE cluster_id = $1::uuid AND id = $2::uuid`

// ─────────────────────────────────────────────────────────────────────────────
// The reaper
// ─────────────────────────────────────────────────────────────────────────────

// reapBuildsSQL ends builds whose builder stopped saying it was alive.
//
// 🔴 `heartbeat_at_ms IS NOT NULL` is not a tidiness check. A row without a
// heartbeat has no evidence of being stale and is left alone forever, which is
// why the admitting statement stamps one.
//
// 🔴 No ready predicate here either, for the obvious reason: everything this
// touches is by definition not ready.
//
// 🔴 Both ends of the comparison come from the database. $2 is a duration, not
// an instant, and there is deliberately no way for a caller to supply "now":
// this is the statement where a clock read from the wrong machine ends work
// that is still running. See nowMs.
const reapBuildsSQL = `
UPDATE builds
   SET status         = 'error',
       finished_at_ms = ` + nowMs + `,
       error_reason   = $3::jsonb
 WHERE cluster_id = $1::uuid
   AND status_group IN ('pending', 'in_progress')
   AND heartbeat_at_ms IS NOT NULL
   AND heartbeat_at_ms < ` + nowMs + ` - $2
RETURNING id::text, template_id::text, node_id`

// failReapedTemplatesSQL frees the template rows those builds were holding.
//
// 🔴 The half that is easy to leave out, and leaving it out defeats the other
// half. `builds_one_active_per_template` stops blocking the template the moment
// the build row leaves the active group — but `markSnapshotBuildingSQL` refuses
// a template still sitting at `building`, so the template stays unbuildable
// through a different door. Both, or neither.
const failReapedTemplatesSQL = `
UPDATE snapshots
   SET status        = 'error',
       build_error   = $2::jsonb,
       updated_at_ms = ` + nowMs + `
 WHERE cluster_id = $1::uuid
   AND id = ANY($3::uuid[])
   AND status = 'building'`

// reapedBuildError is the reason recorded on both halves.
//
// A JSON object, not a bare string: `snapshots.build_error` is a
// TemplateBuildErrorReason and the node's decoder for it is the authority on
// what that means. This side writes the shape and never reads it back.
const reapedBuildError = `{"message":"build heartbeat lapsed","step":null}`

// ─────────────────────────────────────────────────────────────────────────────
// Classification
// ─────────────────────────────────────────────────────────────────────────────

// observedSnapshotStatusSQL says what a row that refused a fenced write carries
// now.
//
// It ignores deleted_at_ms in the predicate and reads it as a value instead: a
// caller whose write missed because the row was deleted needs to be told
// "gone", not "no such row ever", and those are the same answer only if you do
// not have to decide what to do next.
const observedSnapshotStatusSQL = `
SELECT status, deleted_at_ms FROM snapshots WHERE id = $1::uuid AND cluster_id = $2::uuid`
