package catalog

import (
	"fmt"
	"strings"
)

// This file holds the reads that resolve a snapshot — the ones a caller is
// about to run, list or name.
//
// 🔴 Every one of them carries the ready predicate when the caller asks for it,
// and the caller always asks for it on the paths that end in a running VM.
// There are four such reads and this is all of them: get one, list a page,
// resolve an alias, and the get that a resume performs. The statements that
// must *not* carry it — the build-status read and the reaper — live in
// queries_admin.go, apart, so that neither set can be edited by accident while
// looking at the other.
//
// 🔴 The predicate is what stops a snapshot whose bytes are still uploading
// from being handed to a VM. `snapshots_list_idx` carries the same predicate in
// its own WHERE, so a query that loses it also loses the index and shows up as
// a timing regression rather than as silence — a guardrail with feedback, which
// is the only kind that survives a year.

// readyPredicate is the whole of the rule, in one place.
const readyPredicate = `status_group = 'ready'`

// snapshotColumns is every column a SnapshotRow carries, in scan order.
//
// The uuid columns are cast to text so the decoder does not depend on which
// uuid codec the driver happens to have registered, and so that an id compared
// against a cursor is compared in the same form the cursor holds it.
const snapshotColumns = `s.id::text                AS id,
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
       s.sandbox_started_at_ms   AS sandbox_started_at_ms,
       s.committed_payload       AS committed_payload,
       s.committed_schema        AS committed_schema,
       s.build_error             AS build_error,
       s.published               AS published,
       s.origin_node_id          AS origin_node_id`

// aliasJoin projects the at-most-one alias that names this snapshot.
//
// It is a join and not a subquery because `aliases_one_per_snapshot` makes it
// safe to be one: without that index a second alias row would return this
// snapshot twice inside a single page, and a page of ten would hold nine
// snapshots while every "no duplicates" assertion still passed.
const aliasJoin = `LEFT JOIN aliases a
    ON a.snapshot_id = s.id AND a.cluster_id = s.cluster_id`

// buildJoin projects the newest build's two timestamps.
//
// LATERAL rather than a plain join: a template accumulates build rows over
// time and a plain join would multiply the snapshot row by all of them. Only
// the newest is a fact about the template's current state.
const buildJoin = `LEFT JOIN LATERAL (
    SELECT b.started_at_ms, b.finished_at_ms
      FROM builds b
     WHERE b.template_id = s.id AND b.cluster_id = s.cluster_id
     ORDER BY b.created_at_ms DESC, b.id DESC
     LIMIT 1
) bb ON TRUE`

// buildColumns and noBuildColumns keep one scan shape for both reads.
//
// Selecting typed NULLs when the caller did not ask for the build is cheaper to
// reason about than two decoders that must be kept in step, and a decoder that
// drifts from its query is the failure this avoids.
const buildColumns = `bb.started_at_ms   AS build_started_at_ms,
       bb.finished_at_ms  AS build_finished_at_ms`

const noBuildColumns = `NULL::bigint AS build_started_at_ms,
       NULL::bigint AS build_finished_at_ms`

// ─────────────────────────────────────────────────────────────────────────────
// One row
// ─────────────────────────────────────────────────────────────────────────────

// selectSnapshotSQL builds the single-row read.
//
// by is the predicate selecting the row: an id or an alias. Both forms are
// needed because the caller holds a union it cannot itself resolve — the node's
// own get() takes the same union — and an alias is allowed to look exactly like
// a uuid, so the shape of the string is a hint rather than an answer. The store
// tries the id first and falls back to the alias, which is the only order that
// leaves no value unreachable.
func selectSnapshotSQL(by string, opts ReadOptions) string {
	var sb strings.Builder
	sb.WriteString("SELECT ")
	sb.WriteString(snapshotColumns)
	sb.WriteString(",\n       ")
	if opts.WithBuild {
		sb.WriteString(buildColumns)
	} else {
		sb.WriteString(noBuildColumns)
	}
	sb.WriteString("\n  FROM snapshots s\n  ")
	sb.WriteString(aliasJoin)
	if opts.WithBuild {
		sb.WriteString("\n  ")
		sb.WriteString(buildJoin)
	}
	sb.WriteString("\n WHERE s.cluster_id = $1::uuid\n   AND s.deleted_at_ms IS NULL\n   AND ")
	sb.WriteString(by)
	if opts.OnlyReady {
		sb.WriteString("\n   AND s.")
		sb.WriteString(readyPredicate)
	}
	return sb.String()
}

// byIDPredicate and byAliasPredicate are the two ways to name one row. $2 is
// the value in both.
const (
	byIDPredicate    = `s.id = $2::uuid`
	byAliasPredicate = `a.alias = $2`
)

// resolveAliasSQL answers which snapshot an alias names.
//
// 🔴 published and origin_node_id are in the SELECT list and nowhere else. An
// alias pointing at a snapshot that failed to publish resolves normally — the
// snapshot is complete and its origin can start it — and where it may be
// started is decided afterwards, by PinOriginIfUnpublished, from these two
// projected values. Adding either to this WHERE would report "no such alias"
// about a snapshot the user can still resume.
func resolveAliasSQL(onlyReady bool) string {
	sql := `SELECT s.id::text, s.published, s.origin_node_id
  FROM aliases a
  JOIN snapshots s ON s.id = a.snapshot_id AND s.cluster_id = a.cluster_id
 WHERE a.cluster_id = $1::uuid
   AND a.alias = $2
   AND s.deleted_at_ms IS NULL`
	if onlyReady {
		sql += "\n   AND s." + readyPredicate
	}
	return sql
}

// ─────────────────────────────────────────────────────────────────────────────
// One page
// ─────────────────────────────────────────────────────────────────────────────

const (
	// defaultListLimit is what a caller asking for nothing gets.
	//
	// 🔴 A request for no limit is not a request for every row. The HTTP layer
	// on the node treats a missing limit as "all of them", which is how one
	// request comes to pull ten thousand rows into memory; the same default
	// here would move that fault rather than fix it.
	defaultListLimit uint32 = 100
	// maxListLimit caps what a caller may ask for.
	maxListLimit uint32 = 1000
)

// clampLimit resolves a requested page size.
func clampLimit(requested uint32) uint32 {
	if requested == 0 {
		return defaultListLimit
	}
	if requested > maxListLimit {
		return maxListLimit
	}
	return requested
}

// args accumulates bound parameters and hands back their placeholders, so a
// filter that is present and one that is absent cannot renumber each other.
type args struct{ values []any }

func (a *args) add(v any) string {
	a.values = append(a.values, v)
	return fmt.Sprintf("$%d", len(a.values))
}

// listSnapshotsSQL builds one keyset page.
//
// Three things here are load-bearing and each fails silently when it is wrong:
//
//  1. 🔴 The ORDER BY is spelled the same way round as `snapshots_list_idx`'s
//     key, `(created_at_ms DESC, id)`. Disagree with it and the query sorts the
//     table instead of walking the index — no error, just a listing whose cost
//     grows with the catalog, which is the exact property this move exists to
//     remove.
//
//  2. 🔴 The keyset comparison swaps its operands: `(created_at_ms, cursor_id)
//     < (cursor_ms, id)` is `created_at_ms < cursor_ms OR (created_at_ms =
//     cursor_ms AND id > cursor_id)` written as one comparison. The two halves
//     sort in opposite directions, and spelled out as an OR the planner treats
//     them as two unrelated branches.
//
//  3. 🔴 Both ids are compared as text, because the public cursor orders by the
//     id's string form. For canonical lower-case uuids that order and the
//     column's own byte order agree — which is why the ORDER BY above may use
//     the column and still match this predicate — but the agreement is a
//     property of the ids being canonical, not a licence to compare whichever
//     is convenient. An id in any other form skips rows at a page boundary
//     without raising anything.
//
// The limit is bound as limit+1: reading one row past the page is what says
// whether there is another one. Comparing the page size to the limit instead
// makes every listing whose total is a multiple of the limit end a page early
// and silently.
func listSnapshotsSQL(in ListInput, limit uint32) (string, []any, error) {
	a := &args{}
	cluster := a.add(in.ClusterID)

	var sb strings.Builder
	sb.WriteString("SELECT ")
	sb.WriteString(snapshotColumns)
	sb.WriteString(",\n       ")
	if in.WithBuild {
		sb.WriteString(buildColumns)
	} else {
		sb.WriteString(noBuildColumns)
	}
	sb.WriteString("\n  FROM snapshots s\n  ")
	sb.WriteString(aliasJoin)
	if in.WithBuild {
		sb.WriteString("\n  ")
		sb.WriteString(buildJoin)
	}
	sb.WriteString("\n WHERE s.cluster_id = " + cluster + "::uuid")
	sb.WriteString("\n   AND s.deleted_at_ms IS NULL")
	if in.OnlyReady {
		sb.WriteString("\n   AND s." + readyPredicate)
	}

	if err := appendFilters(&sb, a, in.Filter); err != nil {
		return "", nil, err
	}

	if in.Cursor != nil {
		id, err := requireUUID("cursor snapshot_id", in.Cursor.SnapshotID)
		if err != nil {
			return "", nil, err
		}
		cursorID := a.add(id)
		cursorMs := a.add(in.Cursor.CreatedAtMs)
		sb.WriteString("\n   AND (s.created_at_ms, " + cursorID + "::text) < (" + cursorMs + "::bigint, s.id::text)")
	}

	sb.WriteString("\n ORDER BY s.created_at_ms DESC, s.id ASC")
	sb.WriteString("\n LIMIT " + a.add(int64(limit)+1))

	return sb.String(), a.values, nil
}

// appendFilters writes the conjunction the caller asked for.
//
// 🔴 There is no branch here for `published` or `origin_node_id`, and there
// must never be one. Filtering a listing on them would tell a user a snapshot
// does not exist when what is true is that it can only be started on one
// machine — and it would turn dropping those two columns from one migration
// file into an edit of every query in this package. See the schema's rule V5.
func appendFilters(sb *strings.Builder, a *args, f Filter) error {
	if kinds := trimmedNonEmpty(f.SourceKinds); len(kinds) > 0 {
		for _, kind := range kinds {
			if !knownSourceKind(kind) {
				return fmt.Errorf("%w: source kind %q is not one this table has", ErrInvalidArgument, kind)
			}
		}
		if len(kinds) == 1 {
			// Equality rather than ANY when there is one, so the listing index
			// — whose second key column is source_kind — can supply the order
			// as well as the filter.
			sb.WriteString("\n   AND s.source_kind = " + a.add(kinds[0]))
		} else {
			sb.WriteString("\n   AND s.source_kind = ANY(" + a.add(kinds) + ")")
		}
	}

	if f.AliasPrefix != nil {
		prefix := strings.TrimSpace(*f.AliasPrefix)
		if prefix != "" {
			// starts_with rather than LIKE: the caller's prefix is user input
			// and LIKE would read % and _ in it as wildcards.
			sb.WriteString("\n   AND starts_with(a.alias, " + a.add(prefix) + ")")
		}
	}

	if ids := trimmedNonEmpty(f.SnapshotIDs); len(ids) > 0 {
		validated := make([]string, 0, len(ids))
		for _, raw := range ids {
			id, err := requireUUID("snapshot_id", raw)
			if err != nil {
				return err
			}
			validated = append(validated, id)
		}
		sb.WriteString("\n   AND s.id = ANY(" + a.add(validated) + "::uuid[])")
	}

	if f.SnapshotIDOrAlias != nil {
		value := strings.TrimSpace(*f.SnapshotIDOrAlias)
		if value != "" {
			// A union, because the caller holds one and does not know which.
			// The id half only participates when the value could be one at all.
			if isCanonicalUUID(value) {
				placeholder := a.add(strings.ToLower(value))
				sb.WriteString("\n   AND (s.id = " + placeholder + "::uuid OR a.alias = " + placeholder + ")")
			} else {
				sb.WriteString("\n   AND a.alias = " + a.add(value))
			}
		}
	}

	if f.SourceSandboxID != nil {
		sandbox := strings.TrimSpace(*f.SourceSandboxID)
		if sandbox != "" {
			sb.WriteString("\n   AND s.source_sandbox_id = " + a.add(sandbox))
		}
	}

	if statuses := trimmedNonEmpty(f.TemplateStatuses); len(statuses) > 0 {
		for _, status := range statuses {
			if !knownStatus(status) {
				return fmt.Errorf("%w: status %q is not one this table has", ErrInvalidArgument, status)
			}
		}
		sb.WriteString("\n   AND s.status = ANY(" + a.add(statuses) + ")")
	}

	return nil
}

func trimmedNonEmpty(in []string) []string {
	out := make([]string, 0, len(in))
	for _, raw := range in {
		if v := strings.TrimSpace(raw); v != "" {
			out = append(out, v)
		}
	}
	return out
}
