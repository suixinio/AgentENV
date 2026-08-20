package catalog

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"testing"
)

// The origin block's two rules, tested apart because they fail apart.
//
//	1. What it decides — PinOriginIfUnpublished, below.
//	2. Where it may not appear — every statement this package emits, further
//	   down. That one is structural on purpose: the behavioural version of
//	   "published is not a filter" is a listing that returns a row, and a
//	   listing returns rows for many reasons.

func TestPinOriginIfUnpublished(t *testing.T) {
	cases := []struct {
		name        string
		published   bool
		origin      string
		target      string
		wantAllowed bool
		wantPin     string
	}{
		{
			// The ordinary case: the bytes are in shared storage, so the node
			// asking is as good as any other.
			name:        "published snapshot runs anywhere",
			published:   true,
			origin:      "node-a",
			target:      "node-b",
			wantAllowed: true,
			wantPin:     "node-a",
		},
		{
			// 🔴 A hint that misses is not an error. If this returned false the
			// cluster would bounce every placement back to a node that has no
			// special claim on the snapshot at all.
			name:        "published snapshot with no hint still runs anywhere",
			published:   true,
			origin:      "",
			target:      "node-b",
			wantAllowed: true,
			wantPin:     "",
		},
		{
			// 🔴 The whole reason the two columns exist: the bytes never left
			// node-a, so nobody else can start this.
			name:        "unpublished snapshot refuses another node",
			published:   false,
			origin:      "node-a",
			target:      "node-b",
			wantAllowed: false,
			wantPin:     "node-a",
		},
		{
			// And the other half — an unpublished snapshot is not a broken
			// one. Its origin starts it exactly as it would a published one,
			// which is what makes `ready` the right status for it.
			name:        "unpublished snapshot runs on its origin",
			published:   false,
			origin:      "node-a",
			target:      "node-a",
			wantAllowed: true,
			wantPin:     "node-a",
		},
		{
			// The schema's CHECK makes this row impossible, so the value of
			// pinning it here is that it fails closed if the CHECK ever goes.
			name:        "unpublished snapshot with no origin refuses everyone",
			published:   false,
			origin:      "",
			target:      "node-a",
			wantAllowed: false,
			wantPin:     "",
		},
		{
			// Node ids are opaque strings compared exactly. A pin that matched
			// case-insensitively would let a differently-cased id start a
			// sandbox whose bytes are not there.
			name:        "the pin is an exact match",
			published:   false,
			origin:      "Node-A",
			target:      "node-a",
			wantAllowed: false,
			wantPin:     "Node-A",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			row := SnapshotRow{Published: tc.published, OriginNodeID: tc.origin}
			allowed, pin := PinOriginIfUnpublished(row, tc.target)
			if allowed != tc.wantAllowed {
				t.Fatalf("allowed = %v, want %v", allowed, tc.wantAllowed)
			}
			if pin != tc.wantPin {
				t.Fatalf("pin = %q, want %q", pin, tc.wantPin)
			}

			// The alias entry point must not be a second copy of the rule.
			aliasAllowed, aliasPin := PinAliasTarget(
				AliasTarget{Published: tc.published, OriginNodeID: tc.origin}, tc.target)
			if aliasAllowed != allowed || aliasPin != pin {
				t.Fatalf("PinAliasTarget answered (%v, %q) where PinOriginIfUnpublished answered (%v, %q)",
					aliasAllowed, aliasPin, allowed, pin)
			}
		})
	}
}

// TestPinIsTheOnlyPlaceTheOriginBlockDecidesAnything is rule V5, checked
// against the statements themselves rather than against their results.
//
// 🔴 Behavioural coverage cannot establish this. "An unpublished snapshot is
// still listed" passes whether the predicate is absent or merely happens not to
// exclude the row under test, and the cost of getting it wrong is a user told a
// snapshot they can resume does not exist. So the assertion is that the two
// identifiers do not occur after a WHERE at all.
func TestPinIsTheOnlyPlaceTheOriginBlockDecidesAnything(t *testing.T) {
	for name, sql := range everyStatement() {
		t.Run(name, func(t *testing.T) {
			for _, clause := range predicateClausesOf(sql) {
				for _, forbidden := range []string{"published", "origin_node_id"} {
					if strings.Contains(clause, forbidden) {
						t.Fatalf("%s filters on %s:\n%s\n\n"+
							"The origin block is projected and never filtered on. Filtering hides a snapshot "+
							"its origin node can still start, and makes dropping those two columns an edit of "+
							"every query in this package instead of one migration file.",
							name, forbidden, clause)
					}
				}
			}
		})
	}
}

// TestResolvingReadsProjectTheOriginBlock is the other half: not filtering is
// only correct because the values reach the caller, who decides with them.
func TestResolvingReadsProjectTheOriginBlock(t *testing.T) {
	projecting := map[string]string{
		"selectSnapshotByID":    selectSnapshotSQL(byIDPredicate, ReadOptions{OnlyReady: true}),
		"selectSnapshotByAlias": selectSnapshotSQL(byAliasPredicate, ReadOptions{OnlyReady: true}),
		"resolveAlias":          resolveAliasSQL(true),
		"listSnapshots":         mustListSQL(t, ListInput{ClusterID: anyCluster, ReadOptions: ReadOptions{OnlyReady: true}}),
	}
	for name, sql := range projecting {
		head := sql
		if idx := strings.Index(sql, " WHERE "); idx >= 0 {
			head = sql[:idx]
		}
		for _, wanted := range []string{"published", "origin_node_id"} {
			if !strings.Contains(head, wanted) {
				t.Fatalf("%s does not project %s: a resume resolved through it could not be pinned, "+
					"and the caller would start an unpublished snapshot on a node that has none of its bytes",
					name, wanted)
			}
		}
	}
}

// TestUUIDTextOrderMatchesColumnOrder is what lets the listing ORDER BY use the
// uuid column while the keyset predicate compares text.
//
// 🔴 The two would not have to agree. The public pagination cursor orders by
// the id's string form, and PostgreSQL orders a uuid column by its sixteen
// bytes; the reason those coincide is that the canonical rendering is
// lower-case hex, byte by byte, with the separators always in the same places.
// If the ids stopped being canonical — one upper-case rendering is enough —
// they would come apart, and the failure is silent: a page boundary that skips
// rows. This test is what says the assumption still holds.
func TestUUIDTextOrderMatchesColumnOrder(t *testing.T) {
	const n = 500
	raw := make([][16]byte, 0, n)
	for i := 0; i < n; i++ {
		var b [16]byte
		if _, err := rand.Read(b[:]); err != nil {
			t.Fatalf("generate a uuid: %v", err)
		}
		raw = append(raw, b)
	}

	byText := make([]string, 0, n)
	for _, b := range raw {
		byText = append(byText, canonicalUUID(b))
	}
	sort.Strings(byText)

	byBytes := append([][16]byte(nil), raw...)
	sort.Slice(byBytes, func(i, j int) bool {
		return bytes.Compare(byBytes[i][:], byBytes[j][:]) < 0
	})

	for i := range byBytes {
		if want := canonicalUUID(byBytes[i]); byText[i] != want {
			t.Fatalf("at position %d the text order gives %s and the byte order gives %s: "+
				"ORDER BY on the uuid column and the keyset predicate on its text no longer agree",
				i, byText[i], want)
		}
	}
}

func canonicalUUID(b [16]byte) string {
	h := hex.EncodeToString(b[:])
	return fmt.Sprintf("%s-%s-%s-%s-%s", h[0:8], h[8:12], h[12:16], h[16:20], h[20:32])
}

// ─────────────────────────────────────────────────────────────────────────────
// Statement inventory
// ─────────────────────────────────────────────────────────────────────────────

const anyCluster = "11111111-1111-1111-1111-111111111111"

// everyStatement is every piece of SQL this package can send.
//
// Kept as one list so that a statement added without being added here shows up
// as a gap somebody has to close, rather than as coverage that quietly stopped
// being complete.
func everyStatement() map[string]string {
	out := map[string]string{
		"insertSnapshot":         insertSnapshotSQL,
		"insertTemplate":         insertTemplateSQL,
		"releaseOtherAliases":    releaseOtherAliasesSQL,
		"bindAlias":              bindAliasSQL,
		"aliasHolder":            aliasHolderSQL,
		"commitSnapshot":         commitSnapshotSQL,
		"failSnapshot":           failSnapshotSQL,
		"failActiveBuild":        failActiveBuildSQL,
		"softDeleteSnapshot":     softDeleteSnapshotSQL,
		"dropAliasesOfSnapshot":  dropAliasesOfSnapshotSQL,
		"softDeleteTemplate":     softDeleteTemplateSQL,
		"countActiveBuilds":      countActiveBuildsSQL,
		"activeBuildForTemplate": activeBuildForTemplateSQL,
		"markSnapshotBuilding":   markSnapshotBuildingSQL,
		"insertBuild":            insertBuildSQL,
		"renewBuildLease":        renewBuildLeaseSQL,
		"getBuild":               getBuildSQL,
		"reapBuilds":             reapBuildsSQL,
		"failReapedTemplates":    failReapedTemplatesSQL,
		"observedSnapshotStatus": observedSnapshotStatusSQL,
		"resolveAlias/ready":     resolveAliasSQL(true),
		"resolveAlias/any":       resolveAliasSQL(false),
	}
	for _, opts := range everyReadOption() {
		out[fmt.Sprintf("selectSnapshotByID/%v", opts)] = selectSnapshotSQL(byIDPredicate, opts)
		out[fmt.Sprintf("selectSnapshotByAlias/%v", opts)] = selectSnapshotSQL(byAliasPredicate, opts)
	}
	// The listing in every shape it takes: the filters and the cursor each add
	// their own predicates, and a rule about WHERE clauses that skipped the
	// query with the most of them would be the one place worth checking.
	prefix := "p"
	sandbox := "sbx"
	union := "0189ab00-0000-7000-8000-000000000002"
	rich := Filter{
		SourceKinds:       []string{SourceKindSandbox},
		AliasPrefix:       &prefix,
		SnapshotIDs:       []string{"0189ab00-0000-7000-8000-000000000003"},
		SnapshotIDOrAlias: &union,
		SourceSandboxID:   &sandbox,
		TemplateStatuses:  []string{StatusBuilding},
	}
	for _, opts := range everyReadOption() {
		for _, cursor := range []*Cursor{nil, {CreatedAtMs: 1, SnapshotID: "0189ab00-0000-7000-8000-000000000001"}} {
			for label, filter := range map[string]Filter{"bare": {}, "filtered": rich} {
				in := ListInput{ClusterID: anyCluster, Filter: filter, Cursor: cursor, ReadOptions: opts}
				sql, _, err := listSnapshotsSQL(in, defaultListLimit)
				if err != nil {
					panic(err)
				}
				out[fmt.Sprintf("listSnapshots/%s/%v/cursor=%v", label, opts, cursor != nil)] = sql
			}
		}
	}
	return out
}

func everyReadOption() []ReadOptions {
	return []ReadOptions{
		{},
		{OnlyReady: true},
		{WithBuild: true},
		{OnlyReady: true, WithBuild: true},
	}
}

// predicateClausesOf returns the parts of a statement in which a column name
// means "filter on this": everything from a WHERE onwards, plus a LATERAL
// subquery's own WHERE.
func predicateClausesOf(sql string) []string {
	var clauses []string
	rest := sql
	for {
		idx := strings.Index(rest, " WHERE ")
		if idx < 0 {
			idx = strings.Index(rest, "\n WHERE ")
			if idx < 0 {
				break
			}
		}
		clauses = append(clauses, rest[idx:])
		rest = rest[idx+len(" WHERE "):]
	}
	return clauses
}

func mustListSQL(t *testing.T, in ListInput) string {
	t.Helper()

	sql, _, err := listSnapshotsSQL(in, defaultListLimit)
	if err != nil {
		t.Fatalf("build the listing statement: %v", err)
	}
	return sql
}
