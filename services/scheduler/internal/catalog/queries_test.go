package catalog

import (
	"strings"
	"testing"
)

// The shape of the statements, checked without a database.
//
// These are here because three properties of the listing query fail silently:
// a missing ready predicate returns rows nobody should see, an ORDER BY that
// disagrees with the index returns the right rows slowly, and a keyset
// comparison written the obvious way returns *almost* the right rows. None of
// the three raises anything, so each is asserted against the text.

func TestReadyPredicateIsCarriedExactlyWhereItIsAskedFor(t *testing.T) {
	// 🔴 The four resolving reads. Each of them ends in a caller deciding a
	// snapshot exists and can be started; without the predicate, a snapshot
	// whose bytes are still uploading is one of them.
	withPredicate := map[string]string{
		"get by id":          selectSnapshotSQL(byIDPredicate, ReadOptions{OnlyReady: true}),
		"get by alias":       selectSnapshotSQL(byAliasPredicate, ReadOptions{OnlyReady: true}),
		"resolve alias":      resolveAliasSQL(true),
		"list snapshots":     mustListSQL(t, ListInput{ClusterID: anyCluster, ReadOptions: ReadOptions{OnlyReady: true}}),
		"list with a cursor": mustListSQL(t, ListInput{ClusterID: anyCluster, Cursor: &Cursor{CreatedAtMs: 5, SnapshotID: anyCluster}, ReadOptions: ReadOptions{OnlyReady: true}}),
	}
	for name, sql := range withPredicate {
		if !strings.Contains(sql, readyPredicate) {
			t.Fatalf("%s does not carry %q; a snapshot still uploading would resolve through it:\n%s", name, readyPredicate, sql)
		}
	}

	// 🔴 And the reads that must not carry it. get_build exists to look at a
	// build that is running or has failed, and the reaper acts on nothing else;
	// adding the predicate to either makes them answer "no such build" for
	// every build a caller is actually asking about.
	withoutPredicate := map[string]string{
		"get build":            getBuildSQL,
		"reap builds":          reapBuildsSQL,
		"count active builds":  countActiveBuildsSQL,
		"active build":         activeBuildForTemplateSQL,
		"observed status":      observedSnapshotStatusSQL,
		"get by id, unfenced":  selectSnapshotSQL(byIDPredicate, ReadOptions{}),
		"list, unfenced":       mustListSQL(t, ListInput{ClusterID: anyCluster}),
		"resolve alias, any":   resolveAliasSQL(false),
		"fail active build":    failActiveBuildSQL,
		"finish active build":  finishActiveBuildSQL,
		"fail reaped template": failReapedTemplatesSQL,
	}
	for name, sql := range withoutPredicate {
		if strings.Contains(sql, readyPredicate) {
			t.Fatalf("%s carries %q, and it must not:\n%s", name, readyPredicate, sql)
		}
	}
}

// TestTheHeartbeatAxisNeverTakesACallersClock is the property behind the
// reaper, asserted where it can be seen without a database.
//
// 🔴 The three statements below are the whole of the heartbeat axis: two write
// it and one judges it. If any of them took its instant from a parameter, the
// comparison the reaper makes would be between two machines' clocks — and a
// node running slow would have its builds ended while they were still running,
// with the error saying the heartbeat lapsed when it never had. There is no
// test a caller could write that would notice, because both processes behave
// exactly as written; the only symptom is builds dying.
func TestTheHeartbeatAxisNeverTakesACallersClock(t *testing.T) {
	for name, sql := range map[string]string{
		"admit a build":     insertBuildSQL,
		"renew the lease":   renewBuildLeaseSQL,
		"reap stale builds": reapBuildsSQL,
	} {
		if !strings.Contains(sql, "clock_timestamp()") {
			t.Fatalf("%s does not take its instant from the database:\n%s", name, sql)
		}
	}

	// And the reaper's threshold is that clock minus a duration, not an instant
	// somebody sent. A statement carrying `heartbeat_at_ms < $n` with nothing
	// else is the shape this rules out.
	const threshold = "heartbeat_at_ms < " + nowMs + " - $2"
	if !strings.Contains(reapBuildsSQL, threshold) {
		t.Fatalf("the reaper's staleness test is not %q:\n%s", threshold, reapBuildsSQL)
	}
}

// TestListingOrdersTheWayTheIndexDoes pins the ORDER BY to the index's key.
//
// snapshots_list_idx is (cluster_id, source_kind, created_at_ms DESC, id). A
// query whose ORDER BY disagrees does not fail — it sorts the table, and the
// cost of a listing starts tracking the size of the catalog, which is the one
// property the whole move exists to remove.
func TestListingOrdersTheWayTheIndexDoes(t *testing.T) {
	const want = "ORDER BY s.created_at_ms DESC, s.id ASC"
	sql := mustListSQL(t, ListInput{ClusterID: anyCluster, ReadOptions: ReadOptions{OnlyReady: true}})
	if !strings.Contains(sql, want) {
		t.Fatalf("the listing does not order by %q:\n%s", want, sql)
	}
}

// TestKeysetComparisonSwapsItsOperands is the one that is easiest to write
// wrongly and hardest to notice.
//
// `created_at_ms DESC, id ASC` sorts its two keys in opposite directions, so
// the position after a given row is not expressible as a plain row comparison.
// The swap — (created_at_ms, cursor_id) < (cursor_ms, id) — says it in one
// comparison. Written the obvious way round, or spelled out as an OR, the query
// still returns rows; it returns a page that has quietly skipped some.
func TestKeysetComparisonSwapsItsOperands(t *testing.T) {
	in := ListInput{
		ClusterID:   anyCluster,
		Cursor:      &Cursor{CreatedAtMs: 1700000000123, SnapshotID: "0189ab00-0000-7000-8000-000000000001"},
		ReadOptions: ReadOptions{OnlyReady: true},
	}
	sql, args, err := listSnapshotsSQL(in, 10)
	if err != nil {
		t.Fatalf("build the listing statement: %v", err)
	}

	// The comparison's left side names the row's timestamp and the cursor's id;
	// its right side names the cursor's timestamp and the row's id.
	if !strings.Contains(sql, "AND (s.created_at_ms, $") || !strings.Contains(sql, "::text) < ($") || !strings.Contains(sql, "::bigint, s.id::text)") {
		t.Fatalf("the keyset comparison is not the operand swap:\n%s", sql)
	}

	// 🔴 Both ids as text. Comparing one as a uuid and one as text is accepted
	// by PostgreSQL after a cast it inserts itself, and the result is an order
	// that is not the public cursor's.
	if strings.Count(sql, "::text)") < 2 {
		t.Fatalf("the keyset comparison does not compare both ids as text:\n%s", sql)
	}

	if got := args[len(args)-1]; got != int64(11) {
		t.Fatalf("the bound limit is %v, want limit+1 = 11: reading one row past the page is what says there is another page", got)
	}

	// The cursor's id must reach the statement lower-cased; an upper-case
	// rendering sorts on the far side of every real id, because '0'-'9' <
	// 'A'-'F' < 'a'-'f' in ASCII and the page boundary is decided by comparing
	// ids as text. One page silently skipped, no error anywhere.
	//
	// 🔴 So the cursor this half feeds in is upper-case. Asserting the
	// lower-cased form against an id that was already lower-case is a test of
	// nothing at all: it passes with the conversion deleted.
	upper := ListInput{
		ClusterID:   anyCluster,
		Cursor:      &Cursor{CreatedAtMs: 1700000000123, SnapshotID: "0189AB00-0000-7000-8000-00000000000B"},
		ReadOptions: ReadOptions{OnlyReady: true},
	}
	_, upperArgs, err := listSnapshotsSQL(upper, 10)
	if err != nil {
		t.Fatalf("build the listing statement: %v", err)
	}
	found := false
	for _, a := range upperArgs {
		s, ok := a.(string)
		if !ok {
			continue
		}
		if s == "0189ab00-0000-7000-8000-00000000000b" {
			found = true
		}
		if s == "0189AB00-0000-7000-8000-00000000000B" {
			t.Fatalf("the cursor id reached the statement upper-cased: it sorts after every "+
				"canonical id, so this page boundary skips rows and reports nothing:\n%#v", upperArgs)
		}
	}
	if !found {
		t.Fatalf("the cursor id was not bound lower-cased: %#v", upperArgs)
	}
}

func TestListingWithoutACursorHasNoKeysetPredicate(t *testing.T) {
	sql := mustListSQL(t, ListInput{ClusterID: anyCluster, ReadOptions: ReadOptions{OnlyReady: true}})
	if strings.Contains(sql, "s.id::text)") {
		t.Fatalf("the first page carries a keyset comparison it does not need:\n%s", sql)
	}
}

func TestListingRefusesACursorItCannotCompare(t *testing.T) {
	// An id that is not canonical cannot be compared against the column's own
	// text rendering, and the failure would be a silently skipped page rather
	// than an error. Refused where it can still be named.
	for _, bad := range []string{"not-a-uuid", "0189AB000000700080000000000000001", ""} {
		_, _, err := listSnapshotsSQL(ListInput{
			ClusterID: anyCluster,
			Cursor:    &Cursor{CreatedAtMs: 1, SnapshotID: bad},
		}, 10)
		if err == nil {
			t.Fatalf("cursor id %q was accepted", bad)
		}
	}
}

func TestListingRefusesValuesTheTableDoesNotHave(t *testing.T) {
	_, _, err := listSnapshotsSQL(ListInput{
		ClusterID: anyCluster,
		Filter:    Filter{SourceKinds: []string{"snapshot"}},
	}, 10)
	if err == nil || !strings.Contains(err.Error(), "source kind") {
		t.Fatalf("an unknown source kind was accepted: %v", err)
	}

	_, _, err = listSnapshotsSQL(ListInput{
		ClusterID: anyCluster,
		Filter:    Filter{TemplateStatuses: []string{"snapshotting"}},
	}, 10)
	if err == nil || !strings.Contains(err.Error(), "status") {
		t.Fatalf("an unknown status was accepted: %v", err)
	}

	_, _, err = listSnapshotsSQL(ListInput{
		ClusterID: anyCluster,
		Filter:    Filter{SnapshotIDs: []string{"nope"}},
	}, 10)
	if err == nil {
		t.Fatal("a snapshot id that is not a uuid was accepted")
	}
}

// TestSingleSourceKindUsesEqualityNotAny is a plan property, not a result one:
// snapshots_list_idx's second key column is source_kind, and only an equality
// lets the index supply the ordering as well as the filter.
func TestSingleSourceKindUsesEqualityNotAny(t *testing.T) {
	one := mustListSQL(t, ListInput{ClusterID: anyCluster, Filter: Filter{SourceKinds: []string{SourceKindSandbox}}})
	if !strings.Contains(one, "s.source_kind = $") || strings.Contains(one, "s.source_kind = ANY") {
		t.Fatalf("one source kind did not become an equality:\n%s", one)
	}
	two := mustListSQL(t, ListInput{ClusterID: anyCluster, Filter: Filter{SourceKinds: []string{SourceKindSandbox, SourceKindTemplate}}})
	if !strings.Contains(two, "s.source_kind = ANY(") {
		t.Fatalf("two source kinds did not become an ANY:\n%s", two)
	}
}

func TestAliasPrefixDoesNotBecomeAWildcard(t *testing.T) {
	// A user's prefix reaching LIKE would read % and _ as wildcards, so
	// "prod_" would match "production" and a caller filtering to their own
	// aliases would see somebody else's.
	prefix := "prod_%"
	sql := mustListSQL(t, ListInput{ClusterID: anyCluster, Filter: Filter{AliasPrefix: &prefix}})
	if strings.Contains(sql, "LIKE") {
		t.Fatalf("the alias prefix became a LIKE pattern:\n%s", sql)
	}
	if !strings.Contains(sql, "starts_with(a.alias, $") {
		t.Fatalf("the alias prefix is not a literal prefix test:\n%s", sql)
	}
}

func TestClampLimit(t *testing.T) {
	// 🔴 Zero is not "every row". The node's HTTP layer reads a missing limit
	// that way, which is how one request comes to pull ten thousand rows into
	// memory; repeating it here would move the fault rather than fix it.
	if got := clampLimit(0); got != defaultListLimit {
		t.Fatalf("clampLimit(0) = %d, want the default %d", got, defaultListLimit)
	}
	if got := clampLimit(10); got != 10 {
		t.Fatalf("clampLimit(10) = %d", got)
	}
	if got := clampLimit(maxListLimit + 1); got != maxListLimit {
		t.Fatalf("clampLimit(%d) = %d, want the cap %d", maxListLimit+1, got, maxListLimit)
	}
}

func TestStatusGroupMappingMatchesTheTrigger(t *testing.T) {
	// The Go copy exists only to predict what a row will carry. It is checked
	// against the trigger with a database elsewhere; this is the table of the
	// four the trigger names, so a fifth status added on one side shows up.
	for status, want := range map[string]string{
		StatusWaiting:  StatusGroupPending,
		StatusBuilding: StatusGroupInProgress,
		StatusReady:    StatusGroupReady,
		StatusError:    StatusGroupFailed,
	} {
		if got := statusGroupFor(status); got != want {
			t.Fatalf("statusGroupFor(%q) = %q, want %q", status, got, want)
		}
	}
	if got := statusGroupFor("something-else"); got != StatusGroupFailed {
		t.Fatalf("an unknown status mapped to %q; the trigger's ELSE arm is 'failed'", got)
	}
}

func TestRequireUUIDNormalisesAndRefuses(t *testing.T) {
	got, err := requireUUID("id", "  0189AB00-0000-7000-8000-000000000001 ")
	if err != nil {
		t.Fatalf("a canonical upper-case uuid was refused: %v", err)
	}
	// 🔴 Lower-cased, because page boundaries are decided by comparing ids as
	// text and in ASCII 'A' < 'a'.
	if got != "0189ab00-0000-7000-8000-000000000001" {
		t.Fatalf("requireUUID returned %q, want it lower-cased", got)
	}
	for _, bad := range []string{"", "   ", "0189ab0000007000800000000000001", "{0189ab00-0000-7000-8000-000000000001}", "zzzzzzzz-0000-7000-8000-000000000001"} {
		if _, err := requireUUID("id", bad); err == nil {
			t.Fatalf("%q was accepted as a uuid", bad)
		}
	}
}

func TestRequireJSONObjectRefusesWhatPoisonsAColumn(t *testing.T) {
	if err := requireJSONObject("build_error", []byte(`{"message":"x"}`)); err != nil {
		t.Fatalf("an object was refused: %v", err)
	}
	// 🔴 `null` is the one document that passes every cheaper check and then
	// fails to decode on the node, where the decoder is shared by every read.
	for _, bad := range []string{``, `null`, `[]`, `"x"`, `{`} {
		if err := requireJSONObject("build_error", []byte(bad)); err == nil {
			t.Fatalf("%q was accepted", bad)
		}
	}
}
