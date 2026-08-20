package registry

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"github.com/jackc/pgx/v5"
)

// metadataFixtures are the golden documents the node writes into the metadata
// column, dumped from the real Rust struct by
// `cargo test -p agentenv --lib metadata::golden`. Both sides read these very
// files: they are the only shared statement of what that column contains.
//
// The full one has every optional field set, so both fields the node omits when
// empty are present and the two camelCase keys are exercised. The minimal one
// has neither, because a valid document does not have a fixed key set.
var metadataFixtures = []string{
	"sandbox_metadata_full.json",
	"sandbox_metadata_minimal.json",
}

// requiredFieldsFixture is the third file in that set, and the only one that is
// not a document: it is REQUIRED_FIELDS itself, dumped by the same Rust test
// run. It exists so this side can compare its list against the node's list
// instead of against a document that happens to contain it — see
// TestTheRequiredFieldListMatchesTheNodes for why that difference is the whole
// point.
const requiredFieldsFixture = "sandbox_metadata_required_fields.json"

// requiredMetadataFields have neither a Go-side equivalent of `Option` nor a
// serde default on the node, so a document missing any one of them stops
// decoding there — and `get`, `get_many` and `claim_for_resume` share that
// decoder, which makes the sandbox unreadable and unclaimable at the same time.
// The failure only surfaces at the next resume, possibly days later.
//
// The Rust side proves the list is exact by dropping each field and checking
// the record stops decoding, and publishes it as a fixture of its own that
// TestTheRequiredFieldListMatchesTheNodes compares this list against.
//
// 🔴 Kept in step by hand until 2026-08-20, and the hand-sync had teeth in one
// direction only: these names were asserted to be *present* in the metadata
// fixtures, so adding a field the Rust side does not require failed here, while
// dropping one the Rust side does require passed — the fixtures simply carry
// more keys than this list names. `execution_id` was exactly that case: the
// node made it required and regenerated the fixtures, and this list stayed
// green while silently naming one field fewer.
//
// ✅ 已订正: the list is no longer a restatement. metadata.rs publishes
// REQUIRED_FIELDS into requiredFieldsFixture, both sides read that same file,
// and the comparison below is set equality rather than containment — so a name
// added there and a name dropped here each fail. This list is kept in source
// anyway, spelled out, because the reader of these tests has to be able to see
// what is being asserted without opening a fixture; what changed is that it is
// now checked rather than trusted.
var requiredMetadataFields = []string{
	"id",
	// Required on the node since the incarnation work: a record without it is
	// refused at load rather than given a fresh incarnation, because a fresh
	// one would name a VM that never ran.
	"execution_id",
	"snapshot_id",
	"state",
	"created_at",
	"timeout_action",
	"auto_resume",
	"runtime_versions",
	"resources",
	"context",
	"network_policy",
}

func readMetadataFixture(t *testing.T, name string) json.RawMessage {
	t.Helper()

	// The fixtures live at the repository root, outside this Go module, on
	// purpose: a copy inside `services/` would be a second truth that drifts.
	path := filepath.Join("..", "..", "..", "..", "tests", "fixtures", name)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if !json.Valid(raw) {
		t.Fatalf("%s is not valid JSON", path)
	}

	return json.RawMessage(raw)
}

// decodeCanonical decodes into plain Go values with numbers left as their
// literal text.
//
// Decoding into `any` the ordinary way turns every number into a float64, which
// would quietly agree that 1755561600123456789 round-tripped when it had been
// rounded. json.Number keeps the digits, so the comparison sees the difference.
func decodeCanonical(t *testing.T, raw []byte) any {
	t.Helper()

	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()

	var value any
	if err := decoder.Decode(&value); err != nil {
		t.Fatalf("decode metadata: %v", err)
	}

	return value
}

// TestMetadataSurvivesTheJSONBRoundTrip is the safety net for the control plane
// starting to carry this column.
//
// The node's struct has no `deny_unknown_fields`, so anything that passes the
// document through a hand-written Go schema drops what it does not recognise
// without reporting it. Treating it as opaque bytes — json.RawMessage in, jsonb
// out — is the only shape that cannot lose a field, and this proves it does not.
func TestMetadataSurvivesTheJSONBRoundTrip(t *testing.T) {
	dsn := setupRegistryDatabase(t)

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect to test database failed: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })

	for i, name := range metadataFixtures {
		t.Run(name, func(t *testing.T) {
			fixture := readMetadataFixture(t, name)
			sandboxID := metadataFixtureSandboxID(i)

			if _, err := conn.Exec(ctx, `
INSERT INTO paused_sandboxes (
    sandbox_id, cluster_id, state, generation, origin_node_id, claimed_by_node_id,
    snapshot_id, metadata, paused_at, updated_at, lease_expires_at, sandbox_expires_at
) VALUES ($1, $2, 'paused', 1, 'node-a', NULL, NULL, $3, now(), now(), now() + interval '90 seconds', NULL)`,
				sandboxID, clusterA, []byte(fixture),
			); err != nil {
				t.Fatalf("insert metadata failed: %v", err)
			}

			var readBack []byte
			if err := conn.QueryRow(ctx,
				"SELECT metadata FROM paused_sandboxes WHERE sandbox_id = $1", sandboxID,
			).Scan(&readBack); err != nil {
				t.Fatalf("read metadata back failed: %v", err)
			}

			// PostgreSQL reorders jsonb keys and drops insignificant
			// whitespace, so the bytes are allowed to differ; the key set and
			// the values are not.
			if !reflect.DeepEqual(decodeCanonical(t, fixture), decodeCanonical(t, readBack)) {
				t.Fatalf("metadata did not survive the round trip.\nwrote: %s\nread:  %s", fixture, readBack)
			}

			// Same claim, asserted by the database itself rather than by our
			// decoder, so a bug in decodeCanonical cannot hide a real loss.
			var equal bool
			if err := conn.QueryRow(ctx,
				"SELECT metadata = $2::jsonb FROM paused_sandboxes WHERE sandbox_id = $1",
				sandboxID, []byte(fixture),
			).Scan(&equal); err != nil {
				t.Fatalf("compare metadata failed: %v", err)
			}
			if !equal {
				t.Fatal("the stored jsonb is not equal to the document that was written")
			}

			stored, ok := decodeCanonical(t, readBack).(map[string]any)
			if !ok {
				t.Fatal("metadata did not read back as an object")
			}
			for _, field := range requiredMetadataFields {
				if _, present := stored[field]; !present {
					t.Fatalf("required field %q did not survive the round trip", field)
				}
			}

			assertImageConfigKeys(t, name, stored)
		})
	}
}

// assertImageConfigKeys pins the two camelCase keys in an otherwise snake_case
// document. Any wholesale tag policy on the Go side — camel everything, snake
// everything — gets exactly these two wrong, and gets them wrong silently.
func assertImageConfigKeys(t *testing.T, fixture string, stored map[string]any) {
	t.Helper()

	entries, present := stored["image_configs"]
	if !present {
		// The minimal document omits the field entirely, which is a valid
		// shape and the reason there are two fixtures.
		if fixture == "sandbox_metadata_full.json" {
			t.Fatal("the full fixture must carry image_configs")
		}

		return
	}

	seen := map[string]bool{}
	for _, entry := range entries.([]any) {
		for key := range entry.(map[string]any) {
			seen[key] = true
		}
	}
	for _, key := range []string{"mountPath", "driveId"} {
		if !seen[key] {
			t.Fatalf("camelCase key %q did not survive the round trip", key)
		}
	}
}

// metadataFixtureSandboxID keeps these rows clear of the ones
// setupRegistryDatabase seeds.
func metadataFixtureSandboxID(index int) string {
	return [...]string{
		"dddddddd-0000-0000-0000-000000000001",
		"dddddddd-0000-0000-0000-000000000002",
	}[index]
}

// TestMetadataFixturesAreReadableWithoutADatabase catches a moved or renamed
// fixture even on a runner with no PostgreSQL, where the round-trip test above
// skips.
func TestMetadataFixturesAreReadableWithoutADatabase(t *testing.T) {
	for _, name := range metadataFixtures {
		fixture := readMetadataFixture(t, name)
		stored, ok := decodeCanonical(t, fixture).(map[string]any)
		if !ok {
			t.Fatalf("%s is not a JSON object", name)
		}
		for _, field := range requiredMetadataFields {
			if _, present := stored[field]; !present {
				t.Fatalf("%s is missing required field %q", name, field)
			}
		}
	}
}

// TestTheRequiredFieldListMatchesTheNodes is the tooth the hand-sync did not
// have.
//
// 🔴 Set equality, not containment, and the difference is the entire reason
// this test exists. The two assertions above check that every name in
// requiredMetadataFields is present in a document — and the documents carry
// more keys than the list names, so they answer "is this list a subset of a
// valid record", which stays true no matter how many names the list loses.
// This one answers "is this list the node's list", which is the question the
// column's decodability actually turns on:
//
//   - a name added here that the node does not require fails, as it did before;
//   - a name dropped here that the node does require now fails too. That is the
//     direction `execution_id` slipped through, and it is the dangerous one: a
//     writer on this side is free to omit what nobody names, and the row it
//     produces stops decoding on every node read path at once — get, get_many
//     and claim_for_resume share the decoder — surfacing at the next resume,
//     possibly days later.
//
// Neither side owns the file: the Rust test writes it under
// UPDATE_METADATA_GOLDEN=1 and fails when it is stale, so a change to
// REQUIRED_FIELDS that is not published fails there, and one that is published
// but not adopted fails here. Same shape as the manifest test in
// services/shared/config: read the other side's artefact, do not restate it.
//
// No database and no fixtures-as-documents involved, so this runs on every
// runner, including the ones where the round-trip test above has no PostgreSQL.
func TestTheRequiredFieldListMatchesTheNodes(t *testing.T) {
	var published []string
	if err := json.Unmarshal(readMetadataFixture(t, requiredFieldsFixture), &published); err != nil {
		t.Fatalf("decode %s: %v", requiredFieldsFixture, err)
	}
	if len(published) == 0 {
		// A file that decoded to nothing would agree with an empty list on this
		// side, which is the one shape that must not read as agreement.
		t.Fatalf("%s names no fields at all", requiredFieldsFixture)
	}

	ours := map[string]bool{}
	for _, field := range requiredMetadataFields {
		if ours[field] {
			t.Fatalf("requiredMetadataFields names %q twice", field)
		}
		ours[field] = true
	}

	theirs := map[string]bool{}
	for _, field := range published {
		theirs[field] = true
	}

	var missing, extra []string
	for field := range theirs {
		if !ours[field] {
			missing = append(missing, field)
		}
	}
	for field := range ours {
		if !theirs[field] {
			extra = append(extra, field)
		}
	}
	sort.Strings(missing)
	sort.Strings(extra)

	if len(missing) > 0 {
		t.Errorf("the node requires %v and this side does not name them. A write from here may omit "+
			"one, and the row it produces stops decoding on the node in every read path at once. "+
			"Add them to requiredMetadataFields", missing)
	}
	if len(extra) > 0 {
		t.Errorf("this side names %v and the node does not require them. Either the node dropped a "+
			"requirement and %s was regenerated, or these names were never right",
			extra, requiredFieldsFixture)
	}
}
