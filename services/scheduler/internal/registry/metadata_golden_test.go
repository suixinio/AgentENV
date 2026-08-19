package registry

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
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

// requiredMetadataFields have neither a Go-side equivalent of `Option` nor a
// serde default on the node, so a document missing any one of them stops
// decoding there — and `get`, `get_many` and `claim_for_resume` share that
// decoder, which makes the sandbox unreadable and unclaimable at the same time.
// The failure only surfaces at the next resume, possibly days later.
//
// Kept in step by hand with REQUIRED_FIELDS in
// src/orchestrator/store/metadata.rs; the Rust side proves the list is exact by
// dropping each field and checking the record stops decoding.
var requiredMetadataFields = []string{
	"id",
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
