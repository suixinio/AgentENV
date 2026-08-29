package routing

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	schedulerv1 "agentenv/services/api/proto"
)

func TestParseRecordCases(t *testing.T) {
	cases := []struct {
		name        string
		raw         string
		wantOK      bool
		wantNode    Node
		wantExecID  string
		explanation string
	}{
		{
			name:       "full record",
			raw:        `{"node":{"node_id":"node-a","endpoint":"http://node-a","pod_name":"pod-a"},"execution_id":"0198b7cc-1111-7000-8000-000000000001"}`,
			wantOK:     true,
			wantNode:   Node{ID: "node-a", Endpoint: "http://node-a", PodName: "pod-a"},
			wantExecID: "0198b7cc-1111-7000-8000-000000000001",
		},
		{
			name:     "no incarnation is unknown, not a decode failure",
			raw:      `{"node":{"node_id":"node-a","endpoint":"http://node-a"}}`,
			wantOK:   true,
			wantNode: Node{ID: "node-a", Endpoint: "http://node-a"},
			// 🔴 This is what every record written before the field existed
			// looks like. Refusing it would blank the table on an upgrade.
			wantExecID: "",
		},
		{
			name:     "whitespace is trimmed on the way out",
			raw:      `{"node":{"node_id":" node-a ","endpoint":" http://node-a ","pod_name":" pod-a "},"execution_id":" abc "}`,
			wantOK:   true,
			wantNode: Node{ID: "node-a", Endpoint: "http://node-a", PodName: "pod-a"},

			wantExecID: "abc",
		},
		{name: "no node id names nowhere", raw: `{"node":{"endpoint":"http://node-a"}}`, wantOK: false},
		{name: "no endpoint names nowhere", raw: `{"node":{"node_id":"node-a"}}`, wantOK: false},
		{name: "not json", raw: `not-json`, wantOK: false},
		{name: "empty object", raw: `{}`, wantOK: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := ParseRecord([]byte(tc.raw))
			if ok != tc.wantOK {
				t.Fatalf("ParseRecord ok = %v, want %v", ok, tc.wantOK)
			}
			if !tc.wantOK {
				if got != (Record{}) {
					t.Fatalf("a refused record must come back zeroed, got %+v", got)
				}
				return
			}
			if got.Node != tc.wantNode {
				t.Fatalf("node = %+v, want %+v", got.Node, tc.wantNode)
			}
			if got.ExecutionID != tc.wantExecID {
				t.Fatalf("execution id = %q, want %q", got.ExecutionID, tc.wantExecID)
			}
		})
	}
}

// TestMarshalRecordShape pins the stored bytes. 🔴 The scheduler's heartbeat
// script splices a record together inside Lua rather than re-encoding the node,
// and it splices against exactly this shape — so a change to the json tags that
// only this side knew about would produce records the script writes one way and
// this reads another.
func TestMarshalRecordShape(t *testing.T) {
	value, err := MarshalRecord(Node{ID: "node-a", Endpoint: "http://node-a"}, "exec-1")
	if err != nil {
		t.Fatalf("MarshalRecord failed: %v", err)
	}
	const want = `{"node":{"node_id":"node-a","endpoint":"http://node-a"},"execution_id":"exec-1"}`
	if value != want {
		t.Fatalf("stored shape drifted:\n got %s\nwant %s", value, want)
	}

	// An empty incarnation is omitted, so a record written now and one written
	// before the field existed are byte-identical.
	value, err = MarshalRecord(Node{ID: "node-a", Endpoint: "http://node-a"}, "")
	if err != nil {
		t.Fatalf("MarshalRecord failed: %v", err)
	}
	if value != `{"node":{"node_id":"node-a","endpoint":"http://node-a"}}` {
		t.Fatalf("empty incarnation must be omitted, got %s", value)
	}
}

// The two shapes a live writer actually puts in Redis, written out.
//
// 🔴 Literals, and not calls to this package's own encoder. Go does not write
// these bytes any more — `aenv-api` does, from `src/binding_store/record.rs`'s
// `marshal_record` — so a test that encoded with a Go function and decoded with
// a Go function would pass for any format the two agreed on, including one no
// writer in the cluster produces. That is not a hypothetical: the format is the
// only thing holding the gateway's routing projection and the api half's
// binding store together, and every other test in this module is green whatever
// it says.
//
// `binding_store/record.rs`'s `go_and_rust_agree_on_the_stored_record_bytes`
// asserts `marshal_record` emits these same two strings, byte for byte. The two
// languages are pinned to one string rather than to each other's code, so
// neither side can move the format by agreeing with itself.
//
// Serde and encoding/json both emit declaration order, and both omit an empty
// `pod_name` / `execution_id`, which is what makes the shape:
//
//	{"node":{"node_id":…,"endpoint":…[,"pod_name":…]}[,"execution_id":…]}
const (
	// With a pod name — what a node whose heartbeat still reports its pod
	// identity produces.
	storedRecordWithPodName = `{"node":{"node_id":"node-a","endpoint":"http://node-a","pod_name":"agentenv-node-7f4c2"},"execution_id":"0198b7cc-1111-7000-8000-000000000001"}`
	// Without one — the ordinary shape, since `pod_name` is omitted when empty.
	storedRecordWithoutPodName = `{"node":{"node_id":"node-a","endpoint":"http://node-a"},"execution_id":"0198b7cc-1111-7000-8000-000000000001"}`
)

// storedRecordExecutionID is the incarnation both literals above carry.
const storedRecordExecutionID = "0198b7cc-1111-7000-8000-000000000001"

// TestTheStoredRecordShapesParse decodes the exact bytes the live writer
// produces, in both of the shapes it produces.
//
// It replaces TestMarshalRecordRoundTrips, which encoded with MarshalRecord and
// decoded with ParseRecord and so asserted only that this package agreed with
// itself. 🔴 The pod-name shape in particular had no literal anywhere on this
// side: the encoder test pinned the form without one, and this side's only
// pod-name literal lived in TestParseRecordCases under a made-up value
// ("pod-a") that no writer emits.
func TestTheStoredRecordShapesParse(t *testing.T) {
	for _, tc := range []struct {
		name     string
		stored   string
		wantNode Node
	}{
		{
			name:     "with a pod name",
			stored:   storedRecordWithPodName,
			wantNode: Node{ID: "node-a", Endpoint: "http://node-a", PodName: "agentenv-node-7f4c2"},
		},
		{
			name:     "without one",
			stored:   storedRecordWithoutPodName,
			wantNode: Node{ID: "node-a", Endpoint: "http://node-a"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := ParseRecord([]byte(tc.stored))
			if !ok {
				t.Fatalf("the bytes the live writer produces did not parse: %s", tc.stored)
			}
			if got.Node != tc.wantNode {
				t.Fatalf("node = %+v, want %+v", got.Node, tc.wantNode)
			}
			if got.ExecutionID != storedRecordExecutionID {
				t.Fatalf("execution id = %q, want %q", got.ExecutionID, storedRecordExecutionID)
			}
		})
	}
}

// rustWriterSource is the file holding the only writer of these bytes in the
// cluster: `aenv-api`'s `marshal_record`.
const rustWriterSource = "../../../src/binding_store/record.rs"

// TestTheStoredLiteralsAreTheOnesRustAssertsToo closes the one gap the two
// suites leave on their own.
//
// Rust asserting `marshal_record` emits a string, and Go asserting `ParseRecord`
// accepts a string, are two true statements about two strings — and nothing so
// far makes them the same string. `ParseRecord` is deliberately tolerant (a
// missing `execution_id` is not a decode failure, whitespace is trimmed), so a
// Go literal that drifted would keep passing here while no writer produced it,
// and Go's decoder would then be verified against a format that exists nowhere.
//
// 🔴 This is a source scan, so its polarity is the whole safety argument: it
// asserts the literals are *present* in the Rust writer's test, and an absent
// file is a failure rather than a skip. Editing either language's copy of a
// literal without the other turns this red — which is what "pinned to one
// string" has to mean to be worth anything.
func TestTheStoredLiteralsAreTheOnesRustAssertsToo(t *testing.T) {
	source, err := os.ReadFile(rustWriterSource)
	if err != nil {
		t.Fatalf("cannot read %s, the writer these literals came from: %v", rustWriterSource, err)
	}
	for name, literal := range map[string]string{
		"storedRecordWithPodName":    storedRecordWithPodName,
		"storedRecordWithoutPodName": storedRecordWithoutPodName,
	} {
		if !strings.Contains(string(source), literal) {
			t.Fatalf("%s is not asserted in %s:\n\t%s\n"+
				"Go decodes this shape and Rust writes it; if one side's literal moved, move both.",
				name, rustWriterSource, literal)
		}
	}
}

func TestBindingKeyAndNodeIndexKey(t *testing.T) {
	if got := BindingKey(DefaultKeyPrefix, "sbx-1"); got != "agentenv:scheduler:bindings:sandbox:sbx-1" {
		t.Fatalf("binding key drifted: %s", got)
	}
	if got := NodeIndexKey(DefaultKeyPrefix, "node-a"); got != "agentenv:scheduler:bindings:node:node-a" {
		t.Fatalf("node index key drifted: %s", got)
	}
}

func TestSynthesizeFillsEveryFieldTheReaderCannotSee(t *testing.T) {
	const podName = "agentenv-node-7f4c2"
	record := Record{
		Node:        Node{ID: "node-a", Endpoint: "http://node-a", PodName: podName},
		ExecutionID: "0198b7cc-1111-7000-8000-000000000001",
	}
	resp := Synthesize(record)

	if resp.GetNode().GetNodeId() != "node-a" || resp.GetNode().GetEndpoint() != "http://node-a" {
		t.Fatalf("node not carried through: %+v", resp.GetNode())
	}
	// 🔴 The pod name is an identity, not an address. Putting it in a lookup
	// answer would invite a caller to forward to it.
	//
	// Asserted over the whole marshalled message rather than over the one
	// field that could hold it today: the wire node has no pod field at all
	// just now, so a field-by-field check would be a check of nothing, and the
	// day something grows one this is what notices.
	raw, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal the synthesized answer: %v", err)
	}
	if strings.Contains(string(raw), podName) {
		t.Fatalf("the pod name reached the lookup answer: %s", raw)
	}
	if resp.GetLocation() != schedulerv1.SandboxLocation_SANDBOX_LOCATION_BOUND {
		t.Fatalf("location = %v, want BOUND: the binding hit this stands in for has exactly one location", resp.GetLocation())
	}
	if resp.GetOriginNodeId() != "" {
		t.Fatalf("origin node id = %q, want empty: the binding hit never names one", resp.GetOriginNodeId())
	}
	if resp.GetExecutionId() != record.ExecutionID {
		t.Fatalf("execution id = %q, want %q", resp.GetExecutionId(), record.ExecutionID)
	}
	if resp.GetExecutionAuthority() != schedulerv1.ExecutionAuthority_EXECUTION_AUTHORITY_REGISTRY {
		t.Fatalf("authority = %v, want REGISTRY", resp.GetExecutionAuthority())
	}
}

func TestSynthesizeWithNoIncarnationClaimsNoAuthority(t *testing.T) {
	resp := Synthesize(Record{Node: Node{ID: "node-a", Endpoint: "http://node-a"}})
	if resp.GetExecutionId() != "" {
		t.Fatalf("execution id = %q, want empty", resp.GetExecutionId())
	}
	if resp.GetExecutionAuthority() != schedulerv1.ExecutionAuthority_EXECUTION_AUTHORITY_UNKNOWN {
		t.Fatalf("authority = %v, want UNKNOWN: REGISTRY is never reported without a value", resp.GetExecutionAuthority())
	}
	if resp.GetLocation() != schedulerv1.SandboxLocation_SANDBOX_LOCATION_BOUND {
		t.Fatalf("location = %v, want BOUND even with no incarnation", resp.GetLocation())
	}
}

func TestNodeProtoRoundTrip(t *testing.T) {
	node := Node{ID: "node-a", Endpoint: "http://node-a", PodName: "pod-a"}
	back := NodeFromProto(node.ToProto())
	if back.ID != node.ID || back.Endpoint != node.Endpoint {
		t.Fatalf("round trip lost the address: %+v", back)
	}
	if back.PodName != "" {
		t.Fatalf("pod name must not travel on the wire node, got %q", back.PodName)
	}
	if got := NodeFromProto(nil); got != (Node{}) {
		t.Fatalf("a nil message must decode to the zero node, got %+v", got)
	}
}
