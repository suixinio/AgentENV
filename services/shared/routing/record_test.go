package routing

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
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
		{
			name:     "an explicit confirmed state routes like an absent one",
			raw:      `{"node":{"node_id":"node-a","endpoint":"http://node-a"},"execution_id":"exec-1","state":"confirmed"}`,
			wantOK:   true,
			wantNode: Node{ID: "node-a", Endpoint: "http://node-a"},
			// The writer omits the field instead of spelling this out, but its
			// own decoder accepts it, so this one does too.
			wantExecID: "exec-1",
		},
		{name: "no node id names nowhere", raw: `{"node":{"endpoint":"http://node-a"}}`, wantOK: false},
		{name: "no endpoint names nowhere", raw: `{"node":{"node_id":"node-a"}}`, wantOK: false},
		{name: "not json", raw: `not-json`, wantOK: false},
		{name: "empty object", raw: `{}`, wantOK: false},
		{
			name: "a reservation names an assignment, not a place to forward to",
			raw:  `{"node":{"node_id":"node-a","endpoint":"http://node-a"},"execution_id":"exec-1","state":"` + stateStarting + `"}`,
			// A complete record, naming a real node — refused on its state
			// alone, which is the only thing separating it from the first case.
			wantOK: false,
		},
		{
			name:   "a state this build does not know is not routable either",
			raw:    `{"node":{"node_id":"node-a","endpoint":"http://node-a"},"state":"draining"}`,
			wantOK: false,
		},
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

// 🔴 TestMarshalRecordShape used to sit here, asserting MarshalRecord's output
// against a literal. It went with the encoder: it was that function's only
// remaining caller, and what it pinned — the field names and order, and
// `execution_id` being omitted when empty — is pinned by
// `marshal_record`'s own tests in `crates/aenv-api/src/binding_store/record.rs`, against the
// process that actually writes these bytes. The literals below are the same
// shape, read from the decoding side.

// The two shapes a live writer actually puts in Redis, written out.
//
// 🔴 Literals, and not calls to this package's own encoder. Go does not write
// these bytes any more — `aenv-api` does, from `crates/aenv-api/src/binding_store/record.rs`'s
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
	// A reservation, as the writer emits it while a create is in flight: the
	// same shape with the state spelled out. 🔴 Its confirmed twin — the same
	// node, the same incarnation, no state — is `storedReservationConfirmed`,
	// and the pair is what makes the state the only difference between a record
	// this package routes and one it refuses.
	storedReservationRecord = `{"node":{"node_id":"node-a","endpoint":"http://node-a"},"execution_id":"exec-1","state":"starting","reserved_at_ms":1700000000000}`
	// The confirmation of that same reservation.
	storedReservationConfirmed = `{"node":{"node_id":"node-a","endpoint":"http://node-a"},"execution_id":"exec-1"}`
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

// TestAReservationIsNotRoutableAndItsConfirmationIs decodes the exact pair of
// strings the live writer produces for one create, either side of the node
// acknowledging the sandbox.
//
// 🔴 The confirmed half is the control, and it is what makes this test about
// the state field rather than about parsing. Both literals name the same node
// and the same incarnation; a change that made ParseRecord refuse everything,
// or accept everything, fails on one half or the other.
//
// A refused reservation is a projection miss, not an error and not an absence:
// the request goes on to the api half, whose `lookup_node` refuses the same
// record as `unavailable_starting`. The gateway never decides this itself.
func TestAReservationIsNotRoutableAndItsConfirmationIs(t *testing.T) {
	if _, ok := ParseRecord([]byte(storedReservationRecord)); ok {
		t.Fatalf("a reservation was routed: %s", storedReservationRecord)
	}

	got, ok := ParseRecord([]byte(storedReservationConfirmed))
	if !ok {
		t.Fatalf("the confirmation of that reservation did not parse: %s", storedReservationConfirmed)
	}
	if got.Node != (Node{ID: "node-a", Endpoint: "http://node-a"}) {
		t.Fatalf("node = %+v", got.Node)
	}
	if got.ExecutionID != "exec-1" {
		t.Fatalf("execution id = %q, want %q", got.ExecutionID, "exec-1")
	}
}

// rustWriterSource is the file holding the only writer of these bytes in the
// cluster: `aenv-api`'s `marshal_record`.
const rustWriterSource = "../../../crates/aenv-api/src/binding_store/record.rs"

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
		"storedReservationRecord":    storedReservationRecord,
		"storedReservationConfirmed": storedReservationConfirmed,
	} {
		if !strings.Contains(string(source), literal) {
			t.Fatalf("%s is not asserted in %s:\n\t%s\n"+
				"Go decodes this shape and Rust writes it; if one side's literal moved, move both.",
				name, rustWriterSource, literal)
		}
	}
}

// TestBindingKey pins the one key this package derives. 🔴 It used to pin a
// second, NodeIndexKey — the reverse index — which went with the function: only
// a writer maintains that set and this module has none. `aenv-api`'s
// `node_index_key` keeps the format, pinned by `keys_match_gos_format`.
func TestBindingKey(t *testing.T) {
	if got := BindingKey(DefaultKeyPrefix, "sbx-1"); got != "agentenv:scheduler:bindings:sandbox:sbx-1" {
		t.Fatalf("binding key drifted: %s", got)
	}
}

func TestSynthesizeFillsEveryFieldTheReaderCannotSee(t *testing.T) {
	const podName = "agentenv-node-7f4c2"
	record := Record{
		Node:        Node{ID: "node-a", Endpoint: "http://node-a", PodName: podName},
		ExecutionID: "0198b7cc-1111-7000-8000-000000000001",
	}
	answer := Synthesize(record)

	if answer.Node.ID != "node-a" || answer.Node.Endpoint != "http://node-a" {
		t.Fatalf("node not carried through: %+v", answer.Node)
	}
	// 🔴 The pod name is an identity, not an address. Putting it in a route
	// answer would invite a caller to forward to it.
	//
	// Asserted over the whole marshalled answer rather than over the one field
	// that could hold it: the day the answer grows another place for it, this
	// is what notices.
	raw, err := json.Marshal(answer)
	if err != nil {
		t.Fatalf("marshal the synthesized answer: %v", err)
	}
	if strings.Contains(string(raw), podName) {
		t.Fatalf("the pod name reached the route answer: %s", raw)
	}
	if answer.Location != LocationBound {
		t.Fatalf("location = %v, want bound: a projection record has exactly one location", answer.Location)
	}
	if answer.ExecutionID != record.ExecutionID {
		t.Fatalf("execution id = %q, want %q", answer.ExecutionID, record.ExecutionID)
	}
	if answer.Authority != AuthorityRegistry {
		t.Fatalf("authority = %v, want registry", answer.Authority)
	}
}

func TestSynthesizeWithNoIncarnationClaimsNoAuthority(t *testing.T) {
	answer := Synthesize(Record{Node: Node{ID: "node-a", Endpoint: "http://node-a"}})
	if answer.ExecutionID != "" {
		t.Fatalf("execution id = %q, want empty", answer.ExecutionID)
	}
	if answer.Authority != AuthorityUnknown {
		t.Fatalf("authority = %v, want unknown: registry is never reported without a value", answer.Authority)
	}
	if answer.Location != LocationBound {
		t.Fatalf("location = %v, want bound even with no incarnation", answer.Location)
	}
}

// 🔴 TestNodeProtoRoundTrip used to close this file, and it was NodeFromProto's
// only caller anywhere — a test of a function nothing else used, which is what
// made both deletable. The one property in it that was about production code
// rather than about the round trip — that a pod name never reaches the wire
// node — is asserted by TestSynthesizeFillsEveryFieldTheReaderCannotSee above,
// over the whole marshalled lookup answer, which is where it actually matters.
