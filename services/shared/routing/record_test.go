package routing

import (
	"encoding/json"
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

func TestMarshalRecordRoundTrips(t *testing.T) {
	node := Node{ID: "node-a", Endpoint: "http://node-a", PodName: "pod-a"}
	value, err := MarshalRecord(node, "exec-1")
	if err != nil {
		t.Fatalf("MarshalRecord failed: %v", err)
	}
	got, ok := ParseRecord([]byte(value))
	if !ok {
		t.Fatal("a record this package wrote must parse back")
	}
	if got.Node != node || got.ExecutionID != "exec-1" {
		t.Fatalf("round trip lost something: got %+v", got)
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
