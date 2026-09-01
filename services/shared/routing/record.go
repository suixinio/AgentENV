// Package routing reads the routing projection: the record written when the
// control plane learns where a sandbox is, and everything needed to turn one
// back into a lookup answer.
//
// 🔴 It reads, and it does not write. It used to do both, when
// `services/scheduler` wrote these keys and answered lookups from them and
// Go's internal rule kept the gateway out of that package's internals. That
// process is deleted: `aenv-api` writes every record now
// (`src/binding_store/record.rs`), and the gateway reads them directly and
// synthesises the answer a lookup would have given.
//
// So the format lives in two languages rather than in two Go packages, which
// changes what keeps the copies together. Nothing here can be exercised against
// the writer by calling it; what holds instead is that the tests on both sides
// assert the same literal bytes, and one of them reads the other language's
// source to say so. See record_test.go's storedRecord* constants.
package routing

import (
	"encoding/json"
	"strings"

	schedulerv1 "agentenv/services/api/proto"
)

// Node is where a sandbox is, as a routing record names it.
//
// 🔴 The json tags are the stored format. A record written by any build must
// decode in every other, so these names are not free to change — renaming one
// blanks the binding table across an upgrade rather than failing loudly.
type Node struct {
	ID       string `json:"node_id"`
	Endpoint string `json:"endpoint"`
	// PodName is the name of the pod currently serving this node, which is the
	// identity a node still reports itself under until its own pod restarts
	// onto the build that reports the machine's name instead. Carrying it lets
	// the registry recognise such a heartbeat rather than rejecting it as an
	// unknown node.
	//
	// Only the *current* pod is aliased — a stale process reporting some
	// earlier pod's name is still refused — and the alias never shadows a real
	// node, so keeping it once the fleet is upgraded costs nothing and leaves
	// the next upgrade equally seamless.
	PodName string `json:"pod_name,omitempty"`
}

// ToProto is the one conversion from a stored node to the wire node.
//
// 🔴 PodName is deliberately absent: it is an identity the registry uses to
// recognise a heartbeat, not an address anything routes to, and putting it in a
// lookup answer would invite a caller to forward to it.
func (n Node) ToProto() *schedulerv1.Node {
	return &schedulerv1.Node{
		NodeId:   n.ID,
		Endpoint: n.Endpoint,
	}
}

// 🔴 There is no inverse. NodeFromProto used to sit here, and nothing outside
// a test of itself ever called it: this package decodes a stored record and
// converts it *towards* the wire, never back. A wire node has no pod name and
// no stored form to return to, so the only thing the reverse could produce is a
// half-populated Node that reads like a stored one.
//
// Record is one routing projection: where a sandbox is, and which incarnation
// of it is there.
type Record struct {
	Node Node `json:"node"`
	// ExecutionID is omitted when empty so a record written by this build and
	// one written before the field existed decode to the same thing: unknown.
	ExecutionID string `json:"execution_id,omitempty"`
	// State says whether the record names a runtime its node has acknowledged.
	// The writer omits it once the node has, so an empty state is confirmed and
	// is what every record written before the field existed carries.
	State string `json:"state,omitempty"`
}

// The states a stored record may name, spelled as `aenv-api`'s BindingState
// serializes them (src/binding_store/record.rs).
//
// A confirmed record is written with the field absent, so stateConfirmed is
// only ever read, never a value the writer emits.
const (
	stateConfirmed = "confirmed"
	stateStarting  = "starting"
)

// BindingKey is where a sandbox's record lives. Both sides derive the key from
// this function rather than from a format string of their own.
func BindingKey(prefix string, sandboxID string) string {
	return prefix + ":sandbox:" + sandboxID
}

// 🔴 The reverse index's key, NodeIndexKey, is not here. It names the set of
// sandboxes a node holds, which only a writer maintains — and this module holds
// no writer. `aenv-api` keeps its own (`src/binding_store/record.rs`'s
// `node_index_key`, pinned to the same `{prefix}:node:{node_id}` format by
// `keys_match_gos_format`), and a second unused copy here is a format free to
// drift with nothing reading either one.
//
// DefaultKeyPrefix is the prefix both processes use unless told otherwise.
const DefaultKeyPrefix = "agentenv:scheduler:bindings"

// ParseRecord decodes a stored record, and says whether it names somewhere to
// forward to.
//
// 🔴 A missing execution_id is an empty incarnation, not a decode failure. That
// is what every record written before the field existed looks like, and
// refusing them would blank the binding table on the first upgrade. A record
// with no node id is a different matter: it names nowhere, so it is not an
// answer at all.
//
// 🔴 A record that is not confirmed names an assignment, not a runtime. The
// writer installs a `starting` record when it picks a node for a create and
// rewrites it once that node acknowledges the sandbox, so forwarding to the
// node it names would put data-plane traffic at a VM that may not exist yet.
// Refusing it here is a projection miss, which sends the request to the half
// that owns the decision — and `lookup_node` refuses the same record there,
// as `unavailable_starting`. The two ends give one verdict because only one of
// them decides it.
//
// Any other non-empty state is refused for the same reason `aenv-api` refuses
// it: its state is an enum, so a value this build does not know fails that
// decode outright, and a gateway that forwarded what the writer's own reader
// rejects would be the looser of the two.
func ParseRecord(raw []byte) (Record, bool) {
	var record Record
	if err := json.Unmarshal(raw, &record); err != nil {
		return Record{}, false
	}
	if record.State != "" && record.State != stateConfirmed {
		return Record{}, false
	}
	node := Node{
		ID:       strings.TrimSpace(record.Node.ID),
		Endpoint: strings.TrimSpace(record.Node.Endpoint),
		PodName:  strings.TrimSpace(record.Node.PodName),
	}
	if node.ID == "" || node.Endpoint == "" {
		return Record{}, false
	}
	return Record{
		Node:        node,
		ExecutionID: strings.TrimSpace(record.ExecutionID),
		State:       record.State,
	}, true
}

// 🔴 There is no encoder here, and its absence is the point.
//
// MarshalRecord used to be "the one encoder", written when `services/scheduler`
// wrote these keys and its Lua heartbeat script had to splice a record together
// against a shape this file defined. That process is deleted; `aenv-api` writes
// every one of these keys now (`src/binding_store/record.rs`'s
// `marshal_record`), and Go only ever reads them.
//
// A leftover encoder is worse than none. Its only callers were tests, which
// then encoded and decoded with the same package and so proved nothing about
// the format the cluster actually stores; and the next writer to need one in Go
// would reach for it rather than noticing that writing these keys from two
// processes is the problem. The tests now feed literals, pinned against
// `marshal_record`'s own output — see record_test.go's storedRecord* constants.
//
// Synthesize turns a record into the lookup answer the scheduler would have
// given for it, so a gateway reading the projection directly returns the same
// thing it would have been told.
//
// The two fields a flat record does not carry are both constants at the one
// exit this stands in for — the binding hit in the scheduler's lookup, which
// always answers BOUND and always names no origin node:
//
//   - location:       BOUND
//   - origin_node_id: ""
//
// 🔴 The incarnation travels through untouched rather than being normalised
// here. Normalisation belongs to the write path, which already does it, and
// doing it a second time on the read side would make this and the scheduler's
// own answer differ for exactly the inputs where it matters.
func Synthesize(record Record) *schedulerv1.LookupNodeResponse {
	return &schedulerv1.LookupNodeResponse{
		Node:               record.Node.ToProto(),
		Location:           schedulerv1.SandboxLocation_SANDBOX_LOCATION_BOUND,
		OriginNodeId:       "",
		ExecutionId:        record.ExecutionID,
		ExecutionAuthority: AuthorityFor(record.ExecutionID),
	}
}
