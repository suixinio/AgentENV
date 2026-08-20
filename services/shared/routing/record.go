// Package routing owns the routing projection: the record a scheduler writes
// when it learns where a sandbox is, and everything needed to read one back.
//
// 🔴 It exists because two processes read the same bytes. The scheduler writes
// the record and answers lookups from it; the gateway reads it directly and
// synthesises the answer the scheduler would have given. Go's internal rule
// keeps the gateway out of services/scheduler/internal, so without a shared
// package the record's shape, its key, and the rule turning an incarnation into
// an authority would each exist twice — and two copies of a wire format drift,
// which here means the gateway routing on a field the scheduler stopped
// writing.
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

// NodeFromProto is the inverse, and tolerates a nil message the way every
// generated getter does.
func NodeFromProto(node *schedulerv1.Node) Node {
	if node == nil {
		return Node{}
	}
	return Node{
		ID:       node.GetNodeId(),
		Endpoint: node.GetEndpoint(),
	}
}

// Record is one routing projection: where a sandbox is, and which incarnation
// of it is there.
type Record struct {
	Node Node `json:"node"`
	// ExecutionID is omitted when empty so a record written by this build and
	// one written before the field existed decode to the same thing: unknown.
	ExecutionID string `json:"execution_id,omitempty"`
}

// BindingKey is where a sandbox's record lives. Both sides derive the key from
// this function rather than from a format string of their own.
func BindingKey(prefix string, sandboxID string) string {
	return prefix + ":sandbox:" + sandboxID
}

// NodeIndexKey is the reverse index: the set of sandboxes a node holds.
func NodeIndexKey(prefix string, nodeID string) string {
	return prefix + ":node:" + nodeID
}

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
func ParseRecord(raw []byte) (Record, bool) {
	var record Record
	if err := json.Unmarshal(raw, &record); err != nil {
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
	return Record{Node: node, ExecutionID: strings.TrimSpace(record.ExecutionID)}, true
}

// MarshalRecord is the one encoder. The scheduler's Lua scripts splice a record
// together in place for the heartbeat path, and they are held to producing
// exactly what this emits — a golden test in the scheduler package asserts it.
func MarshalRecord(node Node, executionID string) (string, error) {
	data, err := json.Marshal(Record{Node: node, ExecutionID: executionID})
	if err != nil {
		return "", err
	}
	return string(data), nil
}

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
