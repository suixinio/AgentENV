package scheduler

import (
	"errors"
	"sort"
	"strings"
	"sync"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	"google.golang.org/protobuf/proto"
)

type NodeRegistry interface {
	Snapshot(allowLingering bool) []Node
	Contains(node Node) bool
	Resolve(nodeID string) (Node, bool)
	Heartbeat(req *schedulerv1.HeartbeatRequest, now time.Time) (Node, string, error)
	ListObserved(clusterID string, now time.Time) []*schedulerv1.ObservedNode
	ListP2pPeers(clusterID string, backend string, excludeNodeID string, now time.Time) []*schedulerv1.P2PPeer
	FilterP2pPeers(clusterID string, backend string, nodeIDs []string, excludeNodeID string, now time.Time) []*schedulerv1.P2PPeer
	GetObserved(nodeID string, clusterID string, now time.Time) (*schedulerv1.ObservedNode, bool)
	// PeekObserved returns the latest heartbeat-reported NodeSnapshot for a node.
	// Unlike GetObserved, it does not derive status from discovery state or TTL,
	// and returns only the raw snapshot suitable for scheduling decisions.
	// Returns nil if the node has never sent a heartbeat.
	PeekObserved(nodeID string) *schedulerv1.NodeSnapshot
	// RosterOf returns the sandbox roster a node reported in its last
	// heartbeat, and when it reported it.
	RosterOf(nodeID string) ([]RosterEntry, time.Time, bool)
	// NodesHolding returns every node whose last heartbeat listed this
	// sandbox. More than one is normal during a cross-node takeover: the
	// origin keeps its paused record until its own reconciliation drops it.
	NodesHolding(sandboxID string) []string
	// RostersInCluster returns one roster per node this scheduler answers for
	// in a cluster, sorted by node id.
	RostersInCluster(clusterID string) []Roster
	UnregisterObserved(nodeID string, serviceInstanceID string) error
}

// Roster is one node's heartbeat-reported sandbox list.
//
// Heartbeats have always carried this list, and until now its only consumer was
// BindingStore.ReconcileNode, which folded it into sandbox-to-node bindings and
// dropped the rest. Keeping it means the scheduler can answer "what does this
// node say it holds" — which is the other half of every reconciliation against
// the paused registry, and the only thing that can cover a binding whose TTL
// lapsed between heartbeats.
//
// A zero LastSeen means the node is known to discovery but has never sent a
// heartbeat. That is a real and reportable state, not a placeholder: a machine
// that came up and never checked in is exactly the one an operator needs to see.
type Roster struct {
	NodeID string
	// Entries carries the incarnation each sandbox is reported under alongside
	// its id. 🔴 The pair travels together: a roster of bare ids can say where
	// a sandbox is but not whether what is there is the copy the cluster
	// believes in, and that is the difference between routing and guessing.
	Entries  []RosterEntry
	LastSeen time.Time
}

// SandboxIDs is the ids alone, for the consumers that only need the set.
func (r Roster) SandboxIDs() []string {
	ids := make([]string, 0, len(r.Entries))
	for _, entry := range r.Entries {
		ids = append(ids, entry.SandboxID)
	}
	return ids
}

var (
	ErrServiceInstanceMismatch = errors.New("service instance mismatch")
	ErrNodeNotInRegistry       = errors.New("node is not in scheduler node list")
	defaultObservedReportTTL   = 30 * time.Second
)

type observedNodeRecord struct {
	node        *schedulerv1.ObservedNode
	p2pEndpoint *schedulerv1.P2PEndpoint
	reportTTL   time.Duration
	// entries is the roster from this node's last heartbeat, normalised.
	entries []RosterEntry
	// lastSeen duplicates node.LastSeenUnixMs as a time.Time so roster
	// freshness is decided without a millisecond round trip.
	lastSeen time.Time
}

type AtomicNodeRegistry struct {
	mu        sync.RWMutex
	nodesByID map[string]Node
	// Maps a node's previous identity (its pod name) to its current one, so a
	// heartbeat sent under the old name during a fleet upgrade is still
	// recognised instead of being rejected as an unknown node. Without it the
	// scheduler stops accepting a node's heartbeats the moment discovery starts
	// naming nodes after the machine, and keeps rejecting them until that
	// node's pod restarts — which, behind a drain that waits for every sandbox
	// to be parked, can be hours. The node's bindings expire meanwhile and
	// every sandbox on it answers 404.
	aliasToID        map[string]string
	lingeringIDs     map[string]bool
	observedTTL      time.Duration
	observed         map[string]observedNodeRecord
	cpuIntersection  map[string]string
	intersectionSent map[string]bool
	// sandboxHolders is the reverse of the rosters: sandbox id -> the nodes
	// that reported holding it. Maintained on every roster change so a lookup
	// costs one map hit rather than a scan of every node's roster.
	sandboxHolders map[string]map[string]struct{}
}

func NewAtomicNodeRegistry(nodes []Node, observedTTL time.Duration) *AtomicNodeRegistry {
	ttl := defaultObservedReportTTL
	if observedTTL > 0 {
		ttl = observedTTL
	}

	registry := &AtomicNodeRegistry{
		nodesByID:        make(map[string]Node),
		aliasToID:        make(map[string]string),
		lingeringIDs:     make(map[string]bool),
		observedTTL:      ttl,
		observed:         make(map[string]observedNodeRecord),
		cpuIntersection:  make(map[string]string),
		intersectionSent: make(map[string]bool),
		sandboxHolders:   make(map[string]map[string]struct{}),
	}
	registry.Set(nodes, nil)
	return registry
}

// Snapshot returns discovered nodes filtered by their derived status.
// See NodeStatus in scheduler.proto for the full status derivation table.
func (r *AtomicNodeRegistry) Snapshot(allowLingering bool) []Node {
	r.mu.RLock()
	result := make([]Node, 0, len(r.nodesByID))
	for _, node := range r.nodesByID {
		if r.lingeringIDs[node.ID] && !allowLingering {
			continue
		}
		result = append(result, node)
	}
	r.mu.RUnlock()
	sort.Slice(result, func(i, j int) bool {
		return result[i].ID < result[j].ID
	})
	return result
}

func (r *AtomicNodeRegistry) Contains(node Node) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	known, ok := r.nodesByID[r.canonicalIDLocked(node.ID)]
	return ok && known.Endpoint == node.Endpoint
}

func (r *AtomicNodeRegistry) Resolve(nodeID string) (Node, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	node, ok := r.nodesByID[r.canonicalIDLocked(nodeID)]
	return node, ok
}

// canonicalIDLocked maps whatever identity a caller used onto the one discovery
// currently uses for that node. Unknown identities are returned unchanged, so
// the caller still gets its "not in the registry" answer.
//
// The order is the guarantee, not an optimisation: a real node is always
// resolved as itself, so an alias can never shadow one and attribute one
// machine's heartbeat to another.
func (r *AtomicNodeRegistry) canonicalIDLocked(nodeID string) string {
	if _, ok := r.nodesByID[nodeID]; ok {
		return nodeID
	}
	if canonical, ok := r.aliasToID[nodeID]; ok {
		return canonical
	}

	return nodeID
}

// Set replaces the discovered node list. active nodes are serving and not
// terminating; lingering nodes are serving but terminating (graceful shutdown).
func (r *AtomicNodeRegistry) Set(active []Node, lingering []Node) {
	byID := make(map[string]Node, len(active)+len(lingering))
	for _, node := range active {
		byID[node.ID] = node
	}
	for _, node := range lingering {
		byID[node.ID] = node
	}

	lIDs := make(map[string]bool, len(lingering))
	for _, node := range lingering {
		lIDs[node.ID] = true
	}

	aliases := make(map[string]string)
	for _, node := range byID {
		if node.PodName == "" || node.PodName == node.ID {
			continue
		}
		aliases[node.PodName] = node.ID
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.nodesByID = byID
	r.aliasToID = aliases
	r.lingeringIDs = lIDs
	affectedClusters := make(map[string]struct{})
	for nodeID, record := range r.observed {
		if _, ok := byID[nodeID]; ok {
			continue
		}
		if clusterID := record.node.GetClusterId(); clusterID != "" {
			affectedClusters[clusterID] = struct{}{}
		}
		r.clearRosterLocked(nodeID)
		delete(r.observed, nodeID)
		delete(r.intersectionSent, nodeID)
	}
	for clusterID := range affectedClusters {
		r.invalidateIntersectionLocked(clusterID)
	}
}

func (r *AtomicNodeRegistry) Heartbeat(req *schedulerv1.HeartbeatRequest, now time.Time) (Node, string, error) {
	nowMs := now.UTC().UnixMilli()

	machineInfo := cloneMachineInfo(req.GetMachineInfo())

	r.mu.Lock()
	defer r.mu.Unlock()
	// Everything below keys off the canonical ID, never the one the node sent:
	// a node mid-upgrade still reports its pod name, and recording it under
	// that would give the same machine two observed identities.
	nodeID := r.canonicalIDLocked(req.GetNodeId())
	node, ok := r.nodesByID[nodeID]
	if !ok {
		return Node{}, "", ErrNodeNotInRegistry
	}

	prevCPU, existed := "", false
	if prev, ok := r.observed[nodeID]; ok {
		existed = true
		prevCPU = prev.node.GetMachineInfo().GetCpuConfigJson()
		if machineInfo != nil && machineInfo.CpuConfigJson == "" {
			machineInfo.CpuConfigJson = prevCPU
		}
	}

	record := observedNodeRecord{
		node: &schedulerv1.ObservedNode{
			NodeId:            nodeID,
			Endpoint:          node.Endpoint,
			ClusterId:         req.GetClusterId(),
			ServiceInstanceId: req.GetServiceInstanceId(),
			Version:           req.GetVersion(),
			Commit:            req.GetCommit(),
			MachineInfo:       machineInfo,
			LastSeenUnixMs:    nowMs,
			Snapshot:          cloneSnapshot(req.GetSnapshot()),
		},
		p2pEndpoint: cloneP2PEndpoint(req.GetP2PEndpoint()),
		reportTTL:   r.observedTTL,
		entries:     normalizeHeartbeatRoster(req),
		lastSeen:    now,
	}
	r.applyRosterLocked(nodeID, record.entries)
	if record.node.Snapshot.GetReportedAtUnixMs() == 0 {
		record.node.Snapshot.ReportedAtUnixMs = nowMs
	}
	if record.node.Snapshot.GetStatus() == schedulerv1.NodeStatus_NODE_STATUS_UNSPECIFIED {
		record.node.Snapshot.Status = schedulerv1.NodeStatus_NODE_STATUS_CONNECTING
	}

	r.observed[nodeID] = record

	clusterID := req.GetClusterId()
	if !existed || (machineInfo != nil && machineInfo.GetCpuConfigJson() != prevCPU) {
		r.invalidateIntersectionLocked(clusterID)
	}
	if _, computed := r.cpuIntersection[clusterID]; !computed {
		if r.allConfigsReadyLocked(clusterID) {
			if result := r.computeIntersectionLocked(clusterID); result != "" {
				r.cpuIntersection[clusterID] = result
			}
		}
	}

	if intersection, ok := r.cpuIntersection[clusterID]; ok && !r.intersectionSent[nodeID] {
		r.intersectionSent[nodeID] = true
		return node, intersection, nil
	}
	return node, "", nil
}

func (r *AtomicNodeRegistry) invalidateIntersectionLocked(clusterID string) {
	delete(r.cpuIntersection, clusterID)
	for nodeID, rec := range r.observed {
		if rec.node.GetClusterId() == clusterID {
			delete(r.intersectionSent, nodeID)
		}
	}
}

func (r *AtomicNodeRegistry) allConfigsReadyLocked(clusterID string) bool {
	total, withConfig := 0, 0
	for _, rec := range r.observed {
		if rec.node.GetClusterId() != clusterID {
			continue
		}
		total++
		if rec.node.GetMachineInfo().GetCpuConfigJson() != "" {
			withConfig++
		}
	}
	return total > 0 && withConfig == total
}

func (r *AtomicNodeRegistry) computeIntersectionLocked(clusterID string) string {
	var jsons []string
	for _, rec := range r.observed {
		if rec.node.GetClusterId() != clusterID {
			continue
		}
		if j := rec.node.GetMachineInfo().GetCpuConfigJson(); j != "" {
			jsons = append(jsons, j)
		}
	}
	result, err := IntersectCpuConfigs(jsons)
	if err != nil {
		return ""
	}
	return result
}

func (r *AtomicNodeRegistry) ListObserved(clusterID string, now time.Time) []*schedulerv1.ObservedNode {
	nowMs := now.UTC().UnixMilli()
	trimmedCluster := strings.TrimSpace(clusterID)

	r.mu.RLock()
	defer r.mu.RUnlock()
	nodes := make([]*schedulerv1.ObservedNode, 0, len(r.observed))
	for _, record := range r.observed {
		if trimmedCluster != "" && record.node.GetClusterId() != trimmedCluster {
			continue
		}
		nodes = append(nodes, r.deriveObservedNodeViewLocked(record, nowMs))
	}

	return nodes
}

func (r *AtomicNodeRegistry) ListP2pPeers(clusterID string, backend string, excludeNodeID string, now time.Time) []*schedulerv1.P2PPeer {
	return r.filterP2pPeers(clusterID, backend, nil, excludeNodeID, now)
}

func (r *AtomicNodeRegistry) FilterP2pPeers(clusterID string, backend string, nodeIDs []string, excludeNodeID string, now time.Time) []*schedulerv1.P2PPeer {
	allowed := make(map[string]struct{}, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		allowed[nodeID] = struct{}{}
	}
	if len(allowed) == 0 {
		return nil
	}
	return r.filterP2pPeers(clusterID, backend, allowed, excludeNodeID, now)
}

func (r *AtomicNodeRegistry) filterP2pPeers(clusterID string, backend string, allowed map[string]struct{}, excludeNodeID string, now time.Time) []*schedulerv1.P2PPeer {
	nowMs := now.UTC().UnixMilli()
	trimmedCluster := strings.TrimSpace(clusterID)
	trimmedBackend := strings.TrimSpace(backend)
	trimmedExcludeNodeID := strings.TrimSpace(excludeNodeID)

	r.mu.RLock()
	defer r.mu.RUnlock()
	peers := make([]*schedulerv1.P2PPeer, 0, len(r.observed))
	for _, record := range r.observed {
		if trimmedCluster != "" && record.node.GetClusterId() != trimmedCluster {
			continue
		}
		node := r.deriveObservedNodeViewLocked(record, nowMs)
		if len(allowed) > 0 {
			if _, ok := allowed[node.GetNodeId()]; !ok {
				continue
			}
		}
		if node.GetNodeId() == trimmedExcludeNodeID {
			continue
		}
		if node.GetSnapshot().GetStatus() != schedulerv1.NodeStatus_NODE_STATUS_READY {
			continue
		}
		endpoint := record.p2pEndpoint
		if endpoint.GetBackend() == "" || endpoint.GetAddress() == "" {
			continue
		}
		if trimmedBackend != "" && endpoint.GetBackend() != trimmedBackend {
			continue
		}
		peers = append(peers, &schedulerv1.P2PPeer{
			NodeId:   node.GetNodeId(),
			Endpoint: cloneP2PEndpoint(endpoint),
		})
	}

	return peers
}

func (r *AtomicNodeRegistry) GetObserved(nodeID string, clusterID string, now time.Time) (*schedulerv1.ObservedNode, bool) {
	nowMs := now.UTC().UnixMilli()
	trimmedCluster := strings.TrimSpace(clusterID)

	r.mu.RLock()
	defer r.mu.RUnlock()
	record, ok := r.observed[nodeID]
	if !ok {
		return nil, false
	}
	if trimmedCluster != "" && record.node.GetClusterId() != trimmedCluster {
		return nil, false
	}

	return r.deriveObservedNodeViewLocked(record, nowMs), true
}

func (r *AtomicNodeRegistry) PeekObserved(nodeID string) *schedulerv1.NodeSnapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()
	record, ok := r.observed[nodeID]
	if !ok || record.node == nil {
		return nil
	}
	snapshot := record.node.GetSnapshot()
	if snapshot == nil {
		return nil
	}
	return cloneSnapshot(snapshot)
}

func (r *AtomicNodeRegistry) UnregisterObserved(nodeID string, serviceInstanceID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	record, ok := r.observed[nodeID]
	if !ok {
		return nil
	}
	if record.node.GetServiceInstanceId() != serviceInstanceID {
		return ErrServiceInstanceMismatch
	}

	clusterID := record.node.GetClusterId()
	r.clearRosterLocked(nodeID)
	delete(r.observed, nodeID)
	r.invalidateIntersectionLocked(clusterID)
	return nil
}

// RosterOf returns a copy of a node's last reported roster.
func (r *AtomicNodeRegistry) RosterOf(nodeID string) ([]RosterEntry, time.Time, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	record, ok := r.observed[nodeID]
	if !ok {
		return nil, time.Time{}, false
	}
	return append([]RosterEntry(nil), record.entries...), record.lastSeen, true
}

// NodesHolding returns the nodes whose last heartbeat listed this sandbox,
// sorted by node id.
func (r *AtomicNodeRegistry) NodesHolding(sandboxID string) []string {
	sandboxID = strings.TrimSpace(sandboxID)
	if sandboxID == "" {
		return nil
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	holders, ok := r.sandboxHolders[sandboxID]
	if !ok {
		return nil
	}
	nodeIDs := make([]string, 0, len(holders))
	for nodeID := range holders {
		nodeIDs = append(nodeIDs, nodeID)
	}
	sort.Strings(nodeIDs)
	return nodeIDs
}

// RostersInCluster returns one roster per node this scheduler answers for in
// the given cluster, sorted by node id. An empty clusterID means no filter,
// which is the right answer for a database that serves a single cluster — and
// the same condition the registry reader applies to its own SQL.
//
// Two things it deliberately does that a plain "list what we observed" would
// not:
//
// A node discovery knows about but that has never sent a heartbeat is included,
// with a zero LastSeen and no sandboxes. Reporting nothing for it would hide
// the one node an operator most needs to see, and it would make the "never
// reported" branch of every consumer unreachable in production.
//
// A node whose heartbeat named another cluster is excluded even though
// discovery knows it, because it has answered — just not to us.
func (r *AtomicNodeRegistry) RostersInCluster(clusterID string) []Roster {
	wanted := normalizeClusterID(clusterID)

	r.mu.RLock()
	defer r.mu.RUnlock()

	rosters := make([]Roster, 0, len(r.observed)+len(r.nodesByID))
	for nodeID, record := range r.observed {
		if wanted != "" && normalizeClusterID(record.node.GetClusterId()) != wanted {
			continue
		}
		rosters = append(rosters, Roster{
			NodeID:   nodeID,
			Entries:  append([]RosterEntry(nil), record.entries...),
			LastSeen: record.lastSeen,
		})
	}
	for nodeID := range r.nodesByID {
		if _, reported := r.observed[nodeID]; reported {
			continue
		}
		rosters = append(rosters, Roster{NodeID: nodeID})
	}
	sort.Slice(rosters, func(i, j int) bool {
		return rosters[i].NodeID < rosters[j].NodeID
	})
	return rosters
}

// normalizeClusterID puts two cluster ids in a comparable form. Both sides are
// UUID text that travelled through a config file and an environment variable,
// and a difference in case or padding between them would silently empty the
// roster side of every comparison.
func normalizeClusterID(clusterID string) string {
	return strings.ToLower(strings.TrimSpace(clusterID))
}

// applyRosterLocked moves a node from its previous roster to a new one,
// keeping the reverse index in step. r.mu must be held by the caller.
func (r *AtomicNodeRegistry) applyRosterLocked(nodeID string, roster []RosterEntry) {
	next := make(map[string]struct{}, len(roster))
	for _, entry := range roster {
		next[entry.SandboxID] = struct{}{}
	}

	for _, entry := range r.observed[nodeID].entries {
		if _, ok := next[entry.SandboxID]; ok {
			continue
		}
		r.removeHolderLocked(entry.SandboxID, nodeID)
	}

	for sandboxID := range next {
		holders, ok := r.sandboxHolders[sandboxID]
		if !ok {
			holders = make(map[string]struct{}, 1)
			r.sandboxHolders[sandboxID] = holders
		}
		holders[nodeID] = struct{}{}
	}
}

// clearRosterLocked drops a node from the reverse index entirely. r.mu must be
// held by the caller.
func (r *AtomicNodeRegistry) clearRosterLocked(nodeID string) {
	for _, entry := range r.observed[nodeID].entries {
		r.removeHolderLocked(entry.SandboxID, nodeID)
	}
}

func (r *AtomicNodeRegistry) removeHolderLocked(sandboxID string, nodeID string) {
	holders, ok := r.sandboxHolders[sandboxID]
	if !ok {
		return
	}
	delete(holders, nodeID)
	if len(holders) == 0 {
		delete(r.sandboxHolders, sandboxID)
	}
}

// normalizeHeartbeatRoster is the one place a heartbeat's roster becomes the
// scheduler's, and the only place the two generations of the field are
// reconciled.
//
// 🔴 It does not count anything. The service layer counts, once, on the same
// answer — this is called from the node registry as well, and a metric
// incremented in both would report twice as many old nodes as there are.
func normalizeHeartbeatRoster(req *schedulerv1.HeartbeatRequest) []RosterEntry {
	entries, _ := rosterFromHeartbeat(req)
	return entries
}

// rosterFromHeartbeat collapses the two generations of the roster field into
// one shape, and says which one it used.
//
//	roster present                  → use it
//	roster empty, sandbox_ids present → use those, with no incarnations
//	both empty                        → a genuinely empty roster
//
// 🔴 The fallback is not politeness towards old builds, it is the difference
// between a rolling upgrade and an outage. Nodes are a DaemonSet and roll one
// at a time, so a scheduler that only read the new field would see an empty
// roster from every node it has not reached yet — and an empty roster is not a
// degraded report here, it is "this node holds nothing", which deletes every
// binding that node owns. The sandboxes that then answer nothing are the ones
// that have never been paused, because those have no registry row to fall back
// to. The field goes when heartbeat_legacy_roster_total has been zero across a
// release, not before.
func rosterFromHeartbeat(req *schedulerv1.HeartbeatRequest) (entries []RosterEntry, legacy bool) {
	if roster := req.GetRoster(); len(roster) > 0 {
		out := make([]RosterEntry, 0, len(roster))
		seen := make(map[string]struct{}, len(roster))
		for _, item := range roster {
			sandboxID := strings.TrimSpace(item.GetSandboxId())
			if sandboxID == "" {
				continue
			}
			if _, ok := seen[sandboxID]; ok {
				continue
			}
			seen[sandboxID] = struct{}{}
			out = append(out, RosterEntry{
				SandboxID:   sandboxID,
				ExecutionID: normalizeExecutionID(item.GetExecutionId()),
			})
		}
		if len(out) == 0 {
			return nil, false
		}
		return out, false
	}

	legacyIDs := req.GetSandboxIds() //nolint:staticcheck // the deprecated field is the rollout fallback; see the note above.
	if len(legacyIDs) == 0 {
		return nil, false
	}
	out := make([]RosterEntry, 0, len(legacyIDs))
	seen := make(map[string]struct{}, len(legacyIDs))
	for _, sandboxID := range legacyIDs {
		sandboxID = strings.TrimSpace(sandboxID)
		if sandboxID == "" {
			continue
		}
		if _, ok := seen[sandboxID]; ok {
			continue
		}
		seen[sandboxID] = struct{}{}
		out = append(out, RosterEntry{SandboxID: sandboxID})
	}
	if len(out) == 0 {
		return nil, false
	}
	return out, true
}

// normalizeExecutionID trims, checks the shape, and lower-cases.
//
// 🔴 A value that is not a canonical uuid is dropped rather than carried: the
// arbitration orders these as strings, so anything that is not the shape it
// expects would order unpredictably against everything else. The roster entry
// itself is kept — losing a route to avoid an unfenced one is a bad trade — and
// the drop is counted, because narrowing something without saying so is how a
// fleet ends up with fencing that is not running.
//
// 🔴 The lower-casing is the part that looks cosmetic and is not: in ASCII
// '0'-'9' < 'A'-'F' < 'a'-'f', so one upper-case id reverses the comparison and
// the older incarnation wins.
func normalizeExecutionID(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		recordRosterDropped("no_execution")
		return ""
	}
	if !isCanonicalUUIDText(trimmed) {
		recordRosterDropped("bad_uuid")
		return ""
	}
	return strings.ToLower(trimmed)
}

// isCanonicalUUIDText is the shape check, deliberately narrow: the ids come
// from a type whose Display is always canonical, so anything else on this path
// came from a caller this build does not recognise.
func isCanonicalUUIDText(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i := 0; i < 36; i++ {
		c := s[i]
		switch i {
		case 8, 13, 18, 23:
			if c != '-' {
				return false
			}
		default:
			isHex := (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
			if !isHex {
				return false
			}
		}
	}
	return true
}

// deriveObservedNodeViewLocked builds the external ObservedNode view for a
// heartbeat record, overriding the endpoint and status based on the current
// discovery state. See NodeStatus in scheduler.proto for the full derivation
// table. r.mu must be held by the caller.
func (r *AtomicNodeRegistry) deriveObservedNodeViewLocked(record observedNodeRecord, nowMs int64) *schedulerv1.ObservedNode {
	out := cloneObservedNode(record.node)
	if out.Snapshot == nil {
		out.Snapshot = &schedulerv1.NodeSnapshot{}
	}

	nodeID := out.GetNodeId()

	knownNode, inDiscovery := r.nodesByID[nodeID]
	isLingering := r.lingeringIDs[nodeID]

	if inDiscovery && strings.TrimSpace(knownNode.Endpoint) != "" {
		out.Endpoint = knownNode.Endpoint
	}

	ttl := record.reportTTL
	if ttl <= 0 {
		ttl = defaultObservedReportTTL
	}

	if out.GetLastSeenUnixMs() > 0 && nowMs-out.GetLastSeenUnixMs() > ttl.Milliseconds() {
		out.Snapshot.Status = schedulerv1.NodeStatus_NODE_STATUS_UNHEALTHY
	} else if !inDiscovery {
		out.Snapshot.Status = schedulerv1.NodeStatus_NODE_STATUS_CONNECTING
	} else if isLingering {
		out.Snapshot.Status = schedulerv1.NodeStatus_NODE_STATUS_LINGERING
	} else {
		// Active — keep the status reported by the node.
		if out.Snapshot.GetStatus() == schedulerv1.NodeStatus_NODE_STATUS_UNSPECIFIED {
			out.Snapshot.Status = schedulerv1.NodeStatus_NODE_STATUS_CONNECTING
		}
	}

	return out
}

func cloneObservedNode(node *schedulerv1.ObservedNode) *schedulerv1.ObservedNode {
	if node == nil {
		return &schedulerv1.ObservedNode{}
	}
	cloned, ok := proto.Clone(node).(*schedulerv1.ObservedNode)
	if ok {
		return cloned
	}
	return &schedulerv1.ObservedNode{}
}

func cloneSnapshot(snapshot *schedulerv1.NodeSnapshot) *schedulerv1.NodeSnapshot {
	if snapshot == nil {
		return &schedulerv1.NodeSnapshot{}
	}
	cloned, ok := proto.Clone(snapshot).(*schedulerv1.NodeSnapshot)
	if ok {
		return cloned
	}
	return &schedulerv1.NodeSnapshot{}
}

func cloneMachineInfo(machine *schedulerv1.MachineInfo) *schedulerv1.MachineInfo {
	if machine == nil {
		return nil
	}
	cloned, ok := proto.Clone(machine).(*schedulerv1.MachineInfo)
	if ok {
		return cloned
	}
	return nil
}

func cloneP2PEndpoint(endpoint *schedulerv1.P2PEndpoint) *schedulerv1.P2PEndpoint {
	if endpoint == nil {
		return nil
	}
	cloned, ok := proto.Clone(endpoint).(*schedulerv1.P2PEndpoint)
	if ok {
		return cloned
	}
	return nil
}
