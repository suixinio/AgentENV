package scheduler

import (
	"strings"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
)

const defaultBindingTTL = 30 * time.Second
const defaultArtifactStoreCapacity = 1_000_000

type BindingStore interface {
	Get(sandboxID string, now time.Time) (Binding, bool, error)
	Record(sandboxID string, binding Binding, now time.Time) error
	ReconcileNode(node Node, roster []RosterEntry, now time.Time) error
	// Delete removes a sandbox's routing projection, but only if the record
	// still names the incarnation the event came from.
	//
	// 🔴 The guard is the whole point. A pause event that arrives late, for a
	// sandbox id that has since been resumed elsewhere under a new
	// incarnation, would otherwise tear down the live record and leave the
	// running sandbox unroutable until the next heartbeat reinstalls it.
	//
	// 🔴 executionID is required and must already be normalised. An empty one
	// is refused by the caller rather than being treated as "delete anything",
	// which would be the unguarded delete this exists to prevent.
	Delete(sandboxID string, executionID string, now time.Time) (BindingDeleteOutcome, error)
}

// BindingDeleteOutcome is what a guarded delete did. Closed set: every return
// below maps onto exactly one, so the metric's series sum to the call count.
type BindingDeleteOutcome string

const (
	// BindingDeleteAbsent: nothing held this sandbox. Ordinary — a delete
	// event can arrive after the heartbeat reconciliation already dropped the
	// record.
	BindingDeleteAbsent BindingDeleteOutcome = "noop_absent"
	// BindingDeleteDeleted: the record named the same incarnation the event
	// did, and is gone.
	BindingDeleteDeleted BindingDeleteOutcome = "deleted"
	// BindingDeleteUnknownIncumbent: the record named no incarnation at all,
	// and is gone.
	//
	// 🔴 Deleted, not refused — and this is deliberately the opposite of what
	// the write path does with an empty incumbent. An empty incumbent means
	// "unknown", not "somebody else's": the write path lets any named
	// challenger take it for exactly that reason. Refusing here instead would
	// mean a create that recorded no incarnation, followed by a delete two
	// seconds later, leaves a long-lived record pointing at a sandbox that no
	// longer exists.
	BindingDeleteUnknownIncumbent BindingDeleteOutcome = "deleted_unknown_incumbent"
	// BindingDeleteRejectedStale: the record names a different incarnation.
	// The guard fired; the live record survives.
	BindingDeleteRejectedStale BindingDeleteOutcome = "rejected_stale"
)

// Binding is where a sandbox is and which incarnation of it is there.
//
// 🔴 The two halves travel together and neither is optional. The node id says
// where to forward; the incarnation id says whether what is there is the one
// the cluster believes in. Carrying only the first is how a node that has been
// superseded goes on being routed to — it keeps reporting the sandbox, and the
// last report used to win.
type Binding struct {
	Node Node
	// ExecutionID is a lower-case canonical UUID v7, or empty.
	//
	// 🔴 Empty means "not known", not "no incarnation". It is what a record
	// written by a node too old to report one looks like, and the arbitration
	// below treats it as a record that may be replaced by anything but may
	// itself replace nothing that names an incarnation.
	ExecutionID string
	// ProjectionTTL is how long this record should live, as the node that owns
	// the sandbox computed it from its own lifetime ceiling.
	//
	// 🔴 Zero means "use the store's binding_ttl". It never means "never
	// expires": a record that outlives every path able to delete it is a route
	// pointing at a sandbox nobody can reach, and the failure looks exactly
	// like a cold cache from the outside.
	ProjectionTTL time.Duration
}

// RosterEntry is one sandbox in a node's heartbeat roster.
type RosterEntry struct {
	SandboxID string
	// ExecutionID is normalised on the way in: trimmed, checked for shape, and
	// lower-cased. Empty where the node did not report one.
	ExecutionID string
	// ProjectionTTL is the same budget Binding.ProjectionTTL carries, on the
	// repair path rather than the create path: a projection write that was
	// lost has to be reinstalled with the sandbox's real budget, not with the
	// receiver's default. Zero means "use the store's binding_ttl".
	ProjectionTTL time.Duration
}

// bindingDecision is what the arbitration did, as a closed set of metric
// labels. The empty value means "not arbitrated" — the rollback mode, which
// records nothing rather than recording a decision it did not make.
type bindingDecision string

const (
	// bindingInstalled: nothing held this sandbox, or what held it named no
	// incarnation and the challenger does.
	bindingInstalled bindingDecision = "installed"
	// bindingInstalledUnknown: nothing held it and the challenger names no
	// incarnation either. Installed, because a route with no fencing is still
	// better than no route — see the note in ReconcileNode.
	bindingInstalledUnknown bindingDecision = "installed_unknown"
	// bindingRefreshed: the same incarnation reporting again, which is what
	// every heartbeat of a healthy sandbox looks like.
	bindingRefreshed bindingDecision = "refreshed"
	// bindingSuperseded: a newer incarnation replacing an older one. The
	// ordinary outcome of a resume.
	bindingSuperseded bindingDecision = "superseded"
	// bindingRejectedOlder: an older incarnation trying to take the binding
	// back. 🔴 On a healthy cluster this is zero; anything else is either a
	// clock that went backwards or two live copies of one sandbox.
	bindingRejectedOlder bindingDecision = "rejected_older"
	// bindingRejectedUnknown: a record naming no incarnation trying to
	// displace one that does.
	bindingRejectedUnknown bindingDecision = "rejected_unknown"
)

// arbiter decides whether a challenger may take a binding, and says what it
// decided.
//
// 🔴 Three of them exist and exactly one is chosen when a store is built. Not
// one function with a mode argument: the "off" behaviour has to be the
// behaviour this had before arbitration existed, testable on its own terms,
// and a shared function with a branch in it can never be tested in either mode
// without also testing the branch.
type arbiter func(incumbent string, held bool, challenger string) (accept bool, decision bindingDecision)

// arbitrateFenced is the rule. Six cases, in the order they are reached.
//
// Comparison is lexicographic over lower-case canonical uuids, which for v7 is
// time order. Two properties hold it up: the ids are normalised at the entry
// points (upper-case hex would reverse the order, since '0'-'9' < 'A'-'F' <
// 'a'-'f'), and two incarnations of one sandbox can only be reported at the
// same time after a takeover, which requires a lease to have lapsed — at least
// thirty seconds apart, against clock skew measured in milliseconds.
func arbitrateFenced(incumbent string, held bool, challenger string) (bool, bindingDecision) {
	if !held || incumbent == "" {
		if challenger == "" {
			return true, bindingInstalledUnknown
		}
		return true, bindingInstalled
	}
	if challenger == "" {
		// 🔴 Refused. A node too old to report an incarnation must not be able
		// to take a sandbox back from one that does, which is precisely the
		// rolling-upgrade window where the old copy is the stale one.
		return false, bindingRejectedUnknown
	}
	switch {
	case challenger == incumbent:
		return true, bindingRefreshed
	case challenger > incumbent:
		return true, bindingSuperseded
	default:
		return false, bindingRejectedOlder
	}
}

// arbitrateObserving works out what arbitrateFenced would have decided and then
// writes anyway. It is the release's first step: the decisions become visible
// in metrics before any of them starts changing what is routed where.
func arbitrateObserving(incumbent string, held bool, challenger string) (bool, bindingDecision) {
	_, decision := arbitrateFenced(incumbent, held, challenger)
	return true, decision
}

// arbitrateOff is the rollback: whoever reported last wins, and nothing is
// counted. Reporting a decision here would be reporting one that was not made.
func arbitrateOff(string, bool, string) (bool, bindingDecision) {
	return true, ""
}

type ArtifactStore interface {
	Record(clusterID string, backend string, key string, nodeID string)
	Forget(clusterID string, backend string, key string, nodeID string)
	Lookup(clusterID string, backend string, key string) []string
	ForgetNode(nodeID string)
}

type bindingRecord struct {
	node        Node
	executionID string
	expiresAt   time.Time
}

type InMemoryBindingStore struct {
	mu          sync.Mutex
	bindingTTL  time.Duration
	bindings    map[string]bindingRecord
	nodeBinding map[string]map[string]struct{}
	// arbitrate is chosen once, here, and every write goes through it. 🔴 It
	// runs under s.mu, which is the whole point: a comparison made outside the
	// lock is a window a resume can install a new incarnation in.
	arbitrate arbiter
}

func NewInMemoryBindingStore(bindingTTL time.Duration) *InMemoryBindingStore {
	return NewInMemoryBindingStoreWithArbitration(bindingTTL, arbitrateFenced)
}

// NewInMemoryBindingStoreWithArbitration builds the store with one of the three
// arbiters. The mode is a construction-time decision, never a per-call one.
func NewInMemoryBindingStoreWithArbitration(bindingTTL time.Duration, arbitrate arbiter) *InMemoryBindingStore {
	if bindingTTL <= 0 {
		bindingTTL = defaultBindingTTL
	}
	if arbitrate == nil {
		arbitrate = arbitrateFenced
	}
	return &InMemoryBindingStore{
		bindingTTL:  bindingTTL,
		bindings:    make(map[string]bindingRecord),
		nodeBinding: make(map[string]map[string]struct{}),
		arbitrate:   arbitrate,
	}
}

func (s *InMemoryBindingStore) Get(sandboxID string, now time.Time) (Binding, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	record, ok := s.bindings[sandboxID]
	if !ok {
		return Binding{}, false, nil
	}
	if !record.expiresAt.After(now) {
		s.deleteLocked(sandboxID)
		return Binding{}, false, nil
	}
	return Binding{Node: record.node, ExecutionID: record.executionID}, true, nil
}

func (s *InMemoryBindingStore) Record(sandboxID string, binding Binding, now time.Time) error {
	sandboxID = strings.TrimSpace(sandboxID)
	if sandboxID == "" {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	// 🔴 Through the same arbitration as a heartbeat. A create or fork
	// response that landed on a node whose sandbox has since moved would
	// otherwise be a way in behind the rule — the one write left unguarded is
	// the one that undoes the others.
	decision := s.upsertLocked(sandboxID, binding, now, bindingSourceAssignment)
	recordBindingArbitration(bindingSourceAssignment, decision)
	return nil
}

// Delete is the guarded delete. See BindingStore.Delete for the rules; they are
// implemented identically here and in the Lua script, because this is the store
// the default build runs and that is the one the tests exercise.
func (s *InMemoryBindingStore) Delete(sandboxID string, executionID string, now time.Time) (BindingDeleteOutcome, error) {
	sandboxID = strings.TrimSpace(sandboxID)
	if sandboxID == "" {
		return BindingDeleteAbsent, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	record, ok := s.bindings[sandboxID]
	if !ok {
		return BindingDeleteAbsent, nil
	}
	if !record.expiresAt.After(now) {
		// Expired is absent everywhere else in this store, and it is absent
		// here too. Tidying it up on the way past costs nothing.
		s.deleteLocked(sandboxID)
		return BindingDeleteAbsent, nil
	}
	if record.executionID != "" && record.executionID != executionID {
		return BindingDeleteRejectedStale, nil
	}
	outcome := BindingDeleteDeleted
	if record.executionID == "" {
		outcome = BindingDeleteUnknownIncumbent
	}
	s.deleteLocked(sandboxID)
	return outcome, nil
}

func (s *InMemoryBindingStore) ReconcileNode(node Node, roster []RosterEntry, now time.Time) error {
	normalized := make(map[string]RosterEntry, len(roster))
	for _, entry := range roster {
		sandboxID := strings.TrimSpace(entry.SandboxID)
		if sandboxID == "" {
			continue
		}
		entry.SandboxID = sandboxID
		normalized[sandboxID] = entry
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if len(normalized) == 0 {
		if bindings, ok := s.nodeBinding[node.ID]; ok {
			for sandboxID := range bindings {
				s.deleteLocked(sandboxID)
			}
		}
		return nil
	}

	for sandboxID, entry := range normalized {
		binding := Binding{Node: node, ExecutionID: entry.ExecutionID, ProjectionTTL: entry.ProjectionTTL}
		decision := s.upsertLocked(sandboxID, binding, now, bindingSourceHeartbeat)
		recordBindingArbitration(bindingSourceHeartbeat, decision)
	}

	current := s.nodeBinding[node.ID]
	if len(current) == 0 {
		return nil
	}

	for sandboxID := range current {
		if _, ok := normalized[sandboxID]; ok {
			continue
		}
		s.deleteLocked(sandboxID)
	}
	return nil
}

// upsertLocked arbitrates and then writes, all under s.mu.
//
// 🔴 The comparison and the write are in one critical section, which is the
// whole property. Read the incumbent, decide, and write in three separate steps
// and a resume can install a new incarnation in the gap — the same shape as the
// registry's single-statement predicates, and as e2b's note about their own
// lockless catalog add.
//
// 🔴 A refused challenger changes nothing at all — not the binding and not the
// reverse index. Putting it in the index alone would leave a node listing a
// sandbox it does not own, and the next heartbeat in which it reports an empty
// roster would delete somebody else's binding on the way past.
func (s *InMemoryBindingStore) upsertLocked(sandboxID string, binding Binding, now time.Time, source string) bindingDecision {
	existing, held := s.bindings[sandboxID]
	if held && !existing.expiresAt.After(now) {
		// Expired is the same as absent: nothing holds this any more, so
		// anybody may take it.
		held = false
	}

	incumbent := ""
	if held {
		incumbent = existing.executionID
	}
	accept, decision := s.arbitrate(incumbent, held, binding.ExecutionID)
	if !accept {
		return decision
	}

	// The previous holder loses its index entry whether or not its record had
	// expired: an expired record still has one, and leaving it behind is how a
	// node comes to delete a binding it no longer owns.
	if _, present := s.bindings[sandboxID]; present && s.bindings[sandboxID].node.ID != binding.Node.ID {
		s.removeNodeBindingLocked(s.bindings[sandboxID].node.ID, sandboxID)
	}

	if _, present := s.nodeBinding[binding.Node.ID]; !present {
		s.nodeBinding[binding.Node.ID] = make(map[string]struct{})
	}
	s.nodeBinding[binding.Node.ID][sandboxID] = struct{}{}
	s.bindings[sandboxID] = bindingRecord{
		node:        binding.Node,
		executionID: binding.ExecutionID,
		expiresAt:   s.expiryFor(existing, held, decision, binding.ProjectionTTL, now, source),
	}
	return decision
}

// expiryFor decides the record's new deadline, and is where a heartbeat stops
// being able to extend one.
//
// 🔴 A heartbeat that finds the same incarnation already recorded keeps the
// deadline the record already has. It is a periodic tick, not a lifecycle
// event, and letting it rewrite the deadline every five seconds would put the
// projection's survival back on a periodic write path — which is exactly what
// the long TTL exists to get it off. The Lua reconciliation says the same thing
// with KEEPTTL, and the two have to agree: the default build runs this one and
// every HA deployment runs that one.
//
// Everything else is a real lifecycle event — a record being installed, a
// missing one being repaired, a new incarnation arriving with a new budget —
// and each of those sets the deadline from the budget it came with.
func (s *InMemoryBindingStore) expiryFor(
	existing bindingRecord,
	held bool,
	decision bindingDecision,
	projectionTTL time.Duration,
	now time.Time,
	source string,
) time.Time {
	if held && source == bindingSourceHeartbeat && decision == bindingRefreshed {
		return existing.expiresAt
	}
	ttl := projectionTTL
	if ttl <= 0 {
		// 🔴 Not "forever". A non-positive budget means the writer had none to
		// give, and the store's own default is what it had before any of this.
		ttl = s.bindingTTL
	}
	return now.Add(ttl)
}

func (s *InMemoryBindingStore) deleteLocked(sandboxID string) {
	record, ok := s.bindings[sandboxID]
	if !ok {
		return
	}
	delete(s.bindings, sandboxID)
	s.removeNodeBindingLocked(record.node.ID, sandboxID)
}

func (s *InMemoryBindingStore) removeNodeBindingLocked(nodeID string, sandboxID string) {
	bindings, ok := s.nodeBinding[nodeID]
	if !ok {
		return
	}
	delete(bindings, sandboxID)
	if len(bindings) == 0 {
		delete(s.nodeBinding, nodeID)
	}
}

type artifactIndexKey struct {
	clusterID string
	backend   string
	key       string
}

type InMemoryArtifactStore struct {
	mu              sync.RWMutex
	entries         map[artifactIndexKey]map[string]struct{}
	nodeKeys        map[string]map[artifactIndexKey]struct{}
	lru             *lru.Cache[artifactIndexKey, struct{}]
	lookupNodeLimit int
}

func NewInMemoryArtifactStore(capacity int, lookupNodeLimit int) *InMemoryArtifactStore {
	if capacity <= 0 {
		capacity = defaultArtifactStoreCapacity
	}

	store := &InMemoryArtifactStore{
		entries:         make(map[artifactIndexKey]map[string]struct{}),
		nodeKeys:        make(map[string]map[artifactIndexKey]struct{}),
		lookupNodeLimit: lookupNodeLimit,
	}
	cache, err := lru.NewWithEvict(capacity, store.evictLocked)
	if err != nil {
		panic(err)
	}
	store.lru = cache
	return store
}

func (s *InMemoryArtifactStore) Record(clusterID string, backend string, key string, nodeID string) {
	indexKey, ok := normalizeArtifactIndexKey(clusterID, backend, key)
	nodeID = strings.TrimSpace(nodeID)
	if !ok || nodeID == "" {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.entries[indexKey]; !ok {
		s.entries[indexKey] = make(map[string]struct{})
	}
	s.entries[indexKey][nodeID] = struct{}{}

	if _, ok := s.nodeKeys[nodeID]; !ok {
		s.nodeKeys[nodeID] = make(map[artifactIndexKey]struct{})
	}
	s.nodeKeys[nodeID][indexKey] = struct{}{}
	s.lru.Add(indexKey, struct{}{})
}

func (s *InMemoryArtifactStore) Forget(clusterID string, backend string, key string, nodeID string) {
	indexKey, ok := normalizeArtifactIndexKey(clusterID, backend, key)
	nodeID = strings.TrimSpace(nodeID)
	if !ok || nodeID == "" {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.forgetLocked(indexKey, nodeID)
}

func (s *InMemoryArtifactStore) Lookup(clusterID string, backend string, key string) []string {
	indexKey, ok := normalizeArtifactIndexKey(clusterID, backend, key)
	if !ok {
		return nil
	}

	s.mu.RLock()
	nodes := s.entries[indexKey]
	if len(nodes) == 0 {
		s.mu.RUnlock()
		return nil
	}
	s.lru.Get(indexKey)
	resultCapacity := len(nodes)
	if s.lookupNodeLimit > 0 && s.lookupNodeLimit < resultCapacity {
		resultCapacity = s.lookupNodeLimit
	}
	result := make([]string, 0, resultCapacity)
	for nodeID := range nodes {
		result = append(result, nodeID)
		if s.lookupNodeLimit > 0 && len(result) >= s.lookupNodeLimit {
			break
		}
	}
	s.mu.RUnlock()

	return result
}

func (s *InMemoryArtifactStore) ForgetNode(nodeID string) {
	nodeID = strings.TrimSpace(nodeID)
	if nodeID == "" {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	keys := s.nodeKeys[nodeID]
	for indexKey := range keys {
		if nodes := s.entries[indexKey]; len(nodes) > 0 {
			delete(nodes, nodeID)
			if len(nodes) == 0 {
				s.removeArtifactKeyLocked(indexKey)
			}
		}
	}
	delete(s.nodeKeys, nodeID)
}

func (s *InMemoryArtifactStore) forgetLocked(indexKey artifactIndexKey, nodeID string) {
	if nodes := s.entries[indexKey]; len(nodes) > 0 {
		delete(nodes, nodeID)
		if len(nodes) == 0 {
			s.removeArtifactKeyLocked(indexKey)
		}
	}

	if keys := s.nodeKeys[nodeID]; len(keys) > 0 {
		delete(keys, indexKey)
		if len(keys) == 0 {
			delete(s.nodeKeys, nodeID)
		}
	}
}

func (s *InMemoryArtifactStore) removeArtifactKeyLocked(indexKey artifactIndexKey) {
	delete(s.entries, indexKey)
	s.lru.Remove(indexKey)
}

// evictLocked runs from LRU callbacks while callers hold s.mu's write lock.
func (s *InMemoryArtifactStore) evictLocked(indexKey artifactIndexKey, _ struct{}) {
	nodes := s.entries[indexKey]
	delete(s.entries, indexKey)
	for nodeID := range nodes {
		if keys := s.nodeKeys[nodeID]; len(keys) > 0 {
			delete(keys, indexKey)
			if len(keys) == 0 {
				delete(s.nodeKeys, nodeID)
			}
		}
	}
}

func normalizeArtifactIndexKey(clusterID string, backend string, key string) (artifactIndexKey, bool) {
	indexKey := artifactIndexKey{
		clusterID: strings.TrimSpace(clusterID),
		backend:   strings.TrimSpace(backend),
		key:       strings.TrimSpace(key),
	}
	return indexKey, indexKey.clusterID != "" && indexKey.backend != "" && indexKey.key != ""
}

// InMemoryArbitrationFor and RedisArbitrationFor turn the configured mode into
// the arbiter each store takes.
//
// 🔴 They live here, next to the rule, so there is one mapping from the setting
// to a behaviour rather than one per store. An unrecognised value resolves to
// enforcing: the setting is validated at start-up and cannot reach this, and if
// it somehow did, the safe direction is the one that refuses.
func InMemoryArbitrationFor(mode string) arbiter {
	switch mode {
	case "off":
		return arbitrateOff
	case "observe":
		return arbitrateObserving
	default:
		return arbitrateFenced
	}
}

func RedisArbitrationFor(mode string) string {
	switch mode {
	case "off":
		return redisArbitrationOff
	case "observe":
		return redisArbitrationObserving
	default:
		return redisArbitrationFenced
	}
}
