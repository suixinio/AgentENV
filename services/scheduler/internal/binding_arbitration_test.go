package scheduler

import (
	"context"
	"encoding/json"
	"net"
	"strings"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// ─────────────────────────────────────────────────────────────────────────────
// The arbitration contract, run against both stores
// ─────────────────────────────────────────────────────────────────────────────

// 🔴 One contract, two implementations. Written this way because the two have
// already drifted once: the in-memory store is what every test in this package
// used, and the Redis one — the only store any HA deployment runs — was covered
// by nothing that asked it a question about routing. A contract that lives in
// one place is the only structure in which changing one implementation and not
// the other turns red immediately.

// executionIDs used across these tests. Ordered, because the rule is "the
// larger id is the newer incarnation" and unordered inputs would exercise the
// comparison with the values it is not meant to see.
const (
	execOld    = "00000001-0000-7000-8000-000000000001"
	execMiddle = "00000001-0000-7000-8000-000000000002"
	execNew    = "00000001-0000-7000-8000-000000000003"
)

var (
	arbNodeA = Node{ID: "node-a", Endpoint: "http://node-a"}
	arbNodeB = Node{ID: "node-b", Endpoint: "http://node-b"}
)

type bindingStoreFactory func(t *testing.T, ttl time.Duration) BindingStore

func bindingStoreContract(t *testing.T, run func(t *testing.T, newStore bindingStoreFactory)) {
	t.Helper()

	t.Run("in-memory", func(t *testing.T) {
		run(t, func(t *testing.T, ttl time.Duration) BindingStore {
			return NewInMemoryBindingStore(ttl)
		})
	})
	t.Run("redis", func(t *testing.T) {
		run(t, func(t *testing.T, ttl time.Duration) BindingStore {
			return newRedisBindingStoreForTest(t, ttl)
		})
	})
}

// TestBindingStoreKeepsTheNewerExecution — both arrival orders, because the
// defect being fixed is precisely that the order used to decide.
func TestBindingStoreKeepsTheNewerExecution(t *testing.T) {
	bindingStoreContract(t, func(t *testing.T, newStore bindingStoreFactory) {
		t.Run("newer arrives second", func(t *testing.T) {
			store := newStore(t, time.Minute)
			now := time.Now()
			mustReconcile(t, store, arbNodeA, []RosterEntry{{SandboxID: "sbx", ExecutionID: execOld}}, now)
			mustReconcile(t, store, arbNodeB, []RosterEntry{{SandboxID: "sbx", ExecutionID: execNew}}, now)
			assertBinding(t, store, "sbx", arbNodeB.ID, execNew)
		})

		t.Run("newer arrives first", func(t *testing.T) {
			store := newStore(t, time.Minute)
			now := time.Now()
			mustReconcile(t, store, arbNodeB, []RosterEntry{{SandboxID: "sbx", ExecutionID: execNew}}, now)
			mustReconcile(t, store, arbNodeA, []RosterEntry{{SandboxID: "sbx", ExecutionID: execOld}}, now)
			assertBinding(t, store, "sbx", arbNodeB.ID, execNew)
		})
	})
}

// TestBindingStoreRefusesToGoBackToAnOlderExecution is the nail in the defect:
// a node that lost a sandbox and then came back used to take its binding back
// on every single heartbeat, for ever.
func TestBindingStoreRefusesToGoBackToAnOlderExecution(t *testing.T) {
	bindingStoreContract(t, func(t *testing.T, newStore bindingStoreFactory) {
		store := newStore(t, time.Minute)
		now := time.Now()

		mustReconcile(t, store, arbNodeB, []RosterEntry{{SandboxID: "sbx", ExecutionID: execNew}}, now)
		// The old holder comes back and keeps reporting.
		for i := 0; i < 3; i++ {
			mustReconcile(t, store, arbNodeA, []RosterEntry{{SandboxID: "sbx", ExecutionID: execOld}}, now)
		}
		assertBinding(t, store, "sbx", arbNodeB.ID, execNew)

		// 🔴 And the refusal left no trace on the loser's reverse index. If it
		// had, node-a's next empty roster would delete node-b's binding on its
		// way past — a refusal that undoes itself one heartbeat later.
		mustReconcile(t, store, arbNodeA, nil, now)
		assertBinding(t, store, "sbx", arbNodeB.ID, execNew)
	})
}

// TestBindingStoreAcceptsTheSameNodeReportingANewerExecution is the control.
//
// 🟢 Without it, a store that refused every overwrite would pass the test above
// and would freeze the first binding a sandbox ever got.
func TestBindingStoreAcceptsTheSameNodeReportingANewerExecution(t *testing.T) {
	bindingStoreContract(t, func(t *testing.T, newStore bindingStoreFactory) {
		store := newStore(t, time.Minute)
		now := time.Now()

		mustReconcile(t, store, arbNodeA, []RosterEntry{{SandboxID: "sbx", ExecutionID: execOld}}, now)
		mustReconcile(t, store, arbNodeA, []RosterEntry{{SandboxID: "sbx", ExecutionID: execNew}}, now)
		assertBinding(t, store, "sbx", arbNodeA.ID, execNew)
	})
}

// TestBindingStoreTreatsAMissingExecutionAsUnknown.
//
// Unknown may be installed where nothing else holds the sandbox — losing a
// usable route to avoid an unfenced one is a bad trade — but it may not
// displace a record that names an incarnation.
func TestBindingStoreTreatsAMissingExecutionAsUnknown(t *testing.T) {
	bindingStoreContract(t, func(t *testing.T, newStore bindingStoreFactory) {
		t.Run("installed when nothing holds it", func(t *testing.T) {
			store := newStore(t, time.Minute)
			mustReconcile(t, store, arbNodeA, []RosterEntry{{SandboxID: "sbx"}}, time.Now())
			assertBinding(t, store, "sbx", arbNodeA.ID, "")
		})

		t.Run("refused against an incumbent that names one", func(t *testing.T) {
			store := newStore(t, time.Minute)
			now := time.Now()
			mustReconcile(t, store, arbNodeB, []RosterEntry{{SandboxID: "sbx", ExecutionID: execNew}}, now)
			mustReconcile(t, store, arbNodeA, []RosterEntry{{SandboxID: "sbx"}}, now)
			assertBinding(t, store, "sbx", arbNodeB.ID, execNew)
		})

		t.Run("an incumbent without one is upgraded", func(t *testing.T) {
			store := newStore(t, time.Minute)
			now := time.Now()
			mustReconcile(t, store, arbNodeA, []RosterEntry{{SandboxID: "sbx"}}, now)
			mustReconcile(t, store, arbNodeB, []RosterEntry{{SandboxID: "sbx", ExecutionID: execOld}}, now)
			assertBinding(t, store, "sbx", arbNodeB.ID, execOld)
		})
	})
}

// TestBindingStoreExpiredIncumbentsDoNotFence: an expired record holds nothing.
//
// Without this a binding whose TTL lapsed would go on fencing from beyond the
// grave, and the sandbox would be unroutable until somebody restarted the
// scheduler.
func TestBindingStoreExpiredIncumbentsDoNotFence(t *testing.T) {
	store := NewInMemoryBindingStore(time.Second)
	base := time.Unix(1000, 0)

	mustReconcile(t, store, arbNodeB, []RosterEntry{{SandboxID: "sbx", ExecutionID: execNew}}, base)
	mustReconcile(t, store, arbNodeA, []RosterEntry{{SandboxID: "sbx", ExecutionID: execOld}}, base.Add(2*time.Second))

	binding, ok, err := store.Get("sbx", base.Add(2*time.Second))
	if err != nil || !ok {
		t.Fatalf("expected a binding: ok=%v err=%v", ok, err)
	}
	if binding.Node.ID != arbNodeA.ID {
		t.Fatalf("an expired record still fenced: got %q", binding.Node.ID)
	}
}

// TestBindingStoreNormalisesExecutionCase.
//
// 🔴 The comparison is lexicographic, and in ASCII '0'-'9' < 'A'-'F' <
// 'a'-'f'. The inputs are built to make an unnormalised comparison get it
// backwards: the incumbent is the *newer* incarnation spelled in upper case,
// the challenger is an older one in lower case, and "0…a1" > "0…A2" byte for
// byte — so without normalisation the older VM wins.
func TestBindingStoreNormalisesExecutionCase(t *testing.T) {
	upperNewer := "00000001-0000-7000-8000-0000000000A2"
	lowerOlder := "00000001-0000-7000-8000-0000000000a1"
	if !(lowerOlder > upperNewer) {
		t.Fatalf("this test's inputs no longer demonstrate the reversal: %q vs %q", lowerOlder, upperNewer)
	}

	registry := NewAtomicNodeRegistry([]Node{arbNodeA, arbNodeB}, defaultObservedReportTTL)
	store := NewInMemoryBindingStore(time.Minute)
	svc := NewService(zap.NewNop(), registry, NewStrategy("round_robin"), store)

	heartbeatWithExecutions(t, svc, arbNodeB.ID, RosterEntry{SandboxID: "sbx", ExecutionID: upperNewer})
	heartbeatWithExecutions(t, svc, arbNodeA.ID, RosterEntry{SandboxID: "sbx", ExecutionID: lowerOlder})

	binding, ok, err := store.Get("sbx", time.Now())
	if err != nil || !ok {
		t.Fatalf("expected a binding: ok=%v err=%v", ok, err)
	}
	if binding.Node.ID != arbNodeB.ID {
		t.Fatalf("the older incarnation won because the ids were compared without normalising case: got %q", binding.Node.ID)
	}
	if binding.ExecutionID != strings.ToLower(upperNewer) {
		t.Fatalf("the stored incarnation was not normalised: %q", binding.ExecutionID)
	}
}

// TestRecordAssignmentGoesThroughTheSameArbitration — S6's non-negotiable half.
//
// 🔴 The assignment write is the one place a caller can reach the binding table
// without a heartbeat. Left unguarded it is a way around everything the
// heartbeat path enforces: a create response that landed on a node the sandbox
// has since left would put that node back in charge.
func TestRecordAssignmentGoesThroughTheSameArbitration(t *testing.T) {
	bindingStoreContract(t, func(t *testing.T, newStore bindingStoreFactory) {
		store := newStore(t, time.Minute)
		now := time.Now()

		mustReconcile(t, store, arbNodeB, []RosterEntry{{SandboxID: "sbx", ExecutionID: execNew}}, now)
		if err := store.Record("sbx", Binding{Node: arbNodeA, ExecutionID: execOld}, now); err != nil {
			t.Fatalf("record: %v", err)
		}
		assertBinding(t, store, "sbx", arbNodeB.ID, execNew)

		// 🟢 The control: an assignment carrying a *newer* incarnation is
		// still accepted, so this is not a store that simply refuses
		// assignments.
		if err := store.Record("sbx", Binding{Node: arbNodeA, ExecutionID: execNew + "0"}, now); err == nil {
			// execNew+"0" is 37 characters and therefore not a canonical uuid;
			// build a proper larger one instead.
			_ = err
		}
		if err := store.Record("sbx", Binding{Node: arbNodeA, ExecutionID: "00000001-0000-7000-8000-000000000009"}, now); err != nil {
			t.Fatalf("record: %v", err)
		}
		assertBinding(t, store, "sbx", arbNodeA.ID, "00000001-0000-7000-8000-000000000009")
	})
}

// TestRecordAssignmentThroughTheServiceCarriesTheExecution: the field has to
// survive the handler, not merely exist on the message.
func TestRecordAssignmentThroughTheServiceCarriesTheExecution(t *testing.T) {
	registry := NewAtomicNodeRegistry([]Node{arbNodeA}, defaultObservedReportTTL)
	store := NewInMemoryBindingStore(time.Minute)
	svc := NewService(zap.NewNop(), registry, NewStrategy("round_robin"), store)

	if _, err := svc.RecordAssignment(context.Background(), &schedulerv1.RecordAssignmentRequest{
		SandboxId:   "sbx",
		Node:        arbNodeA.ToProto(),
		ExecutionId: strings.ToUpper(execNew),
	}); err != nil {
		t.Fatalf("record assignment: %v", err)
	}

	binding, ok, err := store.Get("sbx", time.Now())
	if err != nil || !ok {
		t.Fatalf("expected a binding: ok=%v err=%v", ok, err)
	}
	if binding.ExecutionID != execNew {
		t.Fatalf("the assignment's incarnation did not reach the store normalised: %q", binding.ExecutionID)
	}
}

// TestRedisReconcileTakesOneRoundTrip — M4's catcher, and deterministic.
//
// 🔴 What it rules out is an implementation shape, not a wrong answer: reading
// the incumbent in Go, comparing there, and then writing. That version passes
// nearly every behavioural test in this file and loses to any resume that lands
// between its read and its write. Counting the commands is how the shape itself
// is asserted, with no sleep and no reliance on a race actually happening.
func TestRedisReconcileTakesOneRoundTrip(t *testing.T) {
	store := newRedisBindingStoreForTest(t, time.Minute)
	counter := &redisCommandCounter{}
	store.client.AddHook(counter)

	// Warm the script cache first, so the EVALSHA/EVAL fallback of a first call
	// is not what this measures.
	mustReconcile(t, store, arbNodeA, []RosterEntry{{SandboxID: "warm", ExecutionID: execOld}}, time.Now())
	counter.reset()

	mustReconcile(t, store, arbNodeA, []RosterEntry{{SandboxID: "sbx", ExecutionID: execNew}}, time.Now())

	if got := counter.count("get"); got != 0 {
		t.Fatalf("the reconcile issued %d GET commands; the comparison is being made in Go, outside the script, which is the window a resume slips through", got)
	}
	if got := counter.count("evalsha") + counter.count("eval"); got != 1 {
		t.Fatalf("expected exactly one script call, got %d (commands: %v)", got, counter.names())
	}
}

// TestRedisRecordTakesOneRoundTrip is the same assertion for the assignment
// path, which is the other write and therefore the other place the comparison
// could be lifted out of the script.
func TestRedisRecordTakesOneRoundTrip(t *testing.T) {
	store := newRedisBindingStoreForTest(t, time.Minute)
	counter := &redisCommandCounter{}
	store.client.AddHook(counter)

	if err := store.Record("warm", Binding{Node: arbNodeA, ExecutionID: execOld}, time.Now()); err != nil {
		t.Fatalf("record: %v", err)
	}
	counter.reset()

	if err := store.Record("sbx", Binding{Node: arbNodeA, ExecutionID: execNew}, time.Now()); err != nil {
		t.Fatalf("record: %v", err)
	}
	if got := counter.count("get"); got != 0 {
		t.Fatalf("the assignment write issued %d GET commands outside the script", got)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// The rollback modes
// ─────────────────────────────────────────────────────────────────────────────

// TestExecutionArbitrationOffMatchesLegacyBehaviour.
//
// 🔴 The assertion is positive: with the switch off, the older incarnation
// *does* take the binding back. Asserting only "no error" would pass against a
// mode that quietly went on arbitrating, and the whole value of a rollback is
// that it restores a behaviour somebody is depending on.
func TestExecutionArbitrationOffMatchesLegacyBehaviour(t *testing.T) {
	t.Run("in-memory", func(t *testing.T) {
		store := NewInMemoryBindingStoreWithArbitration(time.Minute, InMemoryArbitrationFor("off"))
		now := time.Now()
		mustReconcile(t, store, arbNodeB, []RosterEntry{{SandboxID: "sbx", ExecutionID: execNew}}, now)
		mustReconcile(t, store, arbNodeA, []RosterEntry{{SandboxID: "sbx", ExecutionID: execOld}}, now)
		assertBinding(t, store, "sbx", arbNodeA.ID, execOld)
	})

	t.Run("redis", func(t *testing.T) {
		addr := startRedisServerForTest(t)
		store, err := NewRedisBindingStoreWithArbitration(addr, time.Minute, RedisArbitrationFor("off"))
		if err != nil {
			t.Fatalf("create redis binding store: %v", err)
		}
		t.Cleanup(func() { _ = store.Close() })

		now := time.Now()
		mustReconcile(t, store, arbNodeB, []RosterEntry{{SandboxID: "sbx", ExecutionID: execNew}}, now)
		mustReconcile(t, store, arbNodeA, []RosterEntry{{SandboxID: "sbx", ExecutionID: execOld}}, now)
		assertBinding(t, store, "sbx", arbNodeA.ID, execOld)
	})
}

// TestExecutionArbitrationObserveKeepsRoutingButCounts: the release's first
// step. Same routing as off, so nothing moves; the decisions become visible.
func TestExecutionArbitrationObserveKeepsRoutingButCounts(t *testing.T) {
	t.Run("in-memory", func(t *testing.T) {
		store := NewInMemoryBindingStoreWithArbitration(time.Minute, InMemoryArbitrationFor("observe"))
		now := time.Now()
		mustReconcile(t, store, arbNodeB, []RosterEntry{{SandboxID: "sbx", ExecutionID: execNew}}, now)

		before := bindingDecisionCount(t, string(bindingRejectedOlder), bindingSourceHeartbeat)
		mustReconcile(t, store, arbNodeA, []RosterEntry{{SandboxID: "sbx", ExecutionID: execOld}}, now)
		after := bindingDecisionCount(t, string(bindingRejectedOlder), bindingSourceHeartbeat)

		assertBinding(t, store, "sbx", arbNodeA.ID, execOld)
		if after <= before {
			t.Fatalf("observe mode wrote the binding but counted nothing: %v -> %v", before, after)
		}
	})

	t.Run("redis", func(t *testing.T) {
		addr := startRedisServerForTest(t)
		store, err := NewRedisBindingStoreWithArbitration(addr, time.Minute, RedisArbitrationFor("observe"))
		if err != nil {
			t.Fatalf("create redis binding store: %v", err)
		}
		t.Cleanup(func() { _ = store.Close() })

		now := time.Now()
		mustReconcile(t, store, arbNodeB, []RosterEntry{{SandboxID: "sbx", ExecutionID: execNew}}, now)

		before := bindingDecisionCount(t, string(bindingRejectedOlder), bindingSourceHeartbeat)
		mustReconcile(t, store, arbNodeA, []RosterEntry{{SandboxID: "sbx", ExecutionID: execOld}}, now)
		after := bindingDecisionCount(t, string(bindingRejectedOlder), bindingSourceHeartbeat)

		assertBinding(t, store, "sbx", arbNodeA.ID, execOld)
		if after <= before {
			t.Fatalf("observe mode wrote the binding but counted nothing: %v -> %v", before, after)
		}
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

func mustReconcile(t *testing.T, store BindingStore, node Node, roster []RosterEntry, now time.Time) {
	t.Helper()
	if err := store.ReconcileNode(node, roster, now); err != nil {
		t.Fatalf("reconcile %s: %v", node.ID, err)
	}
}

func assertBinding(t *testing.T, store BindingStore, sandboxID, wantNode, wantExecution string) {
	t.Helper()
	binding, ok, err := store.Get(sandboxID, time.Now())
	if err != nil {
		t.Fatalf("get %s: %v", sandboxID, err)
	}
	if !ok {
		t.Fatalf("expected %s to be bound", sandboxID)
	}
	if binding.Node.ID != wantNode {
		t.Fatalf("%s is bound to %q, want %q", sandboxID, binding.Node.ID, wantNode)
	}
	if binding.ExecutionID != wantExecution {
		t.Fatalf("%s carries incarnation %q, want %q", sandboxID, binding.ExecutionID, wantExecution)
	}
}

func heartbeatWithExecutions(t *testing.T, svc *Service, nodeID string, entries ...RosterEntry) {
	t.Helper()

	roster := make([]*schedulerv1.SandboxRosterEntry, 0, len(entries))
	for _, entry := range entries {
		roster = append(roster, &schedulerv1.SandboxRosterEntry{
			SandboxId:   entry.SandboxID,
			ExecutionId: entry.ExecutionID,
		})
	}
	if _, err := svc.Heartbeat(context.Background(), &schedulerv1.HeartbeatRequest{
		NodeId:            nodeID,
		ClusterId:         "cluster-1",
		ServiceInstanceId: nodeID + "-1",
		Snapshot:          &schedulerv1.NodeSnapshot{Status: schedulerv1.NodeStatus_NODE_STATUS_READY},
		Roster:            roster,
	}); err != nil {
		t.Fatalf("heartbeat for %s failed: %v", nodeID, err)
	}
}

// redisCommandCounter counts the commands a client actually sends, which is
// the only way to assert where a comparison is being made.
type redisCommandCounter struct {
	counts map[string]int
	lock   chan struct{}
}

func (c *redisCommandCounter) ensure() {
	if c.lock == nil {
		c.lock = make(chan struct{}, 1)
		c.lock <- struct{}{}
	}
	if c.counts == nil {
		c.counts = map[string]int{}
	}
}

func (c *redisCommandCounter) add(name string) {
	c.ensure()
	<-c.lock
	c.counts[strings.ToLower(name)]++
	c.lock <- struct{}{}
}

func (c *redisCommandCounter) count(name string) int {
	c.ensure()
	<-c.lock
	defer func() { c.lock <- struct{}{} }()
	return c.counts[name]
}

func (c *redisCommandCounter) names() map[string]int {
	c.ensure()
	<-c.lock
	defer func() { c.lock <- struct{}{} }()
	out := map[string]int{}
	for k, v := range c.counts {
		out[k] = v
	}
	return out
}

func (c *redisCommandCounter) reset() {
	c.ensure()
	<-c.lock
	c.counts = map[string]int{}
	c.lock <- struct{}{}
}

func (c *redisCommandCounter) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		return next(ctx, network, addr)
	}
}

func (c *redisCommandCounter) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		c.add(cmd.Name())
		return next(ctx, cmd)
	}
}

func (c *redisCommandCounter) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		for _, cmd := range cmds {
			c.add(cmd.Name())
		}
		return next(ctx, cmds)
	}
}

// bindingDecisionCount reads one series of the arbitration counter.
//
// It gathers through a private registry over the process-wide collector, which
// is the same shape the reconciliation metric tests use: the collector under
// test is the real one, the gathering is not entangled with whatever else is
// registered globally.
func bindingDecisionCount(t *testing.T, decision, source string) float64 {
	t.Helper()

	reg := prometheus.NewRegistry()
	reg.MustRegister(schedulerBindingExecution)
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather binding decisions: %v", err)
	}
	for _, family := range families {
		if family.GetName() != "agentenv_scheduler_binding_execution_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			labels := map[string]string{}
			for _, label := range metric.GetLabel() {
				labels[label.GetName()] = label.GetValue()
			}
			if labels["decision"] == decision && labels["source"] == source {
				return metric.GetCounter().GetValue()
			}
		}
	}
	return 0
}

// TestTheRedisRecordShapeIsTheGoTypes.
//
// 🔴 The reconcile script writes binding records, and the shape of one is
// defined by the Go type — once. Re-encoding the node inside Lua works for the
// two fields the parser reads today and silently drops whatever this type grows
// next; the field that goes missing is then invisible until something needs it.
// So the node half is marshalled in Go and the script splices the incarnation
// beside it, and this test is what says so.
func TestTheRedisRecordShapeIsTheGoTypes(t *testing.T) {
	store := newRedisBindingStoreForTest(t, time.Minute)
	node := Node{ID: "node-a", Endpoint: "http://node-a", PodName: "agentenv-node-xyz"}

	mustReconcile(t, store, node, []RosterEntry{{SandboxID: "sbx", ExecutionID: execNew}}, time.Now())

	raw, err := store.client.Get(context.Background(), store.bindingKey("sbx")).Bytes()
	if err != nil {
		t.Fatalf("read the stored record: %v", err)
	}
	var record redisBindingRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		t.Fatalf("the stored record is not JSON this build can read: %v; raw=%q", err, raw)
	}
	if record.Node != node {
		t.Fatalf("the reconcile script rewrote the node half: got %+v, want %+v (raw=%q)", record.Node, node, raw)
	}
	if record.ExecutionID != execNew {
		t.Fatalf("execution_id: got %q, want %q (raw=%q)", record.ExecutionID, execNew, raw)
	}

	// And the assignment path, which marshals the whole record in Go, agrees.
	if err := store.Record("sbx-2", Binding{Node: node, ExecutionID: execNew}, time.Now()); err != nil {
		t.Fatalf("record: %v", err)
	}
	other, err := store.client.Get(context.Background(), store.bindingKey("sbx-2")).Bytes()
	if err != nil {
		t.Fatalf("read the stored record: %v", err)
	}
	var second redisBindingRecord
	if err := json.Unmarshal(other, &second); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if second.Node != record.Node || second.ExecutionID != record.ExecutionID {
		t.Fatalf("the two write paths store different records:\n heartbeat %q\n assignment %q", raw, other)
	}
}
