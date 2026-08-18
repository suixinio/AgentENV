package scheduler

import (
	"sync/atomic"
	"time"
)

// defaultWarmupTimeout bounds how long a cold scheduler may withhold "not
// found" answers. Long enough for every live node to deliver a heartbeat
// (nodes report every few seconds), short enough that a cluster whose nodes are
// genuinely down starts answering honestly again quickly.
const defaultWarmupTimeout = 15 * time.Second

// warmupGate decides whether the binding store is warm enough for a miss to
// mean "this sandbox is not assigned anywhere".
//
// Bindings are a cache: they are held in memory, expire on a TTL, and are
// re-seeded entirely from node heartbeats. A scheduler that has just started
// therefore knows nothing — and answering NotFound from that state is not a
// cache miss, it is the cache asserting something it cannot know. Three things
// downstream take that assertion at face value:
//
//   - the gateway turns it into a 404, so traffic to perfectly healthy
//     sandboxes fails for as long as the window lasts;
//   - the gateway treats NotFound on a resume as "nobody holds this sandbox"
//     and hands the resume to a scheduled node, which claims the sandbox from
//     the paused registry and rebuilds it from object storage — while the node
//     that has it sits idle with the artifacts on local disk;
//   - a sandbox whose snapshot never published (`local_only`) cannot be
//     rebuilt anywhere else at all, so that resume comes back 409 instead of
//     succeeding on its own node in a second.
//
// e2b closes the same window by running a full node sync before serving rather
// than waiting for the first tick (`e2b/packages/api/internal/orchestrator/cache.go:34`,
// *"Running the initial node sync"*). We cannot pull — nodes push — so the
// equivalent is to withhold the assertion until the pushes have arrived.
//
// Warm means: at least one node is known, and every known node has reported
// since this process started. Nodes that are down never report, so the deadline
// is what stops a single dead node from withholding every answer forever.
type warmupGate struct {
	nodes    NodeRegistry
	deadline time.Time
	// Latched once warm: nodes come and go afterwards, and a node joining an
	// hour later must not put the scheduler back into warm-up.
	warm atomic.Bool
	// Whether any node has reported at all. Without it the "every known node
	// has reported" test passes vacuously whenever the node list is empty —
	// which is true both before discovery has run (the coldest state there is)
	// and after the last node unregisters (long since warm).
	reported atomic.Bool
}

func newWarmupGate(nodes NodeRegistry, timeout time.Duration, now time.Time) *warmupGate {
	if timeout <= 0 {
		timeout = defaultWarmupTimeout
	}

	return &warmupGate{nodes: nodes, deadline: now.Add(timeout)}
}

// reportedIn records that a node has delivered a heartbeat, and with it the
// roster of sandboxes that node holds.
//
// It re-evaluates the gate rather than only setting a flag, because the moment
// the last node reports is the moment warm-up is over — and waiting for a
// lookup to notice would leave the gate shut through a node unregistering, or
// a node dropping out of discovery, either of which puts a never-reported node
// back in the list and reads as cold all over again.
func (g *warmupGate) reportedIn(now time.Time) {
	g.reported.Store(true)
	g.warmedUp(now)
}

// warmedUp reports whether a binding miss may be answered as NotFound.
func (g *warmupGate) warmedUp(now time.Time) bool {
	if g.warm.Load() {
		return true
	}
	if !now.Before(g.deadline) {
		g.warm.Store(true)

		return true
	}
	if !g.reported.Load() {
		return false
	}

	// Lingering nodes are excluded deliberately: they are on their way out of
	// discovery and waiting for a heartbeat that may never come would hold the
	// gate shut for the whole deadline.
	for _, node := range g.nodes.Snapshot( /* allowLingering */ false) {
		if g.nodes.PeekObserved(node.ID) == nil {
			return false
		}
	}

	g.warm.Store(true)

	return true
}
