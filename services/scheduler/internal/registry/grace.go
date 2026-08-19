package registry

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"
)

// Phase is how much of the write surface this process is entitled to serve.
//
// 🔴 There is no phase in which a caller is answered with an empty result
// instead of an error. A sandbox missing from a read means the row does not
// exist, and the node deletes local artifacts and tears down VMs on the
// strength of that; "I am not ready to say" and "there is no such row" have to
// be different answers on the wire, or the second one gets given by mistake.
type Phase int

const (
	// PhaseCold is the state before Migrate has succeeded. Nothing is served.
	PhaseCold Phase = iota
	// PhaseGrace is after the leases have been extended but before nodes have
	// had a chance to renew them. Reads and ordinary writes are served;
	// anything that takes a sandbox away from the node holding it is not.
	PhaseGrace
	// PhaseServing is the steady state.
	PhaseServing
)

func (p Phase) String() string {
	switch p {
	case PhaseCold:
		return "cold"
	case PhaseGrace:
		return "grace"
	case PhaseServing:
		return "serving"
	default:
		return "unknown"
	}
}

var (
	// ErrNotReady means this process cannot speak for the table yet — the
	// migration has not finished, or it has not run its restart pass.
	//
	// 🔴 Never fold this into an empty answer. That is the whole point of it
	// having a name.
	ErrNotReady = errors.New("paused registry is not ready")

	// ErrGracePeriod means the request would have taken a sandbox away from
	// the node that holds it, during the window where every lease in the table
	// looks lapsed because this process was not around to renew it.
	ErrGracePeriod = errors.New("paused registry is in its restart grace period")

	// ErrDiscardBreakerTripped means a reclamation pass would have deleted more
	// rows than any explanation this build has for them, so it deleted none.
	ErrDiscardBreakerTripped = errors.New("paused registry discard circuit breaker tripped")
)

var (
	registryWritePhase = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "agentenv_scheduler_registry_write_phase",
		Help: "Paused-registry write surface phase: 0 cold, 1 grace, 2 serving.",
	})
	registryGraceDowntimeSeconds = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "agentenv_scheduler_registry_write_grace_downtime_seconds",
		Help: "Downtime this process inferred from max(updated_at) at start-up.",
	})
	registryGraceLeasesExtended = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "agentenv_scheduler_registry_write_grace_leases_extended",
		Help: "Rows whose lease the restart grace pass extended.",
	})
	registryGraceRefusals = promauto.NewCounter(prometheus.CounterOpts{
		Name: "agentenv_scheduler_registry_write_grace_refusals_total",
		Help: "Requests refused because the write surface was cold or in its grace period.",
	})
	registryGraceTakeoversWithheld = promauto.NewCounter(prometheus.CounterOpts{
		Name: "agentenv_scheduler_registry_write_grace_takeovers_withheld_total",
		Help: "Claims served during the grace period without the lapsed-lease takeover arm.",
	})
	registryClaimRewound = promauto.NewCounter(prometheus.CounterOpts{
		Name: "agentenv_scheduler_registry_write_claim_rewound_total",
		Help: "Claims that took over a parked sandbox whose snapshot never published, rewinding it one snapshot.",
	})
	registryClaimInvariantViolation = promauto.NewCounter(prometheus.CounterOpts{
		Name: "agentenv_scheduler_registry_write_claim_invariant_violation_total",
		Help: "Claims whose previous state the claim predicate should have made unreachable.",
	})
	registryMarkRunningRefused = promauto.NewCounter(prometheus.CounterOpts{
		Name: "agentenv_scheduler_registry_write_mark_running_refused_total",
		Help: "mark_running calls refused because another node held the resume claim.",
	})
	registryReclaimReleased = promauto.NewCounter(prometheus.CounterOpts{
		Name: "agentenv_scheduler_registry_write_reclaim_released_total",
		Help: "Rows reclamation handed back to the cluster as paused.",
	})
	registryReclaimDiscarded = promauto.NewCounter(prometheus.CounterOpts{
		Name: "agentenv_scheduler_registry_write_reclaim_discarded_total",
		Help: "Rows reclamation deleted because no snapshot was ever published for them.",
	})
	registryReclaimBreakerTripped = promauto.NewCounter(prometheus.CounterOpts{
		Name: "agentenv_scheduler_registry_write_reclaim_breaker_tripped_total",
		Help: "Reclamation passes abandoned because they would have discarded too many rows.",
	})
)

// Grace gates the write surface through start-up.
//
// 🔴 The failure it exists for is new in this stage. Until now a lease was
// renewed by the node that held the sandbox, against a database in its own
// cluster; the only thing that could stop it was that node's own health, which
// is exactly what a lapsed lease is supposed to mean. Now every renewal in the
// cluster goes through this one process, which has one replica, no PDB and no
// surge — so a rolling update, an image pull, an eviction or an OOM stops every
// renewal at once. Come back after longer than a lease and the table says every
// node in the fleet is gone, which is the one conclusion that costs users work:
// each parked row becomes claimable elsewhere and comes back a snapshot behind.
//
// So on start-up the leases are pushed out by what was missed before anything
// is allowed to read them as evidence.
type Grace struct {
	ttl time.Duration
	log *zap.Logger

	mu       sync.RWMutex
	phase    Phase
	until    time.Time
	downtime time.Duration
}

// NewGrace returns a gate that is cold until Enter succeeds.
//
// ttl is the lease length this cluster's nodes were told to expect; it is both
// the amount added to every lease and the length of the window afterwards.
func NewGrace(ttl time.Duration, log *zap.Logger) *Grace {
	if ttl <= 0 {
		ttl = defaultLeaseTTL
	}
	if log == nil {
		log = zap.NewNop()
	}
	registryWritePhase.Set(float64(PhaseCold))
	return &Grace{ttl: ttl, log: log, phase: PhaseCold}
}

// GraceObservation is what the restart pass saw and did.
type GraceObservation struct {
	// Downtime is now() - max(updated_at) for the cluster, as the database
	// measured both. One full lease when the table has no rows to measure
	// against, which is the conservative answer rather than zero.
	Downtime time.Duration
	// Extended is how many rows had their lease pushed out.
	Extended int64
	// Until is when the grace window closes.
	Until time.Time
}

// LeaseExtender is the one database operation the restart pass needs.
//
// Named separately, and exported, because it is not part of Store: it is this
// process's own repair of its own absence, not an operation any node can ask
// for. Keeping it off Store is what stops it from turning into one.
type LeaseExtender interface {
	// ExtendLeases pushes every lease in the cluster out by the downtime it
	// infers plus one lease, and reports both what it inferred and how many
	// rows it touched.
	ExtendLeases(ctx context.Context, clusterID string, ttl time.Duration) (downtime time.Duration, extended int64, err error)
}

// Enter runs the restart pass and opens the grace window.
//
// Called after Migrate and before the service is registered. Until it returns
// the phase is cold and every request is refused.
func (g *Grace) Enter(ctx context.Context, ext LeaseExtender, clusterID string) (GraceObservation, error) {
	downtime, extended, err := ext.ExtendLeases(ctx, clusterID, g.ttl)
	if err != nil {
		return GraceObservation{}, err
	}

	until := time.Now().Add(g.ttl)

	g.mu.Lock()
	g.phase = PhaseGrace
	g.until = until
	g.downtime = downtime
	g.mu.Unlock()

	registryWritePhase.Set(float64(PhaseGrace))
	registryGraceDowntimeSeconds.Set(downtime.Seconds())
	registryGraceLeasesExtended.Set(float64(extended))

	g.log.Info("paused registry write surface entering its restart grace period",
		zap.String("cluster_id", clusterID),
		zap.Duration("inferred_downtime", downtime),
		zap.Int64("leases_extended", extended),
		zap.Duration("grace", g.ttl),
	)
	return GraceObservation{Downtime: downtime, Extended: extended, Until: until}, nil
}

// Phase reports where the gate is, promoting grace to serving once the window
// has passed.
//
// The window is measured on this process's own clock rather than the database's
// because it is a statement about this process — how long since it came back —
// and taking a round trip to answer it would put a database outage in the way
// of noticing that the database outage is over.
func (g *Grace) Phase() Phase {
	if g == nil {
		return PhaseServing
	}

	g.mu.RLock()
	phase, until := g.phase, g.until
	g.mu.RUnlock()

	if phase != PhaseGrace || time.Now().Before(until) {
		return phase
	}

	g.mu.Lock()
	if g.phase == PhaseGrace && !time.Now().Before(g.until) {
		g.phase = PhaseServing
		g.log.Info("paused registry write surface is serving; the restart grace period has passed")
	}
	phase = g.phase
	g.mu.Unlock()

	registryWritePhase.Set(float64(phase))
	return phase
}

// Ready reports whether the migration has finished and the restart pass has
// run. A false answer must become an error on the wire, never an empty result.
func (g *Grace) Ready() bool { return g.Phase() != PhaseCold }

// Serving reports whether the grace window has closed.
func (g *Grace) Serving() bool { return g.Phase() == PhaseServing }

// Require refuses everything until the migration and the restart pass are done.
func (g *Grace) Require() error {
	if g.Ready() {
		return nil
	}
	registryGraceRefusals.Inc()
	return ErrNotReady
}

// RequireServing additionally refuses through the grace window. Reclamation
// uses it: nothing about that pass is urgent, and every row it would act on is
// one whose lease this process is the reason nobody renewed.
func (g *Grace) RequireServing() error {
	switch g.Phase() {
	case PhaseServing:
		return nil
	case PhaseGrace:
		registryGraceRefusals.Inc()
		return ErrGracePeriod
	default:
		registryGraceRefusals.Inc()
		return ErrNotReady
	}
}

// allowsLeaseTakeover reports whether a claim may use its lapsed-lease arm.
//
// False through the grace window. A claim on a `paused` row is unaffected —
// that arm never consults the lease, and refusing it would stall every ordinary
// cross-node resume for a full lease after each restart for no reason at all.
// The arm that is withheld is the one that takes a sandbox off a node that was
// still uploading its snapshot, which is precisely the decision a lease this
// process failed to renew cannot support.
func (g *Grace) allowsLeaseTakeover() bool {
	if g == nil {
		return true
	}
	if g.Phase() == PhaseServing {
		return true
	}
	registryGraceTakeoversWithheld.Inc()
	return false
}

// Observation reports the current window for a health endpoint.
func (g *Grace) Observation() (Phase, time.Duration, time.Duration) {
	phase := g.Phase()
	if g == nil {
		return phase, 0, 0
	}
	g.mu.RLock()
	until, downtime := g.until, g.downtime
	g.mu.RUnlock()

	remaining := time.Until(until)
	if phase != PhaseGrace || remaining < 0 {
		remaining = 0
	}
	return phase, remaining, downtime
}

// extendLeasesSQL pushes every lease in the cluster out by the downtime plus a
// full lease, in one statement so both halves see the same snapshot.
//
// 🔴 Additive, not `now() + downtime + ttl`. The additive form is self-limiting
// in the direction that matters: a row's lease can be no later than
// (start of outage) + ttl, so the result can be no later than now + 2·ttl
// however long the outage was — while a row whose lease had *already* lapsed
// well before the outage stays lapsed, because the same amount is added to a
// value that much further in the past. A dead node's rows therefore remain
// reclaimable, which the absolute form would have deferred by the whole
// duration of an outage that had nothing to do with them.
//
// COALESCE(lease_expires_at, updated_at) is the same fallback the lease
// predicate uses, so a row written before the column existed is extended from
// the same instant it is judged against.
//
// updated_at is deliberately not touched. It is the evidence this pass reads to
// infer the downtime, and overwriting it with the time of the repair would
// leave the next restart measuring its outage against this one's clean-up.
const extendLeasesSQL = `
WITH observed AS (
    SELECT GREATEST(
               COALESCE(now() - max(updated_at), make_interval(secs => $2::double precision)),
               interval '0'
           ) AS downtime
      FROM paused_sandboxes
     WHERE cluster_id = $1::uuid
),
extended AS (
    UPDATE paused_sandboxes p
       SET lease_expires_at = COALESCE(p.lease_expires_at, p.updated_at)
                            + (SELECT downtime FROM observed)
                            + make_interval(secs => $2::double precision)
     WHERE p.cluster_id = $1::uuid
    RETURNING 1
)
SELECT EXTRACT(EPOCH FROM (SELECT downtime FROM observed))::double precision AS downtime_secs,
       (SELECT count(*) FROM extended)                                       AS extended`

// ExtendLeases implements LeaseExtender.
func (s *PostgresStore) ExtendLeases(ctx context.Context, clusterID string, ttl time.Duration) (time.Duration, int64, error) {
	cluster, err := requireUUID("cluster_id", clusterID)
	if err != nil {
		return 0, 0, err
	}
	if ttl <= 0 {
		ttl = s.leaseTTL
	}

	ctx, cancel := s.withTimeout(ctx)
	defer cancel()

	var (
		downtimeSecs float64
		extended     int64
	)
	if err := s.pool.QueryRow(ctx, extendLeasesSQL, cluster, ttl.Seconds()).Scan(&downtimeSecs, &extended); err != nil {
		return 0, 0, fmt.Errorf("registry restart grace pass: %w", err)
	}
	return time.Duration(downtimeSecs * float64(time.Second)), extended, nil
}

// DiscardBreaker stops a reclamation pass that would delete more than it can
// account for.
//
// 🔴 Not aimed at a bug anybody has found. The rows this deletes are the ones
// nothing can rebuild — no snapshot was ever published for them — so a pass
// that is wrong about even a handful of them is unrecoverable, and every way it
// could be wrong runs through a predicate over two clocks and a state column.
// A limit that stops the pass and says so costs one round of stranded rows; not
// having one costs whatever the next mistake in that predicate happens to be.
//
// Both limits apply and the stricter wins: a small cluster is protected by the
// ratio, a large one by the absolute count.
type DiscardBreaker struct {
	// MaxRows trips the breaker on its own. Zero takes the default.
	MaxRows int64
	// MaxRatio is a fraction of the cluster's rows. Zero takes the default.
	MaxRatio float64
	// Log receives the refusal.
	Log *zap.Logger
}

const (
	defaultDiscardMaxRows  int64   = 10
	defaultDiscardMaxRatio float64 = 0.10
)

// NewDiscardBreaker fills in whatever the caller left at zero.
func NewDiscardBreaker(maxRows int64, maxRatio float64, log *zap.Logger) *DiscardBreaker {
	if maxRows <= 0 {
		maxRows = defaultDiscardMaxRows
	}
	if maxRatio <= 0 {
		maxRatio = defaultDiscardMaxRatio
	}
	if log == nil {
		log = zap.NewNop()
	}
	return &DiscardBreaker{MaxRows: maxRows, MaxRatio: maxRatio, Log: log}
}

// Allow reports whether a pass that would discard `candidates` rows out of
// `total` in the cluster may proceed.
func (b *DiscardBreaker) Allow(candidates, total int64) error {
	if b == nil || candidates == 0 {
		return nil
	}

	maxRows := b.MaxRows
	if maxRows <= 0 {
		maxRows = defaultDiscardMaxRows
	}
	maxRatio := b.MaxRatio
	if maxRatio <= 0 {
		maxRatio = defaultDiscardMaxRatio
	}

	overCount := candidates > maxRows
	// A ratio needs a denominator. With no rows at all there is nothing to be a
	// fraction of, and the absolute limit is the only one that means anything.
	overRatio := total > 0 && float64(candidates)/float64(total) > maxRatio

	if !overCount && !overRatio {
		return nil
	}

	registryReclaimBreakerTripped.Inc()
	log := b.Log
	if log == nil {
		log = zap.NewNop()
	}
	log.Error("refusing to reclaim: this pass would discard more rows than anything here can explain, "+
		"and a discarded row has no snapshot to come back from",
		zap.Int64("candidates", candidates),
		zap.Int64("cluster_rows", total),
		zap.Int64("max_rows", maxRows),
		zap.Float64("max_ratio", maxRatio),
	)
	return fmt.Errorf("%w: %d of %d rows exceeds the limit of %d or %.0f%%",
		ErrDiscardBreakerTripped, candidates, total, maxRows, maxRatio*100)
}
