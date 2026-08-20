package config

import (
	"testing"
	"time"
)

func clearBindingSweepEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"SCHEDULER_ROUTING_BINDING_SWEEP",
		"SCHEDULER_BINDING_SWEEP_SILENCE",
		"SCHEDULER_BINDING_SWEEP_INTERVAL",
		"SCHEDULER_REPORT_TTL",
	} {
		t.Setenv(key, "")
	}
}

// TestBindingSweepDefaultsOff is the assertion that keeps a scheduler upgrade
// from starting to delete routing records on its own.
//
// 🔴 Every node in a running fleet is already heartbeating, so the sweep's
// inputs exist the moment the binary does. A default of on would mean a cluster
// that rolled this build acquired a timer that retires routing records, having
// asked for nothing — and the failure mode of that timer, if the guard were
// ever wrong, is a live sandbox losing its route.
func TestBindingSweepDefaultsOff(t *testing.T) {
	clearBindingSweepEnv(t)

	cfg, err := Load("", "scheduler")
	if err != nil {
		t.Fatalf("load scheduler config: %v", err)
	}
	if cfg.Scheduler.Routing.BindingSweep {
		t.Fatal("scheduler.routing.binding_sweep defaults on")
	}
	if cfg.Scheduler.BindingSweepSilence != 5*time.Minute {
		t.Fatalf("default silence = %s, want 5m", cfg.Scheduler.BindingSweepSilence)
	}
	if cfg.Scheduler.BindingSweepInterval != 30*time.Second {
		t.Fatalf("default interval = %s, want 30s", cfg.Scheduler.BindingSweepInterval)
	}
}

// TestBindingSweepSilenceOutlastsAnyRoutineAbsence states the threshold's
// justification as an assertion rather than as a comment.
//
// The three numbers it stands above are measured, not chosen:
//
//   - report_ttl (30s) is when a node reads UNHEALTHY and its heartbeat roster
//     stops being usable as a routing fallback. Retire a record below this and
//     the fallback re-answers with the same dead node.
//   - 60s is MAX_REPORT_BACKOFF (src/observability/reporter.rs): the ceiling a
//     node's report interval doubles to when it cannot reach the scheduler. A
//     node pinned at the ceiling still reports five times inside the threshold.
//   - the observed rollout cost was a single missed heartbeat and a 5s backoff.
func TestBindingSweepSilenceOutlastsAnyRoutineAbsence(t *testing.T) {
	clearBindingSweepEnv(t)

	cfg, err := Load("", "scheduler")
	if err != nil {
		t.Fatalf("load scheduler config: %v", err)
	}
	if cfg.Scheduler.BindingSweepSilence <= cfg.Scheduler.ReportTTL {
		t.Fatalf("silence %s is not above report_ttl %s",
			cfg.Scheduler.BindingSweepSilence, cfg.Scheduler.ReportTTL)
	}
	if cfg.Scheduler.BindingSweepSilence <= 60*time.Second {
		t.Fatalf("silence %s does not outlast the node's report backoff ceiling",
			cfg.Scheduler.BindingSweepSilence)
	}
	// 🔴 And far below what it exists to cut short. A threshold near the
	// projection ceiling would leave F4 exactly where it was.
	if cfg.Scheduler.BindingSweepSilence >= cfg.Scheduler.MaxProjectionTTL/10 {
		t.Fatalf("silence %s is not meaningfully shorter than max_projection_ttl %s",
			cfg.Scheduler.BindingSweepSilence, cfg.Scheduler.MaxProjectionTTL)
	}
}

func TestBindingSweepReadsBothTheFileAndTheEnvironment(t *testing.T) {
	clearBindingSweepEnv(t)

	path := writeProjectionConfig(t, "scheduler.json",
		`{"scheduler":{"routing":{"binding_sweep":true},"binding_sweep_silence":"9m","binding_sweep_interval":"45s"}}`)
	cfg, err := Load(path, "scheduler")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if !cfg.Scheduler.Routing.BindingSweep {
		t.Fatal("the file did not turn the sweep on")
	}
	if cfg.Scheduler.BindingSweepSilence != 9*time.Minute {
		t.Fatalf("file silence = %s, want 9m", cfg.Scheduler.BindingSweepSilence)
	}
	if cfg.Scheduler.BindingSweepInterval != 45*time.Second {
		t.Fatalf("file interval = %s, want 45s", cfg.Scheduler.BindingSweepInterval)
	}

	// 🔴 The environment has to be able to turn it back off without editing a
	// ConfigMap, because that is what the rollback is: `kubectl set env`, which
	// rolls the deployment and is loud.
	t.Setenv("SCHEDULER_ROUTING_BINDING_SWEEP", "off")
	t.Setenv("SCHEDULER_BINDING_SWEEP_SILENCE", "12m")
	cfg, err = Load(path, "scheduler")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Scheduler.Routing.BindingSweep {
		t.Fatal("the environment did not override the file's switch")
	}
	if cfg.Scheduler.BindingSweepSilence != 12*time.Minute {
		t.Fatalf("env silence = %s, want 12m", cfg.Scheduler.BindingSweepSilence)
	}
}

// TestBindingSweepNamingTheRoutingBlockWithoutTheKeyLeavesTheDefault mirrors
// the property the switches beside it already have: a config that names the
// routing block for another reason must not blank this one.
func TestBindingSweepNamingTheRoutingBlockWithoutTheKeyLeavesTheDefault(t *testing.T) {
	clearBindingSweepEnv(t)
	t.Setenv("SCHEDULER_ROUTING_BINDING_SWEEP", "on")

	path := writeProjectionConfig(t, "scheduler.json",
		`{"scheduler":{"routing":{"execution_arbitration":"observe"}}}`)
	cfg, err := Load(path, "scheduler")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if !cfg.Scheduler.Routing.BindingSweep {
		t.Fatal("naming the routing block without binding_sweep blanked it")
	}
	if cfg.Scheduler.Routing.ExecutionArbitration != SchedulerExecutionArbitrationObserve {
		t.Fatalf("execution_arbitration = %q", cfg.Scheduler.Routing.ExecutionArbitration)
	}
}

// TestBindingSweepRejectsUnparseableValues. Both durations are refused rather
// than silently defaulted: a threshold that did not parse is a sweep running on
// a number nobody chose.
func TestBindingSweepRejectsUnparseableValues(t *testing.T) {
	clearBindingSweepEnv(t)

	for _, tc := range []struct{ key, value string }{
		{"SCHEDULER_ROUTING_BINDING_SWEEP", "maybe"},
		{"SCHEDULER_BINDING_SWEEP_SILENCE", "five minutes"},
		{"SCHEDULER_BINDING_SWEEP_INTERVAL", "30"},
	} {
		t.Run(tc.key, func(t *testing.T) {
			t.Setenv(tc.key, tc.value)
			if _, err := Load("", "scheduler"); err == nil {
				t.Fatalf("%s=%q was accepted", tc.key, tc.value)
			}
		})
	}

	// 🔴 A numeric duration in the file is refused the way every other
	// scheduler duration is: "30" is thirty nanoseconds to encoding/json and
	// thirty seconds to whoever wrote it.
	path := writeProjectionConfig(t, "scheduler.json", `{"scheduler":{"binding_sweep_silence":300}}`)
	if _, err := Load(path, "scheduler"); err == nil {
		t.Fatal("a numeric binding_sweep_silence was accepted")
	}
}
