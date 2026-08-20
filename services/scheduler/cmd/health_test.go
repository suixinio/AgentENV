package main

import (
	scheduler "agentenv/services/scheduler/internal"
	"agentenv/services/shared/config"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestHealthReportsThePhaseWithoutGatingTheProcess.
//
// 🔴 Always 200. This endpoint says something about one subsystem, and a probe
// wired to it that took the pod out of rotation would answer a registry
// database outage by also stopping the routing and discovery that had nothing
// to do with it — which is the failure the whole start-up path is arranged to
// avoid. The phase is in the body for whoever is looking.
func TestHealthReportsThePhaseWithoutGatingTheProcess(t *testing.T) {
	cases := map[string]struct {
		phase     func() (string, time.Duration, time.Duration)
		clusterID string
		want      string
		ready     bool
		serving   bool
		remaining float64
		fencing   string
	}{
		"off": {phase: nil, want: "off"},
		"cold": {
			phase: func() (string, time.Duration, time.Duration) { return "cold", 0, 0 },
			want:  "cold",
		},
		"grace": {
			phase:     func() (string, time.Duration, time.Duration) { return "grace", 42 * time.Second, 3 * time.Minute },
			want:      "grace",
			ready:     true,
			remaining: 42,
		},
		"serving": {
			phase:     func() (string, time.Duration, time.Duration) { return "serving", 0, 3 * time.Minute },
			clusterID: "11111111-aaaa-4aaa-8aaa-111111111111",
			want:      "serving",
			ready:     true,
			serving:   true,
			fencing:   "enabled",
		},
		// 🔴 The one an operator has to be able to read off this endpoint
		// during an incident: healthy in every other respect, and not checking
		// who is writing to the table.
		"serving with fencing switched off": {
			phase:     func() (string, time.Duration, time.Duration) { return "serving", 0, 3 * time.Minute },
			clusterID: "11111111-aaaa-4aaa-8aaa-111111111111",
			want:      "serving",
			ready:     true,
			serving:   true,
			fencing:   "disabled",
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			fencing := tc.fencing
			if fencing == "" {
				fencing = "enabled"
			}
			rec := httptest.NewRecorder()
			registryHealthHandler(tc.clusterID, tc.phase, healthSwitches{
				writeFencing:         fencing,
				executionArbitration: "enforce",
			})(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))

			if rec.Code != http.StatusOK {
				t.Fatalf("status: got %d, want 200 — this endpoint must not gate the process", rec.Code)
			}

			var body struct {
				Status        string `json:"status"`
				RegistryWrite struct {
					Phase     string  `json:"phase"`
					Ready     bool    `json:"ready"`
					Serving   bool    `json:"serving"`
					ClusterID string  `json:"cluster_id"`
					Remaining float64 `json:"grace_remaining_seconds"`
					Downtime  float64 `json:"inferred_downtime_seconds"`
					Fencing   string  `json:"write_fencing"`
				} `json:"registry_write"`
				Routing struct {
					ExecutionArbitration string `json:"execution_arbitration"`
				} `json:"routing"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatalf("decode %s: %v", rec.Body.String(), err)
			}
			if body.Status != "ok" {
				t.Fatalf("status: %q", body.Status)
			}
			if body.RegistryWrite.Phase != tc.want {
				t.Fatalf("phase: got %q, want %q", body.RegistryWrite.Phase, tc.want)
			}
			// Ready and serving are reported apart because they mean different
			// things: one says the migration is done, the other that the
			// restart grace window has closed and takeovers are allowed again.
			if tc.phase != nil {
				if body.RegistryWrite.Ready != tc.ready {
					t.Fatalf("ready: got %v, want %v", body.RegistryWrite.Ready, tc.ready)
				}
				if body.RegistryWrite.Serving != tc.serving {
					t.Fatalf("serving: got %v, want %v", body.RegistryWrite.Serving, tc.serving)
				}
				if body.RegistryWrite.Remaining != tc.remaining {
					t.Fatalf("grace_remaining_seconds: got %v, want %v", body.RegistryWrite.Remaining, tc.remaining)
				}
				// 🔴 The scope the surface is actually running with, reported
				// whether or not there is one. A write surface with no cluster
				// id is cold forever while every other health signal says the
				// process is fine, and the startup error that explains it has
				// scrolled away by the time anyone looks; an empty cluster_id
				// here is the difference between "the database is down" and
				// "nothing ever told it which cluster it serves".
				if body.RegistryWrite.ClusterID != tc.clusterID {
					t.Fatalf("cluster_id: got %q, want %q", body.RegistryWrite.ClusterID, tc.clusterID)
				}
				// 🔴 Both halves of this release can be switched off by
				// configuration, and neither failure is visible in traffic
				// until it has already cost something. This endpoint is where
				// an operator finds out.
				if body.RegistryWrite.Fencing != fencing {
					t.Fatalf("write_fencing: got %q, want %q", body.RegistryWrite.Fencing, fencing)
				}
			}
			if body.Routing.ExecutionArbitration != "enforce" {
				t.Fatalf("routing.execution_arbitration: got %q, want %q", body.Routing.ExecutionArbitration, "enforce")
			}
		})
	}
}

// TestTheWriteSurfaceNeedsBothItsSwitches: a DSN alone does not make this
// process the table's owner, and neither does the flag without one.
func TestTheWriteSurfaceNeedsBothItsSwitches(t *testing.T) {
	cfg := defaultTestConfig()
	if registryWriteEnabled(cfg) {
		t.Fatal("a config with neither switch enabled the write surface")
	}

	cfg.Scheduler.Registry.DSN = "postgres://writer@db/agentenv"
	if registryWriteEnabled(cfg) {
		t.Fatal("a DSN alone enabled the write surface")
	}

	cfg.Scheduler.Registry.DSN = ""
	cfg.Scheduler.Registry.WriteEnabled = true
	if registryWriteEnabled(cfg) {
		t.Fatal("the flag alone enabled the write surface, with nothing to connect to")
	}

	cfg.Scheduler.Registry.DSN = "postgres://writer@db/agentenv"
	if !registryWriteEnabled(cfg) {
		t.Fatal("both switches set did not enable the write surface")
	}
}

func defaultTestConfig() config.Config { return config.Config{} }

// TestAQueryOnlyReplicaNeverOwnsTheTable.
//
// 🔴 Query-only replicas exist so sandbox lookups survive a primary restart.
// There is exactly one owner of this table's shape, and of the reclamation
// timer that deletes rows from it; a replica that migrated and reclaimed
// alongside the primary would be a second one — and the two would race on the
// bootstrap advisory lock every time either restarted.
func TestAQueryOnlyReplicaNeverOwnsTheTable(t *testing.T) {
	cfg := defaultTestConfig()
	cfg.Scheduler.Registry.DSN = "postgres://writer@db/agentenv"
	cfg.Scheduler.Registry.WriteEnabled = true
	cfg.Scheduler.Registry.LeaseTTL = 90 * time.Second

	store, grace, closeStore := createRegistryStore(zap.NewNop(), cfg, true)
	defer closeStore()

	if store != nil || grace != nil {
		t.Fatal("a query-only replica built the write surface")
	}
}

// TestAWriteSurfaceWithNoClusterScopeStaysCold.
//
// 🔴 Registered and cold, not absent. Reclaiming every cluster in a shared
// database would delete rows this controller was never given, and the restart
// grace pass would extend one cluster's leases while serving another's writes
// with none of theirs extended — so it must not open. But the cluster id
// arrives from an optional Secret key, so refusing to start would answer a
// missing key by stopping routing and discovery too, and reporting the surface
// as "off" would hide that it was configured on.
func TestAWriteSurfaceWithNoClusterScopeStaysCold(t *testing.T) {
	cfg := defaultTestConfig()
	cfg.Scheduler.Registry.DSN = "postgres://writer@db/agentenv"
	cfg.Scheduler.Registry.WriteEnabled = true

	if !registryWriteEnabled(cfg) {
		t.Fatal("the surface should still be built and registered without a cluster id")
	}
	if registryWriteScoped(cfg) {
		t.Fatal("the surface opened without a cluster scope; reclamation would reach every cluster in the database")
	}

	cfg.Scheduler.Registry.ClusterID = "11111111-aaaa-4aaa-8aaa-111111111111"
	if !registryWriteScoped(cfg) {
		t.Fatal("a scoped, enabled write surface did not open")
	}

	// Whitespace is a Secret key that got mangled, not a cluster id.
	cfg.Scheduler.Registry.ClusterID = "   "
	if registryWriteScoped(cfg) {
		t.Fatal("a blank cluster id opened the write surface")
	}
}

// registryGaugeNames are the three series an operator reads together to tell
// the start-up shapes apart. Spelled out rather than gathered by prefix, so a
// rename fails here instead of quietly matching nothing.
var registryGaugeNames = [3]string{
	"agentenv_scheduler_registry_enabled",
	"agentenv_scheduler_registry_write_surface_enabled",
	"agentenv_scheduler_registry_write_fencing_enabled",
}

// registryGauges reads the three back in that order.
func registryGauges(t *testing.T) [3]float64 {
	t.Helper()

	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gather failed: %v", err)
	}

	var values [3]float64
	for i, name := range registryGaugeNames {
		found := false
		for _, family := range families {
			if family.GetName() != name {
				continue
			}
			metrics := family.GetMetric()
			if len(metrics) != 1 {
				t.Fatalf("%s has %d series; it is meant to be a single unlabelled gauge", name, len(metrics))
			}
			values[i] = metrics[0].GetGauge().GetValue()
			found = true
		}
		if !found {
			t.Fatalf("%s is not registered; an operator reading the tuple would see two of three", name)
		}
	}
	return values
}

// setRegistryGauges forces all three to a known state, so the assertions below
// are about what the start-up path wrote and not about what a previous case
// left behind.
func setRegistryGauges(t *testing.T, values [3]float64) {
	t.Helper()

	scheduler.SetRegistryEnabled(values[0] == 1)
	scheduler.SetRegistryWriteSurfaceEnabled(values[1] == 1)
	scheduler.SetRegistryWriteFencingEnabled(values[2] == 1)
}

// TestTheRegistryGaugesTellTheStartupShapesApart.
//
// 🔴 The question these answer is asked months later, during an incident, about
// a process whose logs are long gone: "is the half that costs a workspace
// switched on here?". The fencing gauge alone cannot answer it. It is written
// where the write store is built, and that function returns early on a
// query-only replica and on a cluster with write_enabled=false — so a 0 meant
// either "fencing is off" or "there is no write surface here", and every
// read-only replica in the fleet published the same 0 as the one cluster that
// needed an operator.
//
// So the shapes are driven through the real start-up path and their tuples are
// required to differ. Each case first forces all three gauges to the opposite
// of what it expects, which is what makes this a test of the start-up path
// rather than of whatever ran before it: a path that leaves a gauge unwritten
// fails here instead of inheriting a plausible value.
//
// No database is involved — neither constructor connects, by design, because a
// registry that is down must not stop this process from routing traffic.
func TestTheRegistryGaugesTellTheStartupShapesApart(t *testing.T) {
	const dsn = "postgres://writer@db/agentenv"

	cases := []struct {
		name         string
		dsn          string
		writeEnabled bool
		writeFencing bool
		queryOnly    bool
		want         [3]float64
	}{
		{
			name:         "write surface, fencing on",
			dsn:          dsn,
			writeEnabled: true,
			writeFencing: true,
			want:         [3]float64{1, 1, 1},
		},
		{
			// 🔴 The one shape that needs an operator, and the only one that
			// may read 0 on the fencing gauge.
			name:         "write surface, fencing off",
			dsn:          dsn,
			writeEnabled: true,
			writeFencing: false,
			want:         [3]float64{1, 1, 0},
		},
		{
			// Configured with a DSN and reading from it, writing nothing. Also
			// the shape of a --query-only replica, which is covered below by
			// the same expectation.
			name: "no write surface",
			dsn:  dsn,
			want: [3]float64{1, 0, 0},
		},
		{
			name: "no registry at all",
			want: [3]float64{0, 0, 0},
		},
	}

	seen := map[[3]float64]string{}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Every gauge starts at the opposite of its expectation.
			setRegistryGauges(t, [3]float64{1 - tc.want[0], 1 - tc.want[1], 1 - tc.want[2]})

			cfg := defaultTestConfig()
			cfg.Scheduler.Registry.DSN = tc.dsn
			cfg.Scheduler.Registry.WriteEnabled = tc.writeEnabled
			cfg.Scheduler.Registry.WriteFencing = tc.writeFencing
			cfg.Scheduler.Registry.LeaseTTL = 90 * time.Second

			logger := zap.NewNop()
			_, closeReader := createRegistryReader(logger, cfg)
			defer closeReader()
			_, _, closeStore := createRegistryStore(logger, cfg, tc.queryOnly)
			defer closeStore()

			if got := registryGauges(t); got != tc.want {
				t.Fatalf("%v = %v, want %v", registryGaugeNames, got, tc.want)
			}
		})

		if other, clash := seen[tc.want]; clash {
			t.Fatalf("%q and %q publish the same tuple %v; the gauges cannot tell them apart",
				other, tc.name, tc.want)
		}
		seen[tc.want] = tc.name
	}
}

// TestAQueryOnlyReplicaReadsAsNoWriteSurface pins the replica onto the same
// tuple as a cluster with the write flag off, which is deliberate: neither
// writes, so neither has fencing to report. What must not happen is the replica
// reading like a cluster whose fencing was switched off.
func TestAQueryOnlyReplicaReadsAsNoWriteSurface(t *testing.T) {
	setRegistryGauges(t, [3]float64{1, 1, 1})

	cfg := defaultTestConfig()
	cfg.Scheduler.Registry.DSN = "postgres://writer@db/agentenv"
	cfg.Scheduler.Registry.WriteEnabled = true
	cfg.Scheduler.Registry.WriteFencing = true
	cfg.Scheduler.Registry.LeaseTTL = 90 * time.Second

	logger := zap.NewNop()
	_, closeReader := createRegistryReader(logger, cfg)
	defer closeReader()
	_, _, closeStore := createRegistryStore(logger, cfg, true)
	defer closeStore()

	if got, want := registryGauges(t), [3]float64{1, 0, 0}; got != want {
		t.Fatalf("%v = %v, want %v — a replica that published the write-surface gauge would put "+
			"every read-only pod into an alert meant for the cluster that owns the table",
			registryGaugeNames, got, want)
	}
}
