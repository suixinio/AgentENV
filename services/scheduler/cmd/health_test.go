package main

import (
	"agentenv/services/shared/config"

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
		want      string
		ready     bool
		serving   bool
		remaining float64
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
			phase:   func() (string, time.Duration, time.Duration) { return "serving", 0, 3 * time.Minute },
			want:    "serving",
			ready:   true,
			serving: true,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			registryHealthHandler(tc.phase)(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))

			if rec.Code != http.StatusOK {
				t.Fatalf("status: got %d, want 200 — this endpoint must not gate the process", rec.Code)
			}

			var body struct {
				Status        string `json:"status"`
				RegistryWrite struct {
					Phase     string  `json:"phase"`
					Ready     bool    `json:"ready"`
					Serving   bool    `json:"serving"`
					Remaining float64 `json:"grace_remaining_seconds"`
					Downtime  float64 `json:"inferred_downtime_seconds"`
				} `json:"registry_write"`
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
