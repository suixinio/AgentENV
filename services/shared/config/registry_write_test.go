package config

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// writeEnabledRegistry is a registry block whose read half is already valid, so
// each case below fails for the one reason it is testing.
func writeEnabledRegistry() SchedulerRegistryConfig {
	return SchedulerRegistryConfig{
		DSN:                 "postgres://writer@db:5432/agentenv",
		ClusterID:           "11111111-aaaa-4aaa-8aaa-111111111111",
		MaxConnections:      4,
		ReconcileInterval:   30 * time.Second,
		QueryTimeout:        5 * time.Second,
		LeaseWarnWindow:     30 * time.Second,
		WriteEnabled:        true,
		WriteMaxConnections: 8,
		LeaseTTL:            90 * time.Second,
		ReclaimInterval:     30 * time.Second,
		DiscardMaxRows:      10,
		DiscardMaxRatio:     0.1,
		LeaseTTLFloor:       30 * time.Second,
	}
}

// TestAMissingClusterScopeDoesNotStopTheProcessStarting.
//
// 🔴 The write surface does need a cluster scope — reclaiming every cluster in
// a shared database deletes rows this controller was never given. But the
// cluster id arrives from an optional Secret key, so "missing" is a thing that
// happens on a rollout, and rejecting it here would mean the scheduler does not
// start at all: routing, discovery and bindings would stop over a registry that
// was switched on last week.
//
// The refusal belongs at run time, where it can be narrow — the write surface
// stays cold and says why, and everything else carries on. See
// openRegistryWriteSurface.
func TestAMissingClusterScopeDoesNotStopTheProcessStarting(t *testing.T) {
	for _, writeEnabled := range []bool{false, true} {
		cfg := writeEnabledRegistry()
		cfg.WriteEnabled = writeEnabled
		cfg.ClusterID = ""
		if err := validateSchedulerRegistry(cfg); err != nil {
			t.Fatalf("write_enabled=%v: a missing cluster id must not stop start-up, got %v", writeEnabled, err)
		}
	}

	// A cluster id that is present but not a uuid is a different thing: it
	// cannot have come from an absent Secret, and every query built from it
	// would fail forever.
	cfg := writeEnabledRegistry()
	cfg.ClusterID = "dev-cluster"
	if err := validateSchedulerRegistry(cfg); err == nil {
		t.Fatal("a cluster id that is not a uuid was accepted")
	} else if !strings.Contains(err.Error(), "cluster_id") {
		t.Fatalf("the error should name the field, got %v", err)
	}
}

func TestTheWriteSurfaceRejectsBadValues(t *testing.T) {
	if err := validateSchedulerRegistry(writeEnabledRegistry()); err != nil {
		t.Fatalf("expected a well formed write config to validate, got %v", err)
	}

	cases := map[string]func(*SchedulerRegistryConfig){
		"write_max_connections":  func(c *SchedulerRegistryConfig) { c.WriteMaxConnections = 0 },
		"lease_ttl":              func(c *SchedulerRegistryConfig) { c.LeaseTTL = 0 },
		"reclaim_interval":       func(c *SchedulerRegistryConfig) { c.ReclaimInterval = 0 },
		"discard_max_rows":       func(c *SchedulerRegistryConfig) { c.DiscardMaxRows = 0 },
		"discard_max_ratio zero": func(c *SchedulerRegistryConfig) { c.DiscardMaxRatio = 0 },
		"lease_ttl_floor":        func(c *SchedulerRegistryConfig) { c.LeaseTTLFloor = 0 },
		// A ratio above one can never trip, which is a breaker that is switched
		// off while looking switched on.
		"discard_max_ratio above one": func(c *SchedulerRegistryConfig) { c.DiscardMaxRatio = 1.5 },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			cfg := writeEnabledRegistry()
			mutate(&cfg)
			if err := validateSchedulerRegistry(cfg); err == nil {
				t.Fatalf("expected %s to be rejected", name)
			}
		})
	}
}

// TestTheWriteSurfaceIsOffByDefault.
//
// Switching it on makes this process the owner of a table the nodes are still
// writing themselves. The two are only safe together inside the changeover
// window, and a default of on would put every existing deployment into that
// window on an upgrade nobody asked for.
func TestTheWriteSurfaceIsOffByDefault(t *testing.T) {
	cfg := defaultConfig("scheduler")
	if cfg.Scheduler.Registry.WriteEnabled {
		t.Fatal("the registry write surface defaults to on")
	}
	if cfg.Scheduler.Registry.LeaseTTL != 90*time.Second {
		t.Fatalf("the default lease should match the node's, got %s", cfg.Scheduler.Registry.LeaseTTL)
	}
	if cfg.Scheduler.Registry.DiscardMaxRows <= 0 || cfg.Scheduler.Registry.DiscardMaxRatio <= 0 {
		t.Fatalf("the discard breaker has no default limits: %+v", cfg.Scheduler.Registry)
	}
	// The floor has to be well under a real lease: it is there to catch a
	// reported zero or a units mix-up, not to second-guess a node whose own
	// configuration already checks its lease against its renewal cadence.
	floor := cfg.Scheduler.Registry.LeaseTTLFloor
	if floor <= 0 {
		t.Fatal("there is no default lease floor, so a node reporting zero would stamp a lease that expires on arrival")
	}
	if floor >= cfg.Scheduler.Registry.LeaseTTL {
		t.Fatalf("the floor (%s) is not below a real lease (%s); it would override healthy nodes instead of catching absurd values",
			floor, cfg.Scheduler.Registry.LeaseTTL)
	}
}

func TestTheWriteBlockDecodesFromJSON(t *testing.T) {
	var registry SchedulerRegistryConfig
	raw := `{
      "write_enabled": true,
      "write_max_connections": 12,
      "lease_ttl": "2m",
      "lease_ttl_floor": "15s",
      "reclaim_interval": "45s",
      "discard_max_rows": 25,
      "discard_max_ratio": 0.25
    }`
	if err := json.Unmarshal([]byte(raw), &registry); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if !registry.WriteEnabled || registry.WriteMaxConnections != 12 {
		t.Fatalf("unexpected: %+v", registry)
	}
	if registry.LeaseTTL != 2*time.Minute || registry.ReclaimInterval != 45*time.Second {
		t.Fatalf("durations: %+v", registry)
	}
	if registry.LeaseTTLFloor != 15*time.Second {
		t.Fatalf("lease_ttl_floor: %+v", registry)
	}
	if registry.DiscardMaxRows != 25 || registry.DiscardMaxRatio != 0.25 {
		t.Fatalf("breaker: %+v", registry)
	}
}

// TestADurationHasToBeADurationString: a bare number here is ambiguous between
// seconds and nanoseconds, and Go's zero-value handling would turn the mistake
// into a default rather than an error.
func TestADurationHasToBeADurationString(t *testing.T) {
	for _, raw := range []string{`{"lease_ttl": 90}`, `{"reclaim_interval": 30}`, `{"lease_ttl_floor": 30}`} {
		var registry SchedulerRegistryConfig
		if err := json.Unmarshal([]byte(raw), &registry); err == nil {
			t.Fatalf("%s was accepted", raw)
		}
	}
}
