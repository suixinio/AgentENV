package config

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// The build queue's three numbers, and the two of them that can cost a user
// work if they are wrong.
//
// 🔴 The asymmetry these tests are shaped around: a ceiling set too low refuses
// a build and the user retries; a heartbeat TTL set too short *ends builds that
// are still running*, tells the user their heartbeat lapsed when it never did,
// and fails the template row alongside. So the TTL has a floor and the ceiling
// does not.

func schedulerWithCatalog(catalog SchedulerCatalogConfig) Config {
	cfg := defaultConfig("scheduler")
	cfg.Scheduler.Catalog = catalog
	return cfg
}

func TestTheBuildQueueHasBoundsWithoutBeingConfigured(t *testing.T) {
	cfg := defaultConfig("scheduler")

	catalog := cfg.Scheduler.Catalog
	if catalog.MaxConcurrentBuilds <= 0 {
		t.Fatalf("a cluster that configures nothing has no build ceiling: %+v", catalog)
	}
	if catalog.BuildHeartbeatTTL < schedulerCatalogBuildHeartbeatTTLFloor {
		t.Fatalf("the default TTL %s is below the floor this package refuses", catalog.BuildHeartbeatTTL)
	}
	// 🔴 The reaper holds itself back for a full TTL after it can first hear a
	// heartbeat, so an interval at or above the TTL doubles how long a template
	// stays shut after everyone already knows its build is gone.
	if catalog.BuildReapInterval >= catalog.BuildHeartbeatTTL {
		t.Fatalf("the default reap interval %s is not below the default TTL %s",
			catalog.BuildReapInterval, catalog.BuildHeartbeatTTL)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("the defaults do not validate: %v", err)
	}
}

// TestAHeartbeatTTLTooShortToBeMeantIsRefused is the one that stops a mistyped
// unit from becoming a cluster that reaps its own builds on schedule.
func TestAHeartbeatTTLTooShortToBeMeantIsRefused(t *testing.T) {
	for _, ttl := range []time.Duration{
		0,
		-time.Minute,
		// A duration written as if it were milliseconds.
		300 * time.Nanosecond,
		// One tick under the floor.
		schedulerCatalogBuildHeartbeatTTLFloor - time.Millisecond,
	} {
		cfg := schedulerWithCatalog(SchedulerCatalogConfig{
			MaxConcurrentBuilds:        20,
			BuildHeartbeatTTL:          ttl,
			BuildReapInterval:          time.Nanosecond,
			NodeBuildHeartbeatInterval: time.Nanosecond,
		})
		if err := cfg.Validate(); err == nil {
			t.Fatalf("a heartbeat TTL of %s was accepted", ttl)
		}
	}

	// And the floor itself is allowed: it is a floor, not a minimum somebody
	// has to clear. It takes saying that the nodes renew fast enough for it,
	// which is the other half of the pair — see
	// TestATTLBelowTheClusterSNodeRenewalCadenceIsRefused.
	cfg := schedulerWithCatalog(SchedulerCatalogConfig{
		MaxConcurrentBuilds:        20,
		BuildHeartbeatTTL:          schedulerCatalogBuildHeartbeatTTLFloor,
		BuildReapInterval:          time.Second,
		NodeBuildHeartbeatInterval: schedulerCatalogBuildHeartbeatTTLFloor / schedulerCatalogBuildHeartbeatTTLMissedRenewals,
	})
	if err := cfg.Validate(); err != nil {
		t.Fatalf("the floor itself was refused: %v", err)
	}
}

func TestAReapIntervalLongerThanTheTTLIsRefused(t *testing.T) {
	cfg := schedulerWithCatalog(SchedulerCatalogConfig{
		MaxConcurrentBuilds:        20,
		BuildHeartbeatTTL:          time.Minute,
		BuildReapInterval:          2 * time.Minute,
		NodeBuildHeartbeatInterval: 10 * time.Second,
	})
	err := cfg.Validate()
	if err == nil {
		t.Fatal("an interval longer than the TTL was accepted")
	}
	if !strings.Contains(err.Error(), "build_reap_interval") {
		t.Fatalf("the error does not name the setting: %v", err)
	}

	cfg.Scheduler.Catalog.BuildReapInterval = 0
	if err := cfg.Validate(); err == nil {
		t.Fatal("an interval of zero was accepted, which is a reaper that never runs")
	}
}

// TestNoCeilingTakesWritingANegativeNumber keeps "unset" and "off" apart.
//
// 🔴 Zero means "take the default", because a field somebody forgot must not be
// the field that removes the cluster's only limit on concurrent build VMs. Off
// is available and it takes stating a negative number, which nobody does by
// accident.
func TestNoCeilingTakesWritingANegativeNumber(t *testing.T) {
	var parsed SchedulerConfig
	if err := json.Unmarshal([]byte(`{"catalog":{"build_heartbeat_ttl":"5m"}}`), &parsed); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if parsed.Catalog.MaxConcurrentBuilds != 0 {
		t.Fatalf("an unnamed ceiling decoded to %d, want the zero the store reads as 'default'",
			parsed.Catalog.MaxConcurrentBuilds)
	}

	cfg := schedulerWithCatalog(SchedulerCatalogConfig{
		MaxConcurrentBuilds:        -1,
		BuildHeartbeatTTL:          5 * time.Minute,
		BuildReapInterval:          30 * time.Second,
		NodeBuildHeartbeatInterval: defaultSchedulerCatalogNodeBuildHeartbeatInterval,
	})
	if err := cfg.Validate(); err != nil {
		t.Fatalf("removing the ceiling on purpose was refused: %v", err)
	}
}

// TestNamingOneCatalogKeyKeepsTheDefaultsForTheOthers is the decoding rule that
// makes the floor above reachable only on purpose.
//
// 🔴 Decoded into the existing value rather than through a pointer. Through a
// pointer, a config naming only the ceiling would blank the TTL to zero — and
// zero is below the floor, so the process would refuse to start over a key
// nobody wrote.
func TestNamingOneCatalogKeyKeepsTheDefaultsForTheOthers(t *testing.T) {
	cfg := defaultConfig("scheduler")
	if err := json.Unmarshal([]byte(`{"scheduler":{"catalog":{"max_concurrent_builds":4}}}`), &cfg); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if cfg.Scheduler.Catalog.MaxConcurrentBuilds != 4 {
		t.Fatalf("ceiling = %d, want 4", cfg.Scheduler.Catalog.MaxConcurrentBuilds)
	}
	if cfg.Scheduler.Catalog.BuildHeartbeatTTL != defaultSchedulerCatalogBuildHeartbeatTTL {
		t.Fatalf("naming one key blanked the TTL: %s", cfg.Scheduler.Catalog.BuildHeartbeatTTL)
	}
	if cfg.Scheduler.Catalog.BuildReapInterval != defaultSchedulerCatalogBuildReapInterval {
		t.Fatalf("naming one key blanked the interval: %s", cfg.Scheduler.Catalog.BuildReapInterval)
	}
	if cfg.Scheduler.Catalog.NodeBuildHeartbeatInterval != defaultSchedulerCatalogNodeBuildHeartbeatInterval {
		t.Fatalf("naming one key blanked the node renewal cadence: %s",
			cfg.Scheduler.Catalog.NodeBuildHeartbeatInterval)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
}

// TestTheBuildQueueIsReachableFromTheEnvironment is recon's ConfigMap drift:
// a value edited into the mounted file is put back by the next apply, so the
// numbers an operator reaches for during an incident have to have an env of
// their own.
func TestTheBuildQueueIsReachableFromTheEnvironment(t *testing.T) {
	t.Setenv("SCHEDULER_CATALOG_MAX_CONCURRENT_BUILDS", "7")
	t.Setenv("SCHEDULER_CATALOG_BUILD_HEARTBEAT_TTL", "9m")
	t.Setenv("SCHEDULER_CATALOG_BUILD_REAP_INTERVAL", "45s")
	t.Setenv("SCHEDULER_CATALOG_NODE_BUILD_HEARTBEAT_INTERVAL", "60s")

	cfg := defaultConfig("scheduler")
	if err := overrideWithEnv(&cfg); err != nil {
		t.Fatalf("override: %v", err)
	}
	if cfg.Scheduler.Catalog.MaxConcurrentBuilds != 7 {
		t.Fatalf("ceiling = %d, want 7", cfg.Scheduler.Catalog.MaxConcurrentBuilds)
	}
	if cfg.Scheduler.Catalog.BuildHeartbeatTTL != 9*time.Minute {
		t.Fatalf("ttl = %s, want 9m", cfg.Scheduler.Catalog.BuildHeartbeatTTL)
	}
	if cfg.Scheduler.Catalog.BuildReapInterval != 45*time.Second {
		t.Fatalf("interval = %s, want 45s", cfg.Scheduler.Catalog.BuildReapInterval)
	}
	if cfg.Scheduler.Catalog.NodeBuildHeartbeatInterval != 60*time.Second {
		t.Fatalf("node renewal cadence = %s, want 60s", cfg.Scheduler.Catalog.NodeBuildHeartbeatInterval)
	}

	for _, key := range []string{
		"SCHEDULER_CATALOG_MAX_CONCURRENT_BUILDS",
		"SCHEDULER_CATALOG_BUILD_HEARTBEAT_TTL",
		"SCHEDULER_CATALOG_BUILD_REAP_INTERVAL",
		"SCHEDULER_CATALOG_NODE_BUILD_HEARTBEAT_INTERVAL",
	} {
		t.Run(key+" refuses nonsense", func(t *testing.T) {
			t.Setenv(key, "not-a-value")
			cfg := defaultConfig("scheduler")
			if err := overrideWithEnv(&cfg); err == nil {
				t.Fatalf("%s accepted a value it cannot parse, which would leave the default silently in place", key)
			}
		})
	}
}

// TestATTLBelowTheClusterSNodeRenewalCadenceIsRefused is the check the absolute
// floor could not make.
//
// 🔴 The reaper ends a build it has not heard from within the TTL, and what it
// hears from is a node renewing on a cadence configured in a different process
// — `snapshot.catalog.build_heartbeat_interval_secs` in src/cfg.rs, 100 seconds
// by default. The floor was 30 seconds, so every TTL in [30s, 100s] passed
// validation and would have reaped every healthy build in the cluster, on
// schedule, each one reported as a lapsed heartbeat. Nothing was ever armed
// because the cluster default is 5 minutes, which is luck rather than design:
// the two numbers had no relationship the process could check.
func TestATTLBelowTheClusterSNodeRenewalCadenceIsRefused(t *testing.T) {
	// The window the old floor let through, at the shipped node cadence.
	for _, ttl := range []time.Duration{
		schedulerCatalogBuildHeartbeatTTLFloor,
		time.Minute,
		defaultSchedulerCatalogNodeBuildHeartbeatInterval,
		2 * defaultSchedulerCatalogNodeBuildHeartbeatInterval,
	} {
		cfg := schedulerWithCatalog(SchedulerCatalogConfig{
			MaxConcurrentBuilds:        20,
			BuildHeartbeatTTL:          ttl,
			BuildReapInterval:          time.Second,
			NodeBuildHeartbeatInterval: defaultSchedulerCatalogNodeBuildHeartbeatInterval,
		})
		err := cfg.Validate()
		if err == nil {
			t.Fatalf("a TTL of %s was accepted against nodes renewing every %s: every build in "+
				"the cluster would be reaped while it ran",
				ttl, defaultSchedulerCatalogNodeBuildHeartbeatInterval)
		}
		// The message has to name both halves, because the fix may be in
		// either process.
		for _, needle := range []string{"build_heartbeat_ttl", "node_build_heartbeat_interval", "build_heartbeat_interval_secs"} {
			if !strings.Contains(err.Error(), needle) {
				t.Fatalf("the refusal does not mention %q, so it does not say where the fix is: %v", needle, err)
			}
		}
	}

	// 🔴 And the escape is real: a cluster whose nodes genuinely renew faster
	// may say so and take the shorter TTL. Without this the floor would be a
	// number nobody can move, and the way round it would be to remove it.
	cfg := schedulerWithCatalog(SchedulerCatalogConfig{
		MaxConcurrentBuilds:        20,
		BuildHeartbeatTTL:          time.Minute,
		BuildReapInterval:          time.Second,
		NodeBuildHeartbeatInterval: 20 * time.Second,
	})
	if err := cfg.Validate(); err != nil {
		t.Fatalf("a TTL of three renewals was refused: %v", err)
	}
}

// The default TTL is exactly the number the rule asks for, which is the thing
// that made the old floor look adequate. Say it out loud so that moving either
// end moves this test.
func TestTheDefaultTTLIsThreeOfTheDefaultRenewals(t *testing.T) {
	want := time.Duration(schedulerCatalogBuildHeartbeatTTLMissedRenewals) * defaultSchedulerCatalogNodeBuildHeartbeatInterval
	if defaultSchedulerCatalogBuildHeartbeatTTL != want {
		t.Fatalf("the default TTL is %s but %d renewals of %s is %s: the pair has drifted, and "+
			"the node's snapshot.catalog.build_heartbeat_interval_secs is the other half to check",
			defaultSchedulerCatalogBuildHeartbeatTTL,
			schedulerCatalogBuildHeartbeatTTLMissedRenewals,
			defaultSchedulerCatalogNodeBuildHeartbeatInterval, want)
	}
}

// A renewal cadence of zero is a divide-by-nothing rule, so it is refused
// rather than read as "no check".
func TestARenewalCadenceOfZeroIsRefused(t *testing.T) {
	cfg := schedulerWithCatalog(SchedulerCatalogConfig{
		MaxConcurrentBuilds:        20,
		BuildHeartbeatTTL:          5 * time.Minute,
		BuildReapInterval:          30 * time.Second,
		NodeBuildHeartbeatInterval: 0,
	})
	if err := cfg.Validate(); err == nil {
		t.Fatal("an unset node renewal cadence was accepted, which turns the relationship check off")
	}
}
