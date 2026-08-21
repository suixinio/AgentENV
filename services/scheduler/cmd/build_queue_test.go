package main

import (
	"testing"
	"time"

	"agentenv/services/shared/config"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// The build queue's bounds have to reach the two places that act on them, and
// there is nothing in either place that would notice if they did not.
//
// 🔴 The ceiling and the reaper are silent when they are missing. A ceiling
// that never reaches the store just means the store's own default is what runs,
// and the number an operator set does nothing at all — visible only as a
// cluster that refuses the twenty-first build after being configured for four.
// A reaper that is never started is worse and quieter still: nothing happens,
// no error is logged, and the first symptom is a template that can never be
// built again because the build that died is still holding it.

func buildQueueConfig() config.Config {
	cfg := config.Config{}
	cfg.Scheduler.Catalog = config.SchedulerCatalogConfig{
		MaxConcurrentBuilds: 4,
		BuildHeartbeatTTL:   5 * time.Minute,
		BuildReapInterval:   30 * time.Second,
	}
	return cfg
}

// TestTheConfiguredCeilingReachesTheStore.
//
// 🔴 The failure it catches has no error in it. StoreConfig.MaxConcurrentBuilds
// of zero means "take the default", so a ceiling that never leaves the config
// leaves the store running on twenty whatever the operator wrote — and the only
// way anybody finds out is a build refused at a number nobody set.
func TestTheConfiguredCeilingReachesTheStore(t *testing.T) {
	got := catalogStoreConfig(zap.NewNop(), buildQueueConfig(), nil)
	if got.MaxConcurrentBuilds != 4 {
		t.Fatalf("max_concurrent_builds = %d, want 4", got.MaxConcurrentBuilds)
	}

	// And "off" survives the trip: negative removes the ceiling, and a
	// conversion that clamped it to zero would put the default back.
	cfg := buildQueueConfig()
	cfg.Scheduler.Catalog.MaxConcurrentBuilds = -1
	if got := catalogStoreConfig(zap.NewNop(), cfg, nil); got.MaxConcurrentBuilds != -1 {
		t.Fatalf("a ceiling removed on purpose arrived as %d", got.MaxConcurrentBuilds)
	}
}

// TestTheBuildQueueSaysWhatItResolvedTo covers the announcement, which is the
// only place either number appears before it changes somebody's day.
func TestTheBuildQueueSaysWhatItResolvedTo(t *testing.T) {
	core, logs := observer.New(zapcore.DebugLevel)
	announceBuildQueue(zap.New(core), buildQueueConfig())

	entries := logs.All()
	if len(entries) != 1 {
		t.Fatalf("the build queue announced %d lines, want 1", len(entries))
	}
	if entries[0].Level != zapcore.InfoLevel {
		t.Fatalf("an ordinary build queue announced itself at %s", entries[0].Level)
	}
	fields := entries[0].ContextMap()
	for key, want := range map[string]any{
		"max_concurrent_builds": int64(4),
		"build_heartbeat_ttl":   5 * time.Minute,
		"build_reap_interval":   30 * time.Second,
	} {
		if got := fields[key]; got != want {
			t.Fatalf("%s = %v (%T), want %v", key, got, got, want)
		}
	}
}

// TestRemovingTheCeilingIsAnnouncedLoudly. Legal, deliberate — a negative
// number is not something anybody writes by accident — and still worth a line
// at warning level, because with no ceiling the only bound on concurrent builds
// is how many VMs the fleet can boot, and the first symptom of reaching it is
// nodes running out of memory rather than a refusal anybody can read.
func TestRemovingTheCeilingIsAnnouncedLoudly(t *testing.T) {
	cfg := buildQueueConfig()
	cfg.Scheduler.Catalog.MaxConcurrentBuilds = -1

	core, logs := observer.New(zapcore.DebugLevel)
	announceBuildQueue(zap.New(core), cfg)

	entries := logs.All()
	if len(entries) != 1 {
		t.Fatalf("announced %d lines, want 1", len(entries))
	}
	if entries[0].Level != zapcore.WarnLevel {
		t.Fatalf("a cluster with no build ceiling announced itself at %s", entries[0].Level)
	}
}
