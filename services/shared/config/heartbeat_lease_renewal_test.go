package config

import (
	"encoding/json"
	"testing"
)

// scheduler.registry.heartbeat_lease_renewal is the switch over the
// scheduler's heartbeat-driven paused-lease renewal (see
// services/scheduler/internal/reconcile.go's WithHeartbeatLeaseRenewal).
//
// Unlike write_fencing beside it, this one defaults *off* — see the field's
// own doc for why — so these tests pin the opposite direction: not only that
// the switch itself starts off, but that adding it did not disturb
// write_fencing's own default, which every assertion below carries as its
// control.

// TestHeartbeatLeaseRenewalDefaultsOff is the one property that keeps a
// cluster that has never heard of this setting behaving exactly as it did
// before it existed.
func TestHeartbeatLeaseRenewalDefaultsOff(t *testing.T) {
	cfg := defaultConfig("scheduler")

	if cfg.Scheduler.Registry.HeartbeatLeaseRenewal {
		t.Fatal("scheduler.registry.heartbeat_lease_renewal defaults on; a cluster that never asked for it would acquire a new write path on upgrade alone")
	}
	// The control: this field's addition must not have moved write_fencing's
	// own default, which stays *on* for the opposite reason (see
	// TestTheTwoSchedulerSwitchesDefaultToOn).
	if !cfg.Scheduler.Registry.WriteFencing {
		t.Fatal("adding heartbeat_lease_renewal must not disturb write_fencing's own default")
	}
}

// TestHeartbeatLeaseRenewalFollowsItsOwnJSONKey mirrors
// TestTheThreeSwitchesAreSeparateSettings: naming this key in the config file
// must both take effect on its own and leave its neighbour alone.
func TestHeartbeatLeaseRenewalFollowsItsOwnJSONKey(t *testing.T) {
	raw := `{"scheduler": {"registry": {"heartbeat_lease_renewal": true}}}`

	cfg := defaultConfig("scheduler")
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if !cfg.Scheduler.Registry.HeartbeatLeaseRenewal {
		t.Fatal("scheduler.registry.heartbeat_lease_renewal did not follow its own key")
	}
	if !cfg.Scheduler.Registry.WriteFencing {
		t.Fatal("naming heartbeat_lease_renewal must not change write_fencing's default")
	}
}

// TestNamingHeartbeatLeaseRenewalLeavesWriteFencingAlone is the same claim
// from the other side: naming *write_fencing* must not disturb
// heartbeat_lease_renewal's own default, mirroring
// TestNamingOneSwitchLeavesTheOthersAlone.
func TestNamingHeartbeatLeaseRenewalLeavesWriteFencingAlone(t *testing.T) {
	cfg := defaultConfig("scheduler")
	if err := json.Unmarshal([]byte(`{"scheduler": {"registry": {"write_fencing": false}}}`), &cfg); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if cfg.Scheduler.Registry.WriteFencing {
		t.Fatal("expected write_fencing to have followed its own key to false")
	}
	if cfg.Scheduler.Registry.HeartbeatLeaseRenewal {
		t.Fatal("naming write_fencing must not turn heartbeat_lease_renewal on")
	}
}

// TestHeartbeatLeaseRenewalReadsItsEnvironmentVariable mirrors
// TestTheSchedulerSwitchesReadTheirEnvironmentVariables, in both directions:
// the default is already off, so the meaningful direction is "true" — and the
// control shows an explicit "false" still parses rather than merely doing
// nothing.
func TestHeartbeatLeaseRenewalReadsItsEnvironmentVariable(t *testing.T) {
	t.Run("true", func(t *testing.T) {
		t.Setenv("SCHEDULER_REGISTRY_HEARTBEAT_LEASE_RENEWAL", "true")
		cfg := defaultConfig("scheduler")
		if err := overrideWithEnv(&cfg); err != nil {
			t.Fatalf("override: %v", err)
		}
		if !cfg.Scheduler.Registry.HeartbeatLeaseRenewal {
			t.Fatal("SCHEDULER_REGISTRY_HEARTBEAT_LEASE_RENEWAL=true was ignored")
		}
	})

	t.Run("false", func(t *testing.T) {
		t.Setenv("SCHEDULER_REGISTRY_HEARTBEAT_LEASE_RENEWAL", "false")
		cfg := defaultConfig("scheduler")
		cfg.Scheduler.Registry.HeartbeatLeaseRenewal = true // prove the override actually runs, not just that it stayed at the default
		if err := overrideWithEnv(&cfg); err != nil {
			t.Fatalf("override: %v", err)
		}
		if cfg.Scheduler.Registry.HeartbeatLeaseRenewal {
			t.Fatal("SCHEDULER_REGISTRY_HEARTBEAT_LEASE_RENEWAL=false was ignored")
		}
	})
}

// TestABadHeartbeatLeaseRenewalValueInTheEnvironmentIsRefused mirrors
// TestABadValueInTheEnvironmentIsRefused: an operator's typo during an
// incident must fail loudly, not silently resolve to off.
func TestABadHeartbeatLeaseRenewalValueInTheEnvironmentIsRefused(t *testing.T) {
	t.Setenv("SCHEDULER_REGISTRY_HEARTBEAT_LEASE_RENEWAL", "yes-please")
	cfg := defaultConfig("scheduler")
	if err := overrideWithEnv(&cfg); err == nil {
		t.Fatal("an unparseable boolean was accepted")
	}
}
