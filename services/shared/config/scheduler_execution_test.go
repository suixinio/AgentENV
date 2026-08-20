package config

import (
	"encoding/json"
	"strings"
	"testing"
)

// The two settings this release adds to the scheduler, and the rules they share
// with the gateway's: named for their scope, defaulted to the end state, and
// refused rather than guessed when they cannot be read.

// TestTheTwoSchedulerSwitchesDefaultToOn.
//
// 🔴 The defaults name the end state, not the cautious first step. Starting a
// release on `observe` is release discipline and belongs in a runbook; putting
// it in the default leaves clusters parked there with nobody aware they were
// never switched over — and the half that stays off in that case is the half
// whose failure costs a workspace.
func TestTheTwoSchedulerSwitchesDefaultToOn(t *testing.T) {
	cfg := defaultConfig("scheduler")

	if !cfg.Scheduler.Registry.WriteFencing {
		t.Fatal("scheduler.registry.write_fencing defaults to off; a superseded incarnation could then overwrite the row of the one that replaced it")
	}
	if got := cfg.Scheduler.Routing.ExecutionArbitration; got != SchedulerExecutionArbitrationEnforce {
		t.Fatalf("scheduler.routing.execution_arbitration defaults to %q, want %q", got, SchedulerExecutionArbitrationEnforce)
	}
}

// TestTheThreeSwitchesAreSeparateSettings.
//
// 🔴 Three switches, three scopes, no merging. "Is fencing off" has no single
// answer, and each of them turns off a different half — the registry's SQL
// predicates, the scheduler's binding arbitration, the gateway's routing
// refusal. Sharing one would let a single panicked flip switch off a half
// nobody meant to, and two of those failures are silent.
func TestTheThreeSwitchesAreSeparateSettings(t *testing.T) {
	raw := `{
        "scheduler": {"registry": {"write_fencing": false}, "routing": {"execution_arbitration": "observe"}},
        "gateway": {"routing": {"execution_fencing": "off"}}
    }`

	cfg := defaultConfig("scheduler")
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if cfg.Scheduler.Registry.WriteFencing {
		t.Fatal("scheduler.registry.write_fencing did not follow its own key")
	}
	if got := cfg.Scheduler.Routing.ExecutionArbitration; got != SchedulerExecutionArbitrationObserve {
		t.Fatalf("scheduler.routing.execution_arbitration: got %q, want observe", got)
	}
	if got := cfg.Gateway.Routing.ExecutionFencing; got != GatewayExecutionFencingOff {
		t.Fatalf("gateway.routing.execution_fencing: got %q, want off", got)
	}
}

// TestNamingOneSwitchLeavesTheOthersAlone: each is defaulted independently, so
// a config that mentions one does not blank the rest.
func TestNamingOneSwitchLeavesTheOthersAlone(t *testing.T) {
	cfg := defaultConfig("scheduler")
	if err := json.Unmarshal([]byte(`{"scheduler": {"routing": {"execution_arbitration": "off"}}}`), &cfg); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if !cfg.Scheduler.Registry.WriteFencing {
		t.Fatal("naming the routing switch turned the registry one off")
	}
	if got := cfg.Gateway.Routing.ExecutionFencing; got != GatewayExecutionFencingEnforce {
		t.Fatalf("naming the scheduler switch changed the gateway's: %q", got)
	}

	// And a routing block with no key inside it leaves the default alone,
	// rather than blanking it into the "unset" that validate() refuses.
	other := defaultConfig("scheduler")
	if err := json.Unmarshal([]byte(`{"scheduler": {"routing": {}}}`), &other); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got := other.Scheduler.Routing.ExecutionArbitration; got != SchedulerExecutionArbitrationEnforce {
		t.Fatalf("an empty routing block changed the mode to %q", got)
	}
}

// TestAnUnrecognisedArbitrationModeStopsTheProcess.
//
// 🔴 Never a fallback. A fallback makes one mistyped letter switch arbitration
// off without saying so, and the resulting behaviour is indistinguishable from
// the value having been meant. The empty string is not a mistyped value — it is
// the absence of a setting — so it resolves to the documented default.
func TestAnUnrecognisedArbitrationModeStopsTheProcess(t *testing.T) {
	for _, value := range []string{"enforced", "on", "true", "observed", "0", " off "} {
		mode, err := ParseSchedulerExecutionArbitration(value)
		if strings.TrimSpace(value) == "off" {
			// Surrounding whitespace is trimmed, so " off " is the setting
			// spelled with a stray space and not a typo.
			if err != nil || mode != SchedulerExecutionArbitrationOff {
				t.Fatalf("%q: got (%q, %v), want off", value, mode, err)
			}
			continue
		}
		if err == nil {
			t.Fatalf("%q was accepted as a mode, and resolved to %q", value, mode)
		}
		if !strings.Contains(err.Error(), "scheduler.routing.execution_arbitration") {
			t.Fatalf("the refusal does not name the setting: %v", err)
		}
	}

	if mode, err := ParseSchedulerExecutionArbitration(""); err != nil || mode != SchedulerExecutionArbitrationEnforce {
		t.Fatalf("the empty value must resolve to the default, got (%q, %v)", mode, err)
	}
}

// TestValidateRefusesAnUnrecognisedArbitrationMode: the refusal reaches
// start-up, not merely the parser.
//
// 🔴 Including on the query-only replica, which is the process serving
// data-plane lookups — a typo there is a typo on the path that matters most,
// and validate() returns early for that mode before most of its checks.
func TestValidateRefusesAnUnrecognisedArbitrationMode(t *testing.T) {
	for _, queryOnly := range []bool{false, true} {
		cfg := defaultConfig("scheduler")
		cfg.Scheduler.RedisAddr = "127.0.0.1:6379"
		cfg.Scheduler.Routing.ExecutionArbitration = SchedulerExecutionArbitration("enforcing")

		err := cfg.validate(queryOnly)
		if err == nil {
			t.Fatalf("query_only=%v: an unrecognised arbitration mode started the process", queryOnly)
		}
		if !strings.Contains(err.Error(), "execution_arbitration") {
			t.Fatalf("query_only=%v: the refusal does not name the setting: %v", queryOnly, err)
		}
	}

	// 🟢 The control: a valid mode passes the same path, so this is not a
	// build that refuses every configuration.
	cfg := defaultConfig("scheduler")
	cfg.Scheduler.RedisAddr = "127.0.0.1:6379"
	cfg.Scheduler.Routing.ExecutionArbitration = SchedulerExecutionArbitrationObserve
	if err := cfg.validate(true); err != nil {
		t.Fatalf("a valid mode was refused: %v", err)
	}
}

// TestTheSchedulerSwitchesReadTheirEnvironmentVariables: both arrive the way
// the runbook flips them.
func TestTheSchedulerSwitchesReadTheirEnvironmentVariables(t *testing.T) {
	t.Setenv("SCHEDULER_REGISTRY_WRITE_FENCING", "false")
	t.Setenv("SCHEDULER_ROUTING_EXECUTION_ARBITRATION", "observe")

	cfg := defaultConfig("scheduler")
	if err := overrideWithEnv(&cfg); err != nil {
		t.Fatalf("override: %v", err)
	}
	if cfg.Scheduler.Registry.WriteFencing {
		t.Fatal("SCHEDULER_REGISTRY_WRITE_FENCING was ignored")
	}
	if got := cfg.Scheduler.Routing.ExecutionArbitration; got != SchedulerExecutionArbitrationObserve {
		t.Fatalf("SCHEDULER_ROUTING_EXECUTION_ARBITRATION: got %q, want observe", got)
	}
}

// TestABadValueInTheEnvironmentIsRefused: same rule on the other input path.
// An operator flipping a switch during an incident gets an error, not a
// silently different behaviour.
func TestABadValueInTheEnvironmentIsRefused(t *testing.T) {
	t.Run("write fencing", func(t *testing.T) {
		t.Setenv("SCHEDULER_REGISTRY_WRITE_FENCING", "yes-please")
		cfg := defaultConfig("scheduler")
		if err := overrideWithEnv(&cfg); err == nil {
			t.Fatal("an unparseable boolean was accepted")
		}
	})

	t.Run("arbitration", func(t *testing.T) {
		t.Setenv("SCHEDULER_ROUTING_EXECUTION_ARBITRATION", "enforced")
		cfg := defaultConfig("scheduler")
		if err := overrideWithEnv(&cfg); err == nil {
			t.Fatal("an unrecognised mode was accepted from the environment")
		}
	})
}
