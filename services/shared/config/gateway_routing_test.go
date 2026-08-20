package config

import (
	"os"
	"path/filepath"
	"testing"
)

func writeGatewayConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "gateway.json")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

// The default points at the end state. Starting a rollout on observe is release
// discipline and belongs in the runbook; a cautious default leaves clusters
// parked on it with nobody aware they were never flipped.
func TestGatewayExecutionFencingDefaultsToEnforce(t *testing.T) {
	t.Setenv("GATEWAY_ROUTING_EXECUTION_FENCING", "")

	cfg, err := Load("", "gateway")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Gateway.Routing.ExecutionFencing != GatewayExecutionFencingEnforce {
		t.Fatalf("default is %q, want %q", cfg.Gateway.Routing.ExecutionFencing, GatewayExecutionFencingEnforce)
	}
}

// Both paths have to work: the config file is a ConfigMap and the environment is
// how an operator flips one gateway without editing it.
func TestGatewayExecutionFencingReadsBothTheFileAndTheEnvironment(t *testing.T) {
	path := writeGatewayConfig(t, `{"gateway":{"routing":{"execution_fencing":"observe"}}}`)

	t.Setenv("GATEWAY_ROUTING_EXECUTION_FENCING", "")
	cfg, err := Load(path, "gateway")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Gateway.Routing.ExecutionFencing != GatewayExecutionFencingObserve {
		t.Fatalf("file value came out as %q, want observe", cfg.Gateway.Routing.ExecutionFencing)
	}

	t.Setenv("GATEWAY_ROUTING_EXECUTION_FENCING", "off")
	cfg, err = Load(path, "gateway")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Gateway.Routing.ExecutionFencing != GatewayExecutionFencingOff {
		t.Fatalf("the environment did not override the file: got %q, want off", cfg.Gateway.Routing.ExecutionFencing)
	}
}

// A config file that names the block without naming the key must not blank the
// default — the same rule every other optional key in this file follows.
func TestGatewayRoutingBlockWithoutTheKeyKeepsTheDefault(t *testing.T) {
	t.Setenv("GATEWAY_ROUTING_EXECUTION_FENCING", "")

	cfg, err := Load(writeGatewayConfig(t, `{"gateway":{"routing":{}}}`), "gateway")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Gateway.Routing.ExecutionFencing != GatewayExecutionFencingEnforce {
		t.Fatalf("an empty routing block left %q, want the default", cfg.Gateway.Routing.ExecutionFencing)
	}
}

// 🔴 An unrecognised value stops the process, from either source.
//
// A fallback to a default would make one mistyped letter switch fencing off with
// nothing said about it, and the outcome would be indistinguishable from the
// value having been meant. The two subtests are not redundant: the file and the
// environment reach the field through different code, and only one of them was
// ever going to be exercised by accident.
func TestAnUnrecognisedGatewayExecutionFencingRefusesToLoad(t *testing.T) {
	t.Run("from the config file", func(t *testing.T) {
		t.Setenv("GATEWAY_ROUTING_EXECUTION_FENCING", "")
		if _, err := Load(writeGatewayConfig(t, `{"gateway":{"routing":{"execution_fencing":"enfroce"}}}`), "gateway"); err == nil {
			t.Fatal("a mistyped mode in the config file was accepted")
		}
	})

	t.Run("from the environment", func(t *testing.T) {
		t.Setenv("GATEWAY_ROUTING_EXECUTION_FENCING", "enfroce")
		if _, err := Load("", "gateway"); err == nil {
			t.Fatal("a mistyped mode in the environment was accepted")
		}
	})

	// The control: the three real values load, so the refusal above is about the
	// value and not about the plumbing.
	for _, mode := range []string{"off", "observe", "enforce"} {
		t.Setenv("GATEWAY_ROUTING_EXECUTION_FENCING", mode)
		cfg, err := Load("", "gateway")
		if err != nil {
			t.Fatalf("mode %q was rejected: %v", mode, err)
		}
		if string(cfg.Gateway.Routing.ExecutionFencing) != mode {
			t.Fatalf("mode %q loaded as %q", mode, cfg.Gateway.Routing.ExecutionFencing)
		}
	}
}

// 🔴 The control-plane token is a credential, so it arrives the way the registry
// DSN does. A config file that names it must not be able to set it: the file is
// a ConfigMap, and a secret that can be written there will eventually be.
func TestTheControlPlaneTokenComesOnlyFromTheEnvironment(t *testing.T) {
	t.Setenv("GATEWAY_CONTROL_PLANE_TOKEN", "")

	path := writeGatewayConfig(t, `{"gateway":{"control_plane_token":"a-token-in-a-configmap"}}`)
	cfg, err := Load(path, "gateway")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Gateway.ControlPlaneToken != "" {
		t.Fatalf("the config file set the token to %q; it is a secret and must not be readable from there", cfg.Gateway.ControlPlaneToken)
	}

	t.Setenv("GATEWAY_CONTROL_PLANE_TOKEN", "a-token-from-a-secret")
	cfg, err = Load(path, "gateway")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Gateway.ControlPlaneToken != "a-token-from-a-secret" {
		t.Fatalf("the environment token came out as %q", cfg.Gateway.ControlPlaneToken)
	}
}

func TestParseGatewayExecutionFencing(t *testing.T) {
	for raw, want := range map[string]GatewayExecutionFencing{
		"":         GatewayExecutionFencingEnforce,
		"  ":       GatewayExecutionFencingEnforce,
		"off":      GatewayExecutionFencingOff,
		"OFF":      GatewayExecutionFencingOff,
		" observe": GatewayExecutionFencingObserve,
		"enforce":  GatewayExecutionFencingEnforce,
	} {
		got, err := ParseGatewayExecutionFencing(raw)
		if err != nil {
			t.Fatalf("ParseGatewayExecutionFencing(%q) failed: %v", raw, err)
		}
		if got != want {
			t.Fatalf("ParseGatewayExecutionFencing(%q) = %q, want %q", raw, got, want)
		}
	}

	for _, raw := range []string{"enfroce", "on", "true", "observe-only"} {
		if _, err := ParseGatewayExecutionFencing(raw); err == nil {
			t.Fatalf("ParseGatewayExecutionFencing(%q) was accepted", raw)
		}
	}
}
