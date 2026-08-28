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
	path := writeGatewayConfig(t, `{"gateway":{"routing":{"execution_fencing":"off"}}}`)

	t.Setenv("GATEWAY_ROUTING_EXECUTION_FENCING", "")
	cfg, err := Load(path, "gateway")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Gateway.Routing.ExecutionFencing != GatewayExecutionFencingOff {
		t.Fatalf("file value came out as %q, want off", cfg.Gateway.Routing.ExecutionFencing)
	}

	t.Setenv("GATEWAY_ROUTING_EXECUTION_FENCING", "enforce")
	cfg, err = Load(path, "gateway")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Gateway.Routing.ExecutionFencing != GatewayExecutionFencingEnforce {
		t.Fatalf("the environment did not override the file: got %q, want enforce", cfg.Gateway.Routing.ExecutionFencing)
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

	// 🔴 "observe" gets its own subtest rather than joining the mistyped-mode
	// cases above: it is not a typo, it is the retired third mode, and a
	// manifest that still names it is exactly the case this whole file exists
	// to catch. Both paths, the same as the two subtests above.
	t.Run("observe in the config file", func(t *testing.T) {
		t.Setenv("GATEWAY_ROUTING_EXECUTION_FENCING", "")
		if _, err := Load(writeGatewayConfig(t, `{"gateway":{"routing":{"execution_fencing":"observe"}}}`), "gateway"); err == nil {
			t.Fatal("the retired \"observe\" mode in the config file was accepted")
		}
	})

	t.Run("observe in the environment", func(t *testing.T) {
		t.Setenv("GATEWAY_ROUTING_EXECUTION_FENCING", "observe")
		if _, err := Load("", "gateway"); err == nil {
			t.Fatal("the retired \"observe\" mode in the environment was accepted")
		}
	})

	// The control: the two real values load, so the refusal above is about the
	// value and not about the plumbing.
	for _, mode := range []string{"off", "enforce"} {
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
		"":        GatewayExecutionFencingEnforce,
		"  ":      GatewayExecutionFencingEnforce,
		"off":     GatewayExecutionFencingOff,
		"OFF":     GatewayExecutionFencingOff,
		"enforce": GatewayExecutionFencingEnforce,
	} {
		got, err := ParseGatewayExecutionFencing(raw)
		if err != nil {
			t.Fatalf("ParseGatewayExecutionFencing(%q) failed: %v", raw, err)
		}
		if got != want {
			t.Fatalf("ParseGatewayExecutionFencing(%q) = %q, want %q", raw, got, want)
		}
	}

	// 🔴 "observe" and " observe" (with the whitespace this parser trims) are
	// in the refusal list on purpose: the mode used to be recognised here,
	// with exactly this leading-space spelling accepted in the table above,
	// and is not any more. A fallback to enforce or off would be silent about
	// exactly the value an old runbook or a stale ConfigMap is most likely to
	// still name.
	for _, raw := range []string{"enfroce", "on", "true", "observe-only", "observe", " observe"} {
		if _, err := ParseGatewayExecutionFencing(raw); err == nil {
			t.Fatalf("ParseGatewayExecutionFencing(%q) was accepted", raw)
		}
	}
}
