package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeProjectionConfig(t *testing.T, name string, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

func clearProjectionEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"GATEWAY_ROUTING_PROJECTION_READ",
		"GATEWAY_ROUTING_PROJECTION_AUTHORITATIVE",
		"GATEWAY_REDIS_ADDR",
	} {
		t.Setenv(key, "")
	}
}

// TestProjectionSwitchesDefaultOff is the one assertion that keeps an image
// upgrade from changing what a cluster does to its own routing table.
//
// 🔴 The execution-fencing switch beside this one defaults to its end state,
// and this is deliberately the other way round. That one shipped in a release
// whose whole purpose was to turn it on. This does not: every node in the
// fleet is already emitting the lifecycle events the write side acts on, so a
// cluster that rolled this binary without configuring anything would acquire a
// behaviour it never asked for, at the moment a pod restarted.
func TestProjectionSwitchesDefaultOff(t *testing.T) {
	clearProjectionEnv(t)

	gateway, err := Load("", "gateway")
	if err != nil {
		t.Fatalf("load gateway config: %v", err)
	}
	if gateway.Gateway.Routing.ProjectionRead {
		t.Fatal("gateway.routing.projection_read defaults on")
	}
	if gateway.Gateway.Routing.ProjectionAuthoritative {
		t.Fatal("gateway.routing.projection_authoritative defaults on")
	}
}

func TestProjectionSwitchesReadBothTheFileAndTheEnvironment(t *testing.T) {
	clearProjectionEnv(t)

	path := writeProjectionConfig(t, "gateway.json", `{"gateway":{"redis_addr":"127.0.0.1:6379","routing":{"projection_read":true,"projection_authoritative":true}}}`)
	cfg, err := Load(path, "gateway")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if !cfg.Gateway.Routing.ProjectionRead || !cfg.Gateway.Routing.ProjectionAuthoritative {
		t.Fatalf("file values did not land: %+v", cfg.Gateway.Routing)
	}
	if cfg.Gateway.RedisAddr != "127.0.0.1:6379" {
		t.Fatalf("gateway.redis_addr = %q", cfg.Gateway.RedisAddr)
	}

	// The environment is how an operator flips one deployment without editing
	// a ConfigMap — and it is the form the rollback instructions use, because
	// `kubectl set env` rolls the deployment and is therefore loud.
	t.Setenv("GATEWAY_ROUTING_PROJECTION_READ", "off")
	cfg, err = Load(path, "gateway")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Gateway.Routing.ProjectionRead {
		t.Fatal("the environment did not override the file")
	}
}

// TestProjectionSwitchNamingTheBlockWithoutTheKeyLeavesTheDefault mirrors the
// property the two switches beside these already have: a config file that names
// the routing block but not this key must not blank it.
func TestProjectionSwitchNamingTheBlockWithoutTheKeyLeavesTheDefault(t *testing.T) {
	clearProjectionEnv(t)
	path := writeProjectionConfig(t, "gateway.json", `{"gateway":{"routing":{"execution_fencing":"off"}}}`)

	cfg, err := Load(path, "gateway")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Gateway.Routing.ExecutionFencing != GatewayExecutionFencingOff {
		t.Fatalf("execution_fencing = %q", cfg.Gateway.Routing.ExecutionFencing)
	}
	if cfg.Gateway.Routing.ProjectionRead {
		t.Fatal("projection_read moved because a neighbouring key was named")
	}
}

func TestParseRoutingProjectionSwitch(t *testing.T) {
	on := []string{"on", "ON", " on ", "true", "TRUE", "1"}
	off := []string{"off", "OFF", " off ", "false", "0"}
	bad := []string{"observe", "enforce", "yes", "no", "2", "  "}

	for _, raw := range on {
		got, err := ParseRoutingProjectionSwitch(raw)
		if err != nil || !got {
			t.Fatalf("ParseRoutingProjectionSwitch(%q) = (%v, %v), want on", raw, got, err)
		}
	}
	for _, raw := range off {
		got, err := ParseRoutingProjectionSwitch(raw)
		if err != nil || got {
			t.Fatalf("ParseRoutingProjectionSwitch(%q) = (%v, %v), want off", raw, got, err)
		}
	}
	// 🔴 An unrecognised value stops the process. Guessing would produce a
	// rollout that reports success while the switch it existed for never moved,
	// and "observe" is exactly the kind of value somebody copies across from
	// the switch next door.
	for _, raw := range bad {
		if _, err := ParseRoutingProjectionSwitch(raw); err == nil {
			t.Fatalf("ParseRoutingProjectionSwitch(%q) accepted an unrecognised value", raw)
		}
	}
}

func TestProjectionSwitchEnvironmentRejectsGarbage(t *testing.T) {
	clearProjectionEnv(t)
	for _, key := range []string{
		"GATEWAY_ROUTING_PROJECTION_READ",
		"GATEWAY_ROUTING_PROJECTION_AUTHORITATIVE",
	} {
		t.Setenv(key, "observe")
		if _, err := Load("", "gateway"); err == nil || !strings.Contains(err.Error(), key) {
			t.Fatalf("%s=observe was accepted or the error did not name it: %v", key, err)
		}
		t.Setenv(key, "")
	}
}

// TestProjectionReadRequiresARedisAddress: a read switch with nowhere to read
// from reports as on and does nothing, and the symptom — every request still
// going to the scheduler — is indistinguishable from the switch being off.
func TestProjectionReadRequiresARedisAddress(t *testing.T) {
	clearProjectionEnv(t)
	t.Setenv("GATEWAY_ROUTING_PROJECTION_READ", "on")

	if _, err := Load("", "gateway"); err == nil || !strings.Contains(err.Error(), "gateway.redis_addr") {
		t.Fatalf("expected a refusal naming gateway.redis_addr, got %v", err)
	}

	t.Setenv("GATEWAY_REDIS_ADDR", "127.0.0.1:6379")
	if _, err := Load("", "gateway"); err != nil {
		t.Fatalf("a read switch with an address must load, got %v", err)
	}
}
