package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
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
		"SCHEDULER_ROUTING_PROJECTION_AUTHORITATIVE",
		"GATEWAY_REDIS_ADDR",
		"SCHEDULER_MAX_PROJECTION_TTL",
	} {
		t.Setenv(key, "")
	}
}

// TestProjectionSwitchesDefaultOff is the one assertion that keeps a scheduler
// upgrade from changing what a cluster does to its own routing table.
//
// 🔴 The three incarnation switches beside these default to their end state,
// and this is deliberately the other way round. Those shipped in a release
// whose whole purpose was to turn them on. These do not: every node in the
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

	scheduler, err := Load("", "scheduler")
	if err != nil {
		t.Fatalf("load scheduler config: %v", err)
	}
	if scheduler.Scheduler.Routing.ProjectionAuthoritative {
		t.Fatal("scheduler.routing.projection_authoritative defaults on")
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

	schedulerPath := writeProjectionConfig(t, "scheduler.json", `{"scheduler":{"routing":{"projection_authoritative":true}}}`)
	schedulerCfg, err := Load(schedulerPath, "scheduler")
	if err != nil {
		t.Fatalf("load scheduler config: %v", err)
	}
	if !schedulerCfg.Scheduler.Routing.ProjectionAuthoritative {
		t.Fatal("scheduler file value did not land")
	}
	t.Setenv("SCHEDULER_ROUTING_PROJECTION_AUTHORITATIVE", "off")
	schedulerCfg, err = Load(schedulerPath, "scheduler")
	if err != nil {
		t.Fatalf("load scheduler config: %v", err)
	}
	if schedulerCfg.Scheduler.Routing.ProjectionAuthoritative {
		t.Fatal("the environment did not override the scheduler file")
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

	t.Setenv("SCHEDULER_ROUTING_PROJECTION_AUTHORITATIVE", "maybe")
	if _, err := Load("", "scheduler"); err == nil || !strings.Contains(err.Error(), "SCHEDULER_ROUTING_PROJECTION_AUTHORITATIVE") {
		t.Fatalf("a bad scheduler switch was accepted or the error did not name it: %v", err)
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

// TestSchedulerMaxProjectionTTLDefault pins the extra hour.
//
// 🔴 A node's own ceiling defaults to 24 hours and it adds a grace period on
// top so the record outlives the sandbox rather than dying just before it. A
// 24-hour cap here would clamp every single record by exactly that grace —
// cancelling what the grace is for and pinning the "clamped" counter at 100%,
// where it could never signal a real misconfiguration.
func TestSchedulerMaxProjectionTTLDefault(t *testing.T) {
	clearProjectionEnv(t)

	cfg, err := Load("", "scheduler")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Scheduler.MaxProjectionTTL != 25*time.Hour {
		t.Fatalf("default = %s, want 25h", cfg.Scheduler.MaxProjectionTTL)
	}
	if cfg.Scheduler.MaxProjectionTTL <= 24*time.Hour+time.Minute {
		t.Fatal("the ceiling must sit above a node's default ceiling plus its grace, or every record is clamped")
	}
}

func TestSchedulerMaxProjectionTTLReadsBothTheFileAndTheEnvironment(t *testing.T) {
	clearProjectionEnv(t)

	path := writeProjectionConfig(t, "scheduler.json", `{"scheduler":{"max_projection_ttl":"2h"}}`)
	cfg, err := Load(path, "scheduler")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Scheduler.MaxProjectionTTL != 2*time.Hour {
		t.Fatalf("file value = %s, want 2h", cfg.Scheduler.MaxProjectionTTL)
	}

	t.Setenv("SCHEDULER_MAX_PROJECTION_TTL", "90m")
	cfg, err = Load(path, "scheduler")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Scheduler.MaxProjectionTTL != 90*time.Minute {
		t.Fatalf("env value = %s, want 90m", cfg.Scheduler.MaxProjectionTTL)
	}

	t.Setenv("SCHEDULER_MAX_PROJECTION_TTL", "not-a-duration")
	if _, err := Load(path, "scheduler"); err == nil {
		t.Fatal("an unparseable duration was accepted")
	}
}

// A zero ceiling means "unset" and takes the default, rather than becoming "no
// ceiling" — which would be a store with no limit at all on what a writer can
// ask it to keep.
func TestSchedulerMaxProjectionTTLZeroTakesTheDefault(t *testing.T) {
	clearProjectionEnv(t)
	path := writeProjectionConfig(t, "scheduler.json", `{"scheduler":{"max_projection_ttl":"0s"}}`)

	cfg, err := Load(path, "scheduler")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Scheduler.MaxProjectionTTL != 25*time.Hour {
		t.Fatalf("zero produced %s, want the default", cfg.Scheduler.MaxProjectionTTL)
	}

	// A bare number is refused, like every other duration in this block: the
	// unit is not guessable and a silently wrong one would expire records early.
	numeric := writeProjectionConfig(t, "scheduler-numeric.json", `{"scheduler":{"max_projection_ttl":3600}}`)
	if _, err := Load(numeric, "scheduler"); err == nil {
		t.Fatal("a bare number was accepted as a duration")
	}
}
