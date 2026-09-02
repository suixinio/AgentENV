package config

import (
	"os"
	"path/filepath"
	"testing"
)

// The gateway's rollback window is closed: it forwards no REST, calls no
// sandbox lookup, writes no projection and caps no response, so the keys that
// configured those are gone from GatewayConfig. docs/src/configuration/env-vars.md
// promises that a config or environment still carrying one loads and is
// ignored, and this is the pin for that promise: json.Unmarshal drops a key no
// field claims, and overrideWithEnv reads only the names it knows.
func TestRemovedGatewayKeysAreIgnored(t *testing.T) {
	path := filepath.Join(t.TempDir(), "gateway.json")
	if err := os.WriteFile(path, []byte(`{"gateway":{
		"scheduler_addr":"agentenv-api:8002",
		"rest_upstream_addr":"http://agentenv-api:8000",
		"cold_lookup_timeout":"3s",
		"forward_response_size":4194304,
		"routing":{"projection_authoritative":true}
	}}`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	t.Setenv("GATEWAY_REST_UPSTREAM_ADDR", "http://agentenv-api:8000")
	t.Setenv("GATEWAY_COLD_LOOKUP_TIMEOUT", "3s")
	t.Setenv("GATEWAY_ROUTING_PROJECTION_AUTHORITATIVE", "on")

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("a config carrying removed keys did not load: %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("a config carrying removed keys did not validate: %v", err)
	}
	// The load above read this file rather than falling back to defaults, so
	// the tolerance it demonstrates is about this file's contents.
	if cfg.Gateway.SchedulerAddr != "agentenv-api:8002" {
		t.Fatalf("the loader did not read the file: scheduler_addr came out %q", cfg.Gateway.SchedulerAddr)
	}
}
