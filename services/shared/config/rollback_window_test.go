package config

import (
	"os"
	"path/filepath"
	"testing"
)

// The gateway no longer forwards user-facing REST, so it reads no upstream
// address. The key and the environment variable stay in the deployed manifests
// for one release, because the digest this one rolls back to requires an
// upstream that parses — see
// `TestTheGatewayKeepsTheRestUpstreamKeyForTheRollbackWindow`.
//
// 🔴 That only holds while this build ignores both harmlessly. A loader that
// refused an unknown key, or an environment overlay that failed on an unclaimed
// variable, would turn the manifest that makes rollback possible into a gateway
// that will not start.
func TestALeftoverRestUpstreamDoesNotStopTheGatewayLoading(t *testing.T) {
	path := filepath.Join(t.TempDir(), "gateway.json")
	if err := os.WriteFile(path, []byte(
		`{"gateway":{"scheduler_addr":"agentenv-api:8002","rest_upstream_addr":"http://agentenv-api:8000"}}`,
	), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	t.Setenv("GATEWAY_REST_UPSTREAM_ADDR", "http://agentenv-api:8000")

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("a config carrying the leftover key did not load: %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("a config carrying the leftover key did not validate: %v", err)
	}
	// Resolution: the load above read this file rather than falling back to
	// defaults, so the tolerance it demonstrates is about this file's contents.
	if cfg.Gateway.SchedulerAddr != "agentenv-api:8002" {
		t.Fatalf("the loader did not read the file: scheduler_addr came out %q", cfg.Gateway.SchedulerAddr)
	}

	// The mounted manifest config is the one a deployed gateway actually reads,
	// and it still carries the key.
	mounted, err := Load(filepath.Join(manifestDir, "config", "gateway.json"))
	if err != nil {
		t.Fatalf("the mounted gateway config did not load: %v", err)
	}
	if err := mounted.Validate(); err != nil {
		t.Fatalf("the mounted gateway config did not validate: %v", err)
	}
}
