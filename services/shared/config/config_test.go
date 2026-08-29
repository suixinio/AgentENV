package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// 阶段 3a's REST upstream (`GATEWAY_REST_UPSTREAM_ADDR`) has no code-level
// default — see defaultConfig and GatewayConfig — because there is no sensible
// default for a specific api Service address, so `Config.Validate` refuses to
// load a "gateway" config with it empty. Most of this package's tests load or
// build a "gateway" config to exercise something that has nothing to do with
// that switch, so this sets a placeholder-but-valid value for the whole test
// binary; the handful of tests that are specifically about
// `rest_upstream_addr` override it locally with `t.Setenv`, which restores
// this default once the subtest ends.
//
// It used to seed a second variable, GATEWAY_RESUME_ADDR, under the same
// reasoning. That address is deleted: the wake-up RPC rides
// `gateway.scheduler_addr`'s connection, so there is nothing left to seed.
func TestMain(m *testing.M) {
	os.Setenv("GATEWAY_REST_UPSTREAM_ADDR", "http://agentenv-api.default.svc.cluster.local:8000")
	os.Exit(m.Run())
}

func TestDefaultConfigUsesAutoLogFormat(t *testing.T) {
	cfg := defaultConfig("gateway")
	if cfg.LogFormat != "auto" {
		t.Fatalf("expected default log format auto, got %q", cfg.LogFormat)
	}
}

func TestValidateRejectsUnsupportedLogFormat(t *testing.T) {
	cfg := defaultConfig("gateway")
	cfg.LogFormat = "pretty"
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validate to reject unsupported log_format")
	}
}

func TestValidateAcceptsSupportedLogFormats(t *testing.T) {
	formats := []string{"auto", "console", "json"}
	for _, format := range formats {
		cfg := defaultConfig("gateway")
		cfg.LogFormat = format
		// defaultConfig deliberately gives this no value — see
		// GatewayConfig.RestUpstreamAddr — so a Validate() call made directly
		// against the struct, bypassing Load's env overlay, has to supply it
		// itself.
		cfg.Gateway.RestUpstreamAddr = "http://agentenv-api:8000"
		if err := cfg.Validate(); err != nil {
			t.Fatalf("expected format %q to validate, got error %v", format, err)
		}
	}
}

// 🔴 阶段 3a's REST upstream switch is mandatory now: nodes run `aenv-node`
// and answer 404 on every user-facing REST route, so an empty
// `rest_upstream_addr` is not a rollback, it is an outage discovered only once
// something calls in. This is the direct unit test of that refusal; the
// manifest-level guards in manifest_test.go and
// execution_switches_manifest_test.go check that the deployed cluster never
// actually supplies an empty one.
//
// It used to cover a second address, `resume_addr`, under the same reasoning.
// That one is deleted rather than made optional again — the wake-up RPC rides
// `gateway.scheduler_addr`'s connection now — so there is no second empty to
// refuse.
func TestValidateRefusesAnEmptyGatewayUpstream(t *testing.T) {
	base := func() Config {
		cfg := defaultConfig("gateway")
		cfg.Gateway.RestUpstreamAddr = "http://agentenv-api:8000"
		return cfg
	}

	if err := base().Validate(); err != nil {
		t.Fatalf("a config with the address set was refused: %v", err)
	}

	t.Run("empty rest_upstream_addr", func(t *testing.T) {
		cfg := base()
		cfg.Gateway.RestUpstreamAddr = ""
		if err := cfg.Validate(); err == nil {
			t.Fatal("an empty rest_upstream_addr was accepted")
		}
	})

	t.Run("whitespace rest_upstream_addr", func(t *testing.T) {
		cfg := base()
		cfg.Gateway.RestUpstreamAddr = "   "
		if err := cfg.Validate(); err == nil {
			t.Fatal("a whitespace-only rest_upstream_addr was accepted")
		}
	})
}

func TestLoadParsesGatewayRequestTimeoutDurationString(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "config.json")
	content := `{
		"gateway": {
			"request_timeout": "45s",
			"sandbox_proxy_domains": ["sandbox-proxy.example.invalid", "sandbox-proxy-alt.example.invalid"]
		}
	}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config file failed: %v", err)
	}

	cfg, err := Load(path, "gateway")
	if err != nil {
		t.Fatalf("load config failed: %v", err)
	}
	if cfg.Gateway.RequestTimeout != 45*time.Second {
		t.Fatalf("expected request timeout 45s, got %s", cfg.Gateway.RequestTimeout)
	}
	if got := cfg.Gateway.SandboxProxyDomains; len(got) != 2 || got[0] != "sandbox-proxy.example.invalid" || got[1] != "sandbox-proxy-alt.example.invalid" {
		t.Fatalf("unexpected proxy domains: %#v", got)
	}
}

func TestLoadRejectsNumericGatewayRequestTimeout(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "config.json")
	content := `{
		"gateway": {
			"request_timeout": 30
		}
	}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config file failed: %v", err)
	}

	_, err := Load(path, "gateway")
	if err == nil {
		t.Fatal("expected load to fail for numeric request_timeout")
	}
}

func TestLoadAppliesGatewayRequestTimeoutEnvDuration(t *testing.T) {
	t.Setenv("GATEWAY_REQUEST_TIMEOUT", "1m30s")
	t.Setenv("GATEWAY_SANDBOX_PROXY_DOMAINS", " sandbox-proxy.example.invalid,sandbox-proxy-alt.example.invalid ,,")

	cfg, err := Load("", "gateway")
	if err != nil {
		t.Fatalf("load config failed: %v", err)
	}
	if cfg.Gateway.RequestTimeout != 90*time.Second {
		t.Fatalf("expected request timeout 90s, got %s", cfg.Gateway.RequestTimeout)
	}
	if got := cfg.Gateway.SandboxProxyDomains; len(got) != 2 || got[0] != "sandbox-proxy.example.invalid" || got[1] != "sandbox-proxy-alt.example.invalid" {
		t.Fatalf("unexpected proxy domains from env: %#v", got)
	}
}

func TestLoadRejectsInvalidGatewayRequestTimeoutEnvDuration(t *testing.T) {
	t.Setenv("GATEWAY_REQUEST_TIMEOUT", "1m30")

	_, err := Load("", "gateway")
	if err == nil {
		t.Fatal("expected load to fail for invalid GATEWAY_REQUEST_TIMEOUT")
	}
}

// TestDefaultConfigLeavesTheColdLookupTimeoutAtDefault pins the byte-for-byte
// preservation this setting exists to guarantee: an unconfigured gateway must
// still cap the cold-path LookupNode call at defaultColdLookupTimeout,
// exactly as it did under this field's old name (SchedulerFallbackTimeout).
//
// This replaces TestDefaultConfigLeavesTheSchedulerFallbackOn, dropping the
// half of it that asserted SchedulerFallbackDisabled defaulted to false —
// that field has no replacement; it is deleted outright, not renamed.
func TestDefaultConfigLeavesTheColdLookupTimeoutAtDefault(t *testing.T) {
	cfg := defaultConfig("gateway")
	if cfg.Gateway.ColdLookupTimeout != defaultColdLookupTimeout {
		t.Fatalf("expected cold lookup timeout %s, got %s",
			defaultColdLookupTimeout, cfg.Gateway.ColdLookupTimeout)
	}
}

// TestLoadParsesGatewayColdLookupTimeoutFromFile replaces
// TestLoadParsesGatewaySchedulerFallbackFromFile: same parsing behaviour,
// under the field's current name and JSON key.
func TestLoadParsesGatewayColdLookupTimeoutFromFile(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "config.json")
	content := `{
		"gateway": {
			"cold_lookup_timeout": "5s"
		}
	}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config file failed: %v", err)
	}

	cfg, err := Load(path, "gateway")
	if err != nil {
		t.Fatalf("load config failed: %v", err)
	}
	if cfg.Gateway.ColdLookupTimeout != 5*time.Second {
		t.Fatalf("expected cold lookup timeout 5s, got %s", cfg.Gateway.ColdLookupTimeout)
	}
}

func TestLoadRejectsNumericGatewayColdLookupTimeout(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "config.json")
	content := `{
		"gateway": {
			"cold_lookup_timeout": 5
		}
	}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config file failed: %v", err)
	}

	if _, err := Load(path, "gateway"); err == nil {
		t.Fatal("expected load to fail for numeric cold_lookup_timeout")
	}
}

func TestLoadAppliesGatewayColdLookupTimeoutEnv(t *testing.T) {
	t.Setenv("GATEWAY_COLD_LOOKUP_TIMEOUT", "7s")

	cfg, err := Load("", "gateway")
	if err != nil {
		t.Fatalf("load config failed: %v", err)
	}
	if cfg.Gateway.ColdLookupTimeout != 7*time.Second {
		t.Fatalf("expected cold lookup timeout 7s, got %s", cfg.Gateway.ColdLookupTimeout)
	}
}

func TestLoadRejectsInvalidGatewayColdLookupTimeoutEnv(t *testing.T) {
	t.Setenv("GATEWAY_COLD_LOOKUP_TIMEOUT", "5x")

	if _, err := Load("", "gateway"); err == nil {
		t.Fatal("expected load to fail for invalid GATEWAY_COLD_LOOKUP_TIMEOUT")
	}
}
