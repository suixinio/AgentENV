package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// 阶段 3a's two addresses (`GATEWAY_REST_UPSTREAM_ADDR`, `GATEWAY_RESUME_ADDR`)
// have no code-level default — see defaultConfig and GatewayConfig — because
// there is no sensible default for a specific api Service address, so
// `Config.Validate` refuses to load a "gateway" config with either one empty.
// Most of this package's tests load or build a "gateway" config to exercise
// something that has nothing to do with that switch, so this sets both to
// placeholder-but-valid values for the whole test binary; the handful of
// tests that are specifically about `rest_upstream_addr`/`resume_addr`
// override them locally with `t.Setenv`, which restores this default once the
// subtest ends.
func TestMain(m *testing.M) {
	os.Setenv("GATEWAY_REST_UPSTREAM_ADDR", "http://agentenv-api.default.svc.cluster.local:8000")
	os.Setenv("GATEWAY_RESUME_ADDR", "agentenv-api.default.svc.cluster.local:8002")
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
		// defaultConfig deliberately gives these two no value — see
		// GatewayConfig.RestUpstreamAddr/ResumeAddr — so a Validate() call
		// made directly against the struct, bypassing Load's env overlay, has
		// to supply both itself.
		cfg.Gateway.RestUpstreamAddr = "http://agentenv-api:8000"
		cfg.Gateway.ResumeAddr = "agentenv-api:8002"
		if err := cfg.Validate(); err != nil {
			t.Fatalf("expected format %q to validate, got error %v", format, err)
		}
	}
}

// 🔴 Both halves of 阶段 3a's REST upstream switch are mandatory now: nodes run
// `aenv-node` and answer 404 on every user-facing REST route and have no
// wake-up surface of their own, so an empty `rest_upstream_addr` or
// `resume_addr` is not a rollback, it is an outage discovered only once
// something calls in. This is the direct unit test of that refusal; the
// manifest-level guards in manifest_test.go and
// execution_switches_manifest_test.go check that the deployed cluster never
// actually supplies an empty one.
func TestValidateRefusesAnEmptyGatewayUpstreamOrResumeAddr(t *testing.T) {
	base := func() Config {
		cfg := defaultConfig("gateway")
		cfg.Gateway.RestUpstreamAddr = "http://agentenv-api:8000"
		cfg.Gateway.ResumeAddr = "agentenv-api:8002"
		return cfg
	}

	if err := base().Validate(); err != nil {
		t.Fatalf("a config with both addresses set was refused: %v", err)
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

	t.Run("empty resume_addr", func(t *testing.T) {
		cfg := base()
		cfg.Gateway.ResumeAddr = ""
		if err := cfg.Validate(); err == nil {
			t.Fatal("an empty resume_addr was accepted")
		}
	})

	t.Run("whitespace resume_addr", func(t *testing.T) {
		cfg := base()
		cfg.Gateway.ResumeAddr = "   "
		if err := cfg.Validate(); err == nil {
			t.Fatal("a whitespace-only resume_addr was accepted")
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

// TestDefaultConfigLeavesTheSchedulerFallbackOn pins the byte-for-byte
// preservation this setting exists to guarantee: an unconfigured gateway must
// still ask the scheduler on the LookupNode fallback path, exactly as it did
// before SchedulerFallbackDisabled existed.
func TestDefaultConfigLeavesTheSchedulerFallbackOn(t *testing.T) {
	cfg := defaultConfig("gateway")
	if cfg.Gateway.SchedulerFallbackDisabled {
		t.Fatal("expected the scheduler fallback to default to enabled")
	}
	if cfg.Gateway.SchedulerFallbackTimeout != defaultGatewaySchedulerFallbackTimeout {
		t.Fatalf("expected scheduler fallback timeout %s, got %s",
			defaultGatewaySchedulerFallbackTimeout, cfg.Gateway.SchedulerFallbackTimeout)
	}
}

func TestLoadParsesGatewaySchedulerFallbackFromFile(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "config.json")
	content := `{
		"gateway": {
			"scheduler_fallback_disabled": true,
			"scheduler_fallback_timeout": "5s"
		}
	}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config file failed: %v", err)
	}

	cfg, err := Load(path, "gateway")
	if err != nil {
		t.Fatalf("load config failed: %v", err)
	}
	if !cfg.Gateway.SchedulerFallbackDisabled {
		t.Fatal("expected scheduler_fallback_disabled to be true")
	}
	if cfg.Gateway.SchedulerFallbackTimeout != 5*time.Second {
		t.Fatalf("expected scheduler fallback timeout 5s, got %s", cfg.Gateway.SchedulerFallbackTimeout)
	}
}

func TestLoadRejectsNumericGatewaySchedulerFallbackTimeout(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "config.json")
	content := `{
		"gateway": {
			"scheduler_fallback_timeout": 5
		}
	}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config file failed: %v", err)
	}

	if _, err := Load(path, "gateway"); err == nil {
		t.Fatal("expected load to fail for numeric scheduler_fallback_timeout")
	}
}

func TestLoadAppliesGatewaySchedulerFallbackEnv(t *testing.T) {
	t.Setenv("GATEWAY_SCHEDULER_FALLBACK_DISABLED", "true")
	t.Setenv("GATEWAY_SCHEDULER_FALLBACK_TIMEOUT", "7s")

	cfg, err := Load("", "gateway")
	if err != nil {
		t.Fatalf("load config failed: %v", err)
	}
	if !cfg.Gateway.SchedulerFallbackDisabled {
		t.Fatal("expected GATEWAY_SCHEDULER_FALLBACK_DISABLED=true to disable the fallback")
	}
	if cfg.Gateway.SchedulerFallbackTimeout != 7*time.Second {
		t.Fatalf("expected scheduler fallback timeout 7s, got %s", cfg.Gateway.SchedulerFallbackTimeout)
	}
}

func TestLoadRejectsInvalidGatewaySchedulerFallbackDisabledEnv(t *testing.T) {
	t.Setenv("GATEWAY_SCHEDULER_FALLBACK_DISABLED", "not-a-bool")

	if _, err := Load("", "gateway"); err == nil {
		t.Fatal("expected load to fail for invalid GATEWAY_SCHEDULER_FALLBACK_DISABLED")
	}
}

func TestLoadRejectsInvalidGatewaySchedulerFallbackTimeoutEnv(t *testing.T) {
	t.Setenv("GATEWAY_SCHEDULER_FALLBACK_TIMEOUT", "5x")

	if _, err := Load("", "gateway"); err == nil {
		t.Fatal("expected load to fail for invalid GATEWAY_SCHEDULER_FALLBACK_TIMEOUT")
	}
}
