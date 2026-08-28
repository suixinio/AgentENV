package config

import (
	"os"
	"path/filepath"
	"strings"
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

// TestRefuseRemovedGatewayEnvVarsRefusesEachRemovedVariable mirrors
// aenv-core's a_manifest_still_setting_a_removed_catalog_switch_is_refused
// (src/cfg.rs): a manifest that still sets any one of RemovedGatewayEnvVars
// must be refused, and the refusal must name the exact variable an operator
// has to act on rather than a generic "invalid config" message — the three
// have different remedies (two have none, one renames), so the message has
// to distinguish them.
func TestRefuseRemovedGatewayEnvVarsRefusesEachRemovedVariable(t *testing.T) {
	for _, name := range RemovedGatewayEnvVars {
		name := name
		t.Run(name, func(t *testing.T) {
			err := refuseRemovedGatewayEnvVarsFrom(func(probed string) (string, bool) {
				if probed == name {
					return "anything", true
				}
				return "", false
			})
			if err == nil {
				t.Fatalf("expected %s to be refused", name)
			}
			if !strings.Contains(err.Error(), name) {
				t.Fatalf("error %q does not name the variable an operator has to act on", err)
			}
		})
	}
}

// TestRefuseRemovedGatewayEnvVarsTreatsEmptyAsSet matches the Rust guard's
// own reasoning: GATEWAY_SCHEDULER_FALLBACK_TIMEOUT= in a manifest is still a
// manifest that has not been migrated, the same as one that sets a real
// duration into it, so an empty value must be refused rather than read as
// absent.
func TestRefuseRemovedGatewayEnvVarsTreatsEmptyAsSet(t *testing.T) {
	err := refuseRemovedGatewayEnvVarsFrom(func(probed string) (string, bool) {
		if probed == "GATEWAY_SCHEDULER_FALLBACK_TIMEOUT" {
			return "", true
		}
		return "", false
	})
	if err == nil {
		t.Fatal("expected an empty-but-set removed variable to be refused")
	}
}

// TestRefuseRemovedGatewayEnvVarsAllowsAnUnsetEnvironment is
// TestRefuseRemovedGatewayEnvVarsRefusesEachRemovedVariable's negative
// control: a migrated manifest, which sets none of RemovedGatewayEnvVars,
// must start.
func TestRefuseRemovedGatewayEnvVarsAllowsAnUnsetEnvironment(t *testing.T) {
	err := refuseRemovedGatewayEnvVarsFrom(func(string) (string, bool) { return "", false })
	if err != nil {
		t.Fatalf("expected a migrated manifest to be accepted, got %v", err)
	}
}

// TestRemovedGatewayEnvVarsMembershipIsExact pins the three names by value
// rather than by count, the same way aenv-core's own
// removed_catalog_env_vars_are_exactly_these pins REMOVED_CATALOG_ENV_VARS: a
// change that only ever checked len(RemovedGatewayEnvVars) == 3 would pass
// this test's weaker sibling even if one entry were replaced by an unrelated
// fourth variable.
func TestRemovedGatewayEnvVarsMembershipIsExact(t *testing.T) {
	want := []string{
		"GATEWAY_QUERY_ONLY_SCHEDULER_ADDR",
		"GATEWAY_SCHEDULER_FALLBACK_DISABLED",
		"GATEWAY_SCHEDULER_FALLBACK_TIMEOUT",
	}
	if len(RemovedGatewayEnvVars) != len(want) {
		t.Fatalf("RemovedGatewayEnvVars = %v, want %v", RemovedGatewayEnvVars, want)
	}
	for i, name := range want {
		if RemovedGatewayEnvVars[i] != name {
			t.Fatalf("RemovedGatewayEnvVars[%d] = %q, want %q", i, RemovedGatewayEnvVars[i], name)
		}
	}
}
