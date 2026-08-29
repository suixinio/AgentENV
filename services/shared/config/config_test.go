package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
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
	cfg := defaultConfig()
	if cfg.LogFormat != "auto" {
		t.Fatalf("expected default log format auto, got %q", cfg.LogFormat)
	}
}

func TestValidateRejectsUnsupportedLogFormat(t *testing.T) {
	cfg := defaultConfig()
	cfg.LogFormat = "pretty"
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validate to reject unsupported log_format")
	}
}

func TestValidateAcceptsSupportedLogFormats(t *testing.T) {
	formats := []string{"auto", "console", "json"}
	for _, format := range formats {
		cfg := defaultConfig()
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
		cfg := defaultConfig()
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

	cfg, err := Load(path)
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

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected load to fail for numeric request_timeout")
	}
}

func TestLoadAppliesGatewayRequestTimeoutEnvDuration(t *testing.T) {
	t.Setenv("GATEWAY_REQUEST_TIMEOUT", "1m30s")
	t.Setenv("GATEWAY_SANDBOX_PROXY_DOMAINS", " sandbox-proxy.example.invalid,sandbox-proxy-alt.example.invalid ,,")

	cfg, err := Load("")
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

	_, err := Load("")
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
	cfg := defaultConfig()
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

	cfg, err := Load(path)
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

	if _, err := Load(path); err == nil {
		t.Fatal("expected load to fail for numeric cold_lookup_timeout")
	}
}

func TestLoadAppliesGatewayColdLookupTimeoutEnv(t *testing.T) {
	t.Setenv("GATEWAY_COLD_LOOKUP_TIMEOUT", "7s")

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("load config failed: %v", err)
	}
	if cfg.Gateway.ColdLookupTimeout != 7*time.Second {
		t.Fatalf("expected cold lookup timeout 7s, got %s", cfg.Gateway.ColdLookupTimeout)
	}
}

func TestLoadRejectsInvalidGatewayColdLookupTimeoutEnv(t *testing.T) {
	t.Setenv("GATEWAY_COLD_LOOKUP_TIMEOUT", "5x")

	if _, err := Load(""); err == nil {
		t.Fatal("expected load to fail for invalid GATEWAY_COLD_LOOKUP_TIMEOUT")
	}
}

// 🔴 A deployed ConfigMap may still carry the key the deleted `Config.Service`
// field used to claim.
//
// `Service` was a discriminator with one value: every caller passed "gateway",
// `defaultConfig` used the parameter for nothing else, and the whole gateway
// half of `validate` was indented under `c.Service == "gateway"`. It went with
// `services/scheduler`, the second binary that made it a discriminator at all.
//
// Dropping the field drops the `service` JSON tag with it, and that is the half
// worth pinning: `json.Unmarshal` ignores a key no field claims, so an
// un-migrated gateway.json loads byte-identically to a migrated one. Trusting
// that rather than testing it would be trusting it about a file that is mounted
// into every gateway pod in the cluster at once — the failure, if it were ever
// untrue, is every replica failing `unmarshal config json` on a manifest nobody
// edited.
//
// Both values are covered, not only "gateway": the property is that the key is
// *ignored*, and a loader that had quietly grown a second discriminator would
// pass a test that only ever fed it the one value the old code accepted.
func TestAConfigStillNamingItsServiceLoads(t *testing.T) {
	const gatewayBody = `{
		"log_level": "debug",
		"gateway": {"http_listen_addr": ":8081", "rest_upstream_addr": "http://agentenv-api:8000"}
	}`

	want, err := Load(writeGatewayConfig(t, gatewayBody))
	if err != nil {
		t.Fatalf("the migrated config did not load: %v", err)
	}

	for _, stale := range []string{"gateway", "scheduler"} {
		t.Run(stale, func(t *testing.T) {
			body := `{
				"service": "` + stale + `",
				"log_level": "debug",
				"gateway": {"http_listen_addr": ":8081", "rest_upstream_addr": "http://agentenv-api:8000"}
			}`
			got, err := Load(writeGatewayConfig(t, body))
			if err != nil {
				t.Fatalf("a config still naming %q as its service refused to load: %v", stale, err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("the stale service key changed what loaded:\n got %+v\nwant %+v", got, want)
			}
		})
	}
}

// 🔴 The gateway checks are no longer gated on anything.
//
// They used to run only when `c.Service == "gateway"`, which meant a
// `Config` built any other way — the scheduler's, or a zero value — skipped
// every one of them silently. Nothing selects them now, and this is the
// mutation guard for that: if a discriminator is ever reintroduced and the
// block goes back under it, a `Config` that does not satisfy it stops being
// refused and this notices.
func TestTheGatewayChecksRunForEveryConfig(t *testing.T) {
	cfg := Config{LogLevel: "info", LogFormat: "auto"}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("a config with no gateway settings at all validated; the gateway checks are gated on something again")
	}
	if !strings.Contains(err.Error(), "gateway.") {
		t.Fatalf("the refusal did not come from the gateway checks: %v", err)
	}
}
