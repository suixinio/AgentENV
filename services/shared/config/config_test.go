package config

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDefaultConfigUsesAutoLogFormat(t *testing.T) {
	cfg := defaultConfig("gateway")
	if cfg.LogFormat != "auto" {
		t.Fatalf("expected default log format auto, got %q", cfg.LogFormat)
	}
}

func TestDefaultSchedulerDiscoveryModeIsStatic(t *testing.T) {
	cfg := defaultConfig("scheduler")
	if got := cfg.Scheduler.Discovery.Mode; got != "static" {
		t.Fatalf("expected scheduler discovery mode static, got %q", got)
	}
	if got := cfg.Scheduler.Discovery.Kubernetes.Scheme; got != "http" {
		t.Fatalf("expected kubernetes discovery scheme http, got %q", got)
	}
	if got := cfg.Scheduler.ReportTTL; got != 30*time.Second {
		t.Fatalf("expected scheduler report ttl 30s, got %s", got)
	}
	if got := cfg.Scheduler.BindingTTL; got != 30*time.Second {
		t.Fatalf("expected scheduler binding ttl 30s, got %s", got)
	}
	if got := cfg.Scheduler.MetricsListenAddr; got != ":9101" {
		t.Fatalf("expected scheduler metrics listen addr :9101, got %q", got)
	}
	if got := cfg.Scheduler.ArtifactStoreCapacity; got != defaultSchedulerArtifactStoreCapacity {
		t.Fatalf("expected scheduler artifact store capacity %d, got %d", defaultSchedulerArtifactStoreCapacity, got)
	}
	if got := cfg.Scheduler.ArtifactLookupNodeLimit; got != 0 {
		t.Fatalf("expected scheduler artifact lookup node limit 0, got %d", got)
	}
}

func TestLoadSchedulerAllowsQueryOnlyWithRedisWithoutNodes(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "config.json")
	content := `{
		"scheduler": {
			"redis_addr": "127.0.0.1:6379",
			"nodes": []
		}
	}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config file failed: %v", err)
	}

	cfg, err := LoadScheduler(path, true)
	if err != nil {
		t.Fatalf("load query-only scheduler config failed: %v", err)
	}
	if cfg.Scheduler.RedisAddr != "127.0.0.1:6379" {
		t.Fatalf("unexpected redis addr: %q", cfg.Scheduler.RedisAddr)
	}
}

func TestLoadSchedulerRejectsQueryOnlyWithoutRedis(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "config.json")
	content := `{
		"scheduler": {
			"nodes": []
		}
	}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config file failed: %v", err)
	}

	if _, err := LoadScheduler(path, true); err == nil {
		t.Fatal("expected query-only scheduler without redis_addr to fail")
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
		if err := cfg.Validate(); err != nil {
			t.Fatalf("expected format %q to validate, got error %v", format, err)
		}
	}
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

func TestLoadDefaultsSchedulerDiscoveryToStaticWhenUnset(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "config.json")
	content := `{
		"scheduler": {
			"nodes": [
				{"id": "node-a", "endpoint": "http://node-a:8000"}
			]
		}
	}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config file failed: %v", err)
	}

	cfg, err := Load(path, "scheduler")
	if err != nil {
		t.Fatalf("load config failed: %v", err)
	}
	if got := cfg.Scheduler.Discovery.Mode; got != "static" {
		t.Fatalf("expected discovery mode static, got %q", got)
	}
}

func TestLoadParsesKubernetesSchedulerDiscoveryConfig(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "config.json")
	content := `{
		"scheduler": {
			"discovery": {
				"mode": "kubernetes",
				"kubernetes": {
					"namespace": "agentenv-system",
					"service_name": "agentenv-nodes",
					"port": 8000,
					"ignore_pod_selector": "agentenv.io/discovery=ignore",
					"no_schedule_pod_selector": "agentenv.io/scheduler-state in (draining,no-schedule)"
				}
			}
		}
	}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config file failed: %v", err)
	}

	cfg, err := Load(path, "scheduler")
	if err != nil {
		t.Fatalf("load config failed: %v", err)
	}
	if got := cfg.Scheduler.Discovery.Mode; got != "kubernetes" {
		t.Fatalf("expected discovery mode kubernetes, got %q", got)
	}
	if got := cfg.Scheduler.Discovery.Kubernetes.Scheme; got != "http" {
		t.Fatalf("expected default discovery scheme http, got %q", got)
	}
	if got := cfg.Scheduler.Discovery.Kubernetes.Namespace; got != "agentenv-system" {
		t.Fatalf("expected namespace agentenv-system, got %q", got)
	}
	if got := cfg.Scheduler.Discovery.Kubernetes.IgnorePodSelector; got != "agentenv.io/discovery=ignore" {
		t.Fatalf("expected ignore pod selector, got %q", got)
	}
	if got := cfg.Scheduler.Discovery.Kubernetes.NoSchedulePodSelector; got != "agentenv.io/scheduler-state in (draining,no-schedule)" {
		t.Fatalf("expected no-schedule pod selector, got %q", got)
	}
}

func TestLoadDefaultsSchedulerReportTTLWhenUnset(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "config.json")
	content := `{
		"scheduler": {
			"nodes": [
				{"id": "node-a", "endpoint": "http://node-a:8000"}
			]
		}
	}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config file failed: %v", err)
	}

	cfg, err := Load(path, "scheduler")
	if err != nil {
		t.Fatalf("load config failed: %v", err)
	}
	if got := cfg.Scheduler.ReportTTL; got != 30*time.Second {
		t.Fatalf("expected scheduler report ttl default 30s, got %s", got)
	}
	if got := cfg.Scheduler.BindingTTL; got != 30*time.Second {
		t.Fatalf("expected scheduler binding ttl default 30s, got %s", got)
	}
}

func TestLoadParsesSchedulerReportTTLDurationString(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "config.json")
	content := `{
		"scheduler": {
			"report_ttl": "45s",
			"nodes": [
				{"id": "node-a", "endpoint": "http://node-a:8000"}
			]
		}
	}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config file failed: %v", err)
	}

	cfg, err := Load(path, "scheduler")
	if err != nil {
		t.Fatalf("load config failed: %v", err)
	}
	if got := cfg.Scheduler.ReportTTL; got != 45*time.Second {
		t.Fatalf("expected scheduler report ttl 45s, got %s", got)
	}
}

func TestLoadParsesSchedulerBindingTTLDurationString(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "config.json")
	content := `{
		"scheduler": {
			"binding_ttl": "75s",
			"nodes": [
				{"id": "node-a", "endpoint": "http://node-a:8000"}
			]
		}
	}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config file failed: %v", err)
	}

	cfg, err := Load(path, "scheduler")
	if err != nil {
		t.Fatalf("load config failed: %v", err)
	}
	if got := cfg.Scheduler.BindingTTL; got != 75*time.Second {
		t.Fatalf("expected scheduler binding ttl 75s, got %s", got)
	}
}

func TestLoadParsesSchedulerArtifactStoreCapacity(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "config.json")
	content := `{
		"scheduler": {
			"artifact_store_capacity": 42,
			"nodes": [
				{"id": "node-a", "endpoint": "http://node-a:8000"}
			]
		}
	}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config file failed: %v", err)
	}

	cfg, err := Load(path, "scheduler")
	if err != nil {
		t.Fatalf("load config failed: %v", err)
	}
	if got := cfg.Scheduler.ArtifactStoreCapacity; got != 42 {
		t.Fatalf("expected scheduler artifact store capacity 42, got %d", got)
	}
}

func TestLoadParsesSchedulerArtifactLookupNodeLimit(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "config.json")
	content := `{
		"scheduler": {
			"artifact_lookup_node_limit": 7,
			"nodes": [
				{"id": "node-a", "endpoint": "http://node-a:8000"}
			]
		}
	}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config file failed: %v", err)
	}

	cfg, err := Load(path, "scheduler")
	if err != nil {
		t.Fatalf("load config failed: %v", err)
	}
	if got := cfg.Scheduler.ArtifactLookupNodeLimit; got != 7 {
		t.Fatalf("expected scheduler artifact lookup node limit 7, got %d", got)
	}
}

func TestLoadAllowsNonPositiveSchedulerArtifactLookupNodeLimit(t *testing.T) {
	for _, limit := range []int{0, -1} {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "config.json")
		content := fmt.Sprintf(`{
			"scheduler": {
				"artifact_lookup_node_limit": %d,
				"nodes": [
					{"id": "node-a", "endpoint": "http://node-a:8000"}
				]
			}
		}`, limit)
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatalf("write config file failed: %v", err)
		}

		cfg, err := Load(path, "scheduler")
		if err != nil {
			t.Fatalf("load config failed for limit %d: %v", limit, err)
		}
		if got := cfg.Scheduler.ArtifactLookupNodeLimit; got != limit {
			t.Fatalf("expected scheduler artifact lookup node limit %d, got %d", limit, got)
		}
	}
}

func TestLoadRejectsNonIntegerSchedulerArtifactLookupNodeLimit(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "config.json")
	content := `{
		"scheduler": {
			"artifact_lookup_node_limit": "many",
			"nodes": [
				{"id": "node-a", "endpoint": "http://node-a:8000"}
			]
		}
	}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config file failed: %v", err)
	}

	_, err := Load(path, "scheduler")
	if err == nil {
		t.Fatal("expected load to fail for non-integer scheduler.artifact_lookup_node_limit")
	}
}

func TestLoadRejectsNonPositiveSchedulerArtifactStoreCapacity(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "config.json")
	content := `{
		"scheduler": {
			"artifact_store_capacity": 0,
			"nodes": [
				{"id": "node-a", "endpoint": "http://node-a:8000"}
			]
		}
	}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config file failed: %v", err)
	}

	_, err := Load(path, "scheduler")
	if err == nil {
		t.Fatal("expected load to fail for non-positive scheduler.artifact_store_capacity")
	}
}

func TestLoadRejectsNonIntegerSchedulerArtifactStoreCapacity(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "config.json")
	content := `{
		"scheduler": {
			"artifact_store_capacity": "many",
			"nodes": [
				{"id": "node-a", "endpoint": "http://node-a:8000"}
			]
		}
	}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config file failed: %v", err)
	}

	_, err := Load(path, "scheduler")
	if err == nil {
		t.Fatal("expected load to fail for non-integer scheduler.artifact_store_capacity")
	}
}

func TestLoadRejectsNumericSchedulerReportTTL(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "config.json")
	content := `{
		"scheduler": {
			"report_ttl": 30,
			"nodes": [
				{"id": "node-a", "endpoint": "http://node-a:8000"}
			]
		}
	}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config file failed: %v", err)
	}

	_, err := Load(path, "scheduler")
	if err == nil {
		t.Fatal("expected load to fail for numeric scheduler.report_ttl")
	}
}

func TestLoadRejectsNumericSchedulerBindingTTL(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "config.json")
	content := `{
		"scheduler": {
			"binding_ttl": 30,
			"nodes": [
				{"id": "node-a", "endpoint": "http://node-a:8000"}
			]
		}
	}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config file failed: %v", err)
	}

	_, err := Load(path, "scheduler")
	if err == nil {
		t.Fatal("expected load to fail for numeric scheduler.binding_ttl")
	}
}

func TestLoadAppliesSchedulerBindingTTLEnvDuration(t *testing.T) {
	t.Setenv("SCHEDULER_BINDING_TTL", "45s")

	cfg, err := Load("", "scheduler")
	if err != nil {
		t.Fatalf("load config failed: %v", err)
	}
	if cfg.Scheduler.BindingTTL != 45*time.Second {
		t.Fatalf("expected binding ttl 45s, got %s", cfg.Scheduler.BindingTTL)
	}
}

func TestLoadAppliesSchedulerArtifactStoreCapacityEnv(t *testing.T) {
	t.Setenv("SCHEDULER_ARTIFACT_STORE_CAPACITY", "123")

	cfg, err := Load("", "scheduler")
	if err != nil {
		t.Fatalf("load config failed: %v", err)
	}
	if got := cfg.Scheduler.ArtifactStoreCapacity; got != 123 {
		t.Fatalf("expected artifact store capacity 123, got %d", got)
	}
}

func TestLoadAppliesSchedulerArtifactLookupNodeLimitEnv(t *testing.T) {
	t.Setenv("SCHEDULER_ARTIFACT_LOOKUP_NODE_LIMIT", "9")

	cfg, err := Load("", "scheduler")
	if err != nil {
		t.Fatalf("load config failed: %v", err)
	}
	if got := cfg.Scheduler.ArtifactLookupNodeLimit; got != 9 {
		t.Fatalf("expected artifact lookup node limit 9, got %d", got)
	}
}

func TestLoadAllowsNonPositiveSchedulerArtifactLookupNodeLimitEnv(t *testing.T) {
	t.Setenv("SCHEDULER_ARTIFACT_LOOKUP_NODE_LIMIT", "-1")

	cfg, err := Load("", "scheduler")
	if err != nil {
		t.Fatalf("load config failed: %v", err)
	}
	if got := cfg.Scheduler.ArtifactLookupNodeLimit; got != -1 {
		t.Fatalf("expected artifact lookup node limit -1, got %d", got)
	}
}

func TestLoadRejectsInvalidSchedulerArtifactLookupNodeLimitEnv(t *testing.T) {
	t.Setenv("SCHEDULER_ARTIFACT_LOOKUP_NODE_LIMIT", "many")

	_, err := Load("", "scheduler")
	if err == nil {
		t.Fatal("expected load to fail for invalid SCHEDULER_ARTIFACT_LOOKUP_NODE_LIMIT")
	}
}

func TestLoadRejectsInvalidSchedulerArtifactStoreCapacityEnv(t *testing.T) {
	t.Setenv("SCHEDULER_ARTIFACT_STORE_CAPACITY", "many")

	_, err := Load("", "scheduler")
	if err == nil {
		t.Fatal("expected load to fail for invalid SCHEDULER_ARTIFACT_STORE_CAPACITY")
	}
}

func TestLoadRejectsNonPositiveSchedulerArtifactStoreCapacityEnv(t *testing.T) {
	t.Setenv("SCHEDULER_ARTIFACT_STORE_CAPACITY", "0")

	_, err := Load("", "scheduler")
	if err == nil {
		t.Fatal("expected load to fail for non-positive SCHEDULER_ARTIFACT_STORE_CAPACITY")
	}
}

func TestLoadRejectsInvalidSchedulerBindingTTLEnvDuration(t *testing.T) {
	t.Setenv("SCHEDULER_BINDING_TTL", "45")

	_, err := Load("", "scheduler")
	if err == nil {
		t.Fatal("expected load to fail for invalid SCHEDULER_BINDING_TTL")
	}
}

func TestLoadAppliesSchedulerMetricsListenAddrEnv(t *testing.T) {
	t.Setenv("SCHEDULER_METRICS_LISTEN_ADDR", ":19101")

	cfg, err := Load("", "scheduler")
	if err != nil {
		t.Fatalf("load config failed: %v", err)
	}
	if cfg.Scheduler.MetricsListenAddr != ":19101" {
		t.Fatalf("expected scheduler metrics listen addr :19101, got %q", cfg.Scheduler.MetricsListenAddr)
	}
}

func TestLoadRejectsIncompleteKubernetesSchedulerDiscoveryConfig(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "config.json")
	content := `{
		"scheduler": {
			"discovery": {
				"mode": "kubernetes",
				"kubernetes": {
					"namespace": "agentenv-system",
					"service_name": "",
					"port": 0
				}
			}
		}
	}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config file failed: %v", err)
	}

	if _, err := Load(path, "scheduler"); err == nil {
		t.Fatal("expected load to fail for incomplete kubernetes discovery config")
	}
}

func TestSchedulerWarmupTimeoutDefaultsAndParses(t *testing.T) {
	if got := defaultConfig("scheduler").Scheduler.WarmupTimeout; got != 15*time.Second {
		t.Fatalf("expected a 15s default warmup timeout, got %v", got)
	}

	cfg := defaultConfig("scheduler")
	if err := cfg.Scheduler.UnmarshalJSON([]byte(`{"warmup_timeout":"3s"}`)); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if cfg.Scheduler.WarmupTimeout != 3*time.Second {
		t.Fatalf("expected 3s, got %v", cfg.Scheduler.WarmupTimeout)
	}
}

func TestSchedulerWarmupTimeoutEnvOverride(t *testing.T) {
	t.Setenv("SCHEDULER_WARMUP_TIMEOUT", "7s")
	cfg := defaultConfig("scheduler")
	if err := overrideWithEnv(&cfg); err != nil {
		t.Fatalf("override: %v", err)
	}
	if cfg.Scheduler.WarmupTimeout != 7*time.Second {
		t.Fatalf("expected 7s from the environment, got %v", cfg.Scheduler.WarmupTimeout)
	}
}

func TestDefaultSchedulerRegistryIsOff(t *testing.T) {
	cfg := defaultConfig("scheduler")
	registry := cfg.Scheduler.Registry
	if registry.DSN != "" {
		t.Fatalf("expected registry to default to off, got dsn %q", registry.DSN)
	}
	if registry.ClusterID != "" {
		t.Fatalf("expected empty default cluster id, got %q", registry.ClusterID)
	}
	if registry.MaxConnections != defaultSchedulerRegistryMaxConnections {
		t.Fatalf("expected max connections %d, got %d", defaultSchedulerRegistryMaxConnections, registry.MaxConnections)
	}
	if registry.ReconcileInterval != defaultSchedulerRegistryReconcileInterval {
		t.Fatalf("expected reconcile interval %s, got %s", defaultSchedulerRegistryReconcileInterval, registry.ReconcileInterval)
	}
	if registry.QueryTimeout != defaultSchedulerRegistryQueryTimeout {
		t.Fatalf("expected query timeout %s, got %s", defaultSchedulerRegistryQueryTimeout, registry.QueryTimeout)
	}
	if registry.LeaseWarnWindow != defaultSchedulerRegistryLeaseWarnWindow {
		t.Fatalf("expected lease warn window %s, got %s", defaultSchedulerRegistryLeaseWarnWindow, registry.LeaseWarnWindow)
	}
}

func TestLoadParsesSchedulerRegistryBlock(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "config.json")
	content := `{
		"scheduler": {
			"registry": {
				"dsn": "postgres://reader@127.0.0.1:5432/agentenv",
				"cluster_id": "00000000-0000-0000-0000-000000000000",
				"max_connections": 7,
				"reconcile_interval": "45s",
				"query_timeout": "2s",
				"lease_warn_window": "1m"
			}
		}
	}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config file failed: %v", err)
	}

	cfg, err := LoadScheduler(path, false)
	if err != nil {
		t.Fatalf("load scheduler config failed: %v", err)
	}
	registry := cfg.Scheduler.Registry
	if registry.DSN != "postgres://reader@127.0.0.1:5432/agentenv" {
		t.Fatalf("unexpected dsn %q", registry.DSN)
	}
	if registry.ClusterID != "00000000-0000-0000-0000-000000000000" {
		t.Fatalf("unexpected cluster id %q", registry.ClusterID)
	}
	if registry.MaxConnections != 7 {
		t.Fatalf("unexpected max connections %d", registry.MaxConnections)
	}
	if registry.ReconcileInterval != 45*time.Second {
		t.Fatalf("unexpected reconcile interval %s", registry.ReconcileInterval)
	}
	if registry.QueryTimeout != 2*time.Second {
		t.Fatalf("unexpected query timeout %s", registry.QueryTimeout)
	}
	if registry.LeaseWarnWindow != time.Minute {
		t.Fatalf("unexpected lease warn window %s", registry.LeaseWarnWindow)
	}
}

// A config naming one registry key must not zero the rest of the block; the
// whole point of the pointer/RawMessage decoding is that an absent key keeps
// its default.
func TestLoadKeepsSchedulerRegistryDefaultsForAbsentKeys(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "config.json")
	content := `{
		"scheduler": {
			"registry": {
				"reconcile_interval": "10s"
			}
		}
	}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config file failed: %v", err)
	}

	cfg, err := LoadScheduler(path, false)
	if err != nil {
		t.Fatalf("load scheduler config failed: %v", err)
	}
	registry := cfg.Scheduler.Registry
	if registry.ReconcileInterval != 10*time.Second {
		t.Fatalf("unexpected reconcile interval %s", registry.ReconcileInterval)
	}
	if registry.MaxConnections != defaultSchedulerRegistryMaxConnections {
		t.Fatalf("expected max connections to keep its default, got %d", registry.MaxConnections)
	}
	if registry.QueryTimeout != defaultSchedulerRegistryQueryTimeout {
		t.Fatalf("expected query timeout to keep its default, got %s", registry.QueryTimeout)
	}
	if registry.LeaseWarnWindow != defaultSchedulerRegistryLeaseWarnWindow {
		t.Fatalf("expected lease warn window to keep its default, got %s", registry.LeaseWarnWindow)
	}
}

func TestLoadRejectsNumericSchedulerRegistryReconcileInterval(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "config.json")
	content := `{
		"scheduler": {
			"registry": {
				"reconcile_interval": 45
			}
		}
	}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config file failed: %v", err)
	}

	if _, err := LoadScheduler(path, false); err == nil {
		t.Fatal("expected numeric reconcile_interval to be rejected")
	}
}

func TestLoadAppliesSchedulerRegistryEnvOverrides(t *testing.T) {
	t.Setenv("SCHEDULER_REGISTRY_DSN", "postgres://reader@db:5432/agentenv")
	t.Setenv("SCHEDULER_REGISTRY_CLUSTER_ID", "11111111-2222-3333-4444-555555555555")
	t.Setenv("SCHEDULER_REGISTRY_RECONCILE_INTERVAL", "17s")

	cfg, err := Load("", "scheduler")
	if err != nil {
		t.Fatalf("load config failed: %v", err)
	}
	registry := cfg.Scheduler.Registry
	if registry.DSN != "postgres://reader@db:5432/agentenv" {
		t.Fatalf("unexpected dsn %q", registry.DSN)
	}
	if registry.ClusterID != "11111111-2222-3333-4444-555555555555" {
		t.Fatalf("unexpected cluster id %q", registry.ClusterID)
	}
	if registry.ReconcileInterval != 17*time.Second {
		t.Fatalf("unexpected reconcile interval %s", registry.ReconcileInterval)
	}
}

func TestLoadRejectsInvalidSchedulerRegistryReconcileIntervalEnv(t *testing.T) {
	t.Setenv("SCHEDULER_REGISTRY_RECONCILE_INTERVAL", "soon")

	if _, err := Load("", "scheduler"); err == nil {
		t.Fatal("expected invalid SCHEDULER_REGISTRY_RECONCILE_INTERVAL to fail")
	}
}

// An unset DSN is the default, and every other field is then irrelevant: a
// cluster that never configures a registry must keep starting.
func TestValidateSchedulerRegistryIgnoresEverythingWhenDSNEmpty(t *testing.T) {
	registry := SchedulerRegistryConfig{
		ClusterID:         "not-a-uuid",
		MaxConnections:    -1,
		ReconcileInterval: -time.Second,
		QueryTimeout:      0,
		LeaseWarnWindow:   -time.Minute,
	}
	if err := validateSchedulerRegistry(registry); err != nil {
		t.Fatalf("expected a disabled registry to validate, got %v", err)
	}
}

func TestValidateSchedulerRegistryRejectsBadValuesWhenEnabled(t *testing.T) {
	base := SchedulerRegistryConfig{
		DSN:               "postgres://reader@db:5432/agentenv",
		MaxConnections:    4,
		ReconcileInterval: 30 * time.Second,
		QueryTimeout:      5 * time.Second,
		LeaseWarnWindow:   30 * time.Second,
	}
	if err := validateSchedulerRegistry(base); err != nil {
		t.Fatalf("expected a well formed registry config to validate, got %v", err)
	}

	cases := map[string]func(*SchedulerRegistryConfig){
		"max_connections":    func(c *SchedulerRegistryConfig) { c.MaxConnections = 0 },
		"reconcile_interval": func(c *SchedulerRegistryConfig) { c.ReconcileInterval = 0 },
		"query_timeout":      func(c *SchedulerRegistryConfig) { c.QueryTimeout = 0 },
		"lease_warn_window":  func(c *SchedulerRegistryConfig) { c.LeaseWarnWindow = 0 },
		"cluster_id":         func(c *SchedulerRegistryConfig) { c.ClusterID = "dev-cluster" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			cfg := base
			mutate(&cfg)
			if err := validateSchedulerRegistry(cfg); err == nil {
				t.Fatalf("expected %s to be rejected", name)
			}
		})
	}
}

// A query-only replica gets the same reader, so it has to reject the same bad
// config rather than skipping the check with the rest of the query-only branch.
func TestLoadSchedulerValidatesRegistryInQueryOnlyMode(t *testing.T) {
	t.Setenv("SCHEDULER_REGISTRY_DSN", "postgres://reader@db:5432/agentenv")
	t.Setenv("SCHEDULER_REGISTRY_CLUSTER_ID", "dev-cluster")
	t.Setenv("SCHEDULER_REDIS_ADDR", "127.0.0.1:6379")

	if _, err := LoadScheduler("", true); err == nil {
		t.Fatal("expected query-only scheduler to reject a non-uuid registry cluster id")
	}
}

func TestLooksLikeUUID(t *testing.T) {
	valid := []string{
		"00000000-0000-0000-0000-000000000000",
		"11111111-2222-3333-4444-555555555555",
		"AABBCCDD-EEFF-0011-2233-445566778899",
	}
	for _, value := range valid {
		if !looksLikeUUID(value) {
			t.Fatalf("expected %q to look like a uuid", value)
		}
	}

	invalid := []string{
		"",
		"dev-cluster",
		"00000000-0000-0000-0000-00000000000",
		"00000000-0000-0000-0000-0000000000000",
		"00000000_0000_0000_0000_000000000000",
		"0000000g-0000-0000-0000-000000000000",
		"00000000-0000-0000-000000000000-0000",
	}
	for _, value := range invalid {
		if looksLikeUUID(value) {
			t.Fatalf("expected %q not to look like a uuid", value)
		}
	}
}
