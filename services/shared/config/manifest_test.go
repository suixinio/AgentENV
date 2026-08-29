package config

import (
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	k8syaml "k8s.io/apimachinery/pkg/util/yaml"
)

// manifestDir is the base kustomize layer, reached from this package.
const manifestDir = "../../../deploy/k8s/base"

// 🔴 Both control-plane services serve Prometheus on a listener of their own,
// separate from the port that carries traffic. A listener nothing declares is a
// listener nothing scrapes: the gateway's :9102 existed for as long as the
// gateway did, and every series on it — including the sandbox-location counter
// added for the central registry work — was invisible to Prometheus because the
// Deployment declared only :8080 and the Service exposed only :8080.
//
// The failure is silent in both directions. Nothing crashes, the pod is Ready,
// and `kubectl port-forward` reaches the port perfectly well, so the gap only
// shows up as metrics that are never there when somebody goes looking during an
// incident. This ties each manifest to the port its own mounted config resolves
// to, so the two cannot drift apart again in either direction.
func TestMetricsListenersAreDeclaredAndExposed(t *testing.T) {
	// Neutralise any ambient override, so what is compared is what the pod's
	// ConfigMap resolves to and not what this shell happens to export. An empty
	// value is ignored by overrideWithEnv, which is what makes this a clear
	// rather than a set.
	t.Setenv("GATEWAY_METRICS_LISTEN_ADDR", "")

	for _, tc := range []struct {
		service    string
		configFile string
		deployment string
		svc        string
		metricsOf  func(Config) string
	}{
		{
			service:    "gateway",
			configFile: "config/gateway.json",
			deployment: "gateway-deployment.yaml",
			svc:        "gateway-service.yaml",
			metricsOf:  func(c Config) string { return c.Gateway.MetricsListenAddr },
		},
		// 🔴 阶段四/Stage E: the "scheduler" case that used to run here
		// (config/scheduler.json against scheduler-deployment.yaml /
		// scheduler-service.yaml) was deleted along with those files —
		// services/scheduler has been decommissioned. See
		// services/README.md for the current architecture.
	} {
		t.Run(tc.service, func(t *testing.T) {
			cfg, err := Load(filepath.Join(manifestDir, tc.configFile), tc.service)
			if err != nil {
				t.Fatalf("loading the mounted %s config failed: %v", tc.service, err)
			}
			wantPort := listenPort(t, tc.metricsOf(cfg))

			var deployment appsv1.Deployment
			decodeManifest(t, filepath.Join(manifestDir, tc.deployment), &deployment)
			containers := deployment.Spec.Template.Spec.Containers
			if len(containers) != 1 {
				t.Fatalf("expected one container, got %d", len(containers))
			}
			portName := ""
			for _, port := range containers[0].Ports {
				if port.ContainerPort == wantPort {
					portName = port.Name
					break
				}
			}
			if portName == "" {
				t.Fatalf("%s serves metrics on port %d but the Deployment declares %v",
					tc.service, wantPort, containers[0].Ports)
			}

			var service corev1.Service
			decodeManifest(t, filepath.Join(manifestDir, tc.svc), &service)
			for _, port := range service.Spec.Ports {
				// Matched on the target rather than on the published port, so
				// renumbering the Service port stays legal and pointing it at
				// nothing does not.
				if port.TargetPort.StrVal == portName || int32(port.TargetPort.IntValue()) == wantPort {
					return
				}
			}
			t.Fatalf("%s declares its metrics port as %q but the Service exposes %v",
				tc.service, portName, service.Spec.Ports)
		})
	}
}

// listenPort pulls the port out of a listen address. The addresses in play are
// all of the ":9102" shape; anything else is a config this test cannot judge
// and should say so rather than guess.
func listenPort(t *testing.T, addr string) int32 {
	t.Helper()

	_, rawPort, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("cannot read a port out of listen address %q: %v", addr, err)
	}
	port, err := strconv.Atoi(rawPort)
	if err != nil {
		t.Fatalf("listen address %q does not carry a numeric port: %v", addr, err)
	}
	return int32(port)
}

func decodeManifest(t *testing.T, path string, into any) {
	t.Helper()

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s failed: %v", path, err)
	}
	if err := k8syaml.Unmarshal(raw, into); err != nil {
		t.Fatalf("decoding %s failed: %v", path, err)
	}
}

// generatedLiteral reads a literal out of the base layer's configMapGenerator.
// Read from the generator rather than from a checked-in ConfigMap because the
// generator is what `kustomize build` actually emits.
func generatedLiteral(t *testing.T, configMap, key string) string {
	t.Helper()

	var kustomization struct {
		ConfigMapGenerator []struct {
			Name     string   `json:"name"`
			Literals []string `json:"literals"`
		} `json:"configMapGenerator"`
	}
	decodeManifest(t, filepath.Join(manifestDir, "kustomization.yaml"), &kustomization)

	for _, generator := range kustomization.ConfigMapGenerator {
		if generator.Name != configMap {
			continue
		}
		for _, literal := range generator.Literals {
			name, value, found := strings.Cut(literal, "=")
			if found && name == key {
				return value
			}
		}
		t.Fatalf("the generator for %s carries no %s: %v", configMap, key, generator.Literals)
	}
	t.Fatalf("nothing in the base layer generates the %s ConfigMap the workloads read their cluster id from", configMap)
	return ""
}

// nodeIdentityClusterID reads `[node_identity].cluster_id` out of the runtime
// config file. Scanned rather than parsed: this package has no TOML dependency,
// and the one line it is after is a plain assignment.
func nodeIdentityClusterID(t *testing.T) string {
	t.Helper()

	raw, err := os.ReadFile(filepath.Join(manifestDir, "..", "..", "..", "config", "default.toml"))
	if err != nil {
		t.Fatalf("reading the runtime config failed: %v", err)
	}

	inSection := false
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "[") {
			inSection = line == "[node_identity]"
			continue
		}
		if !inSection {
			continue
		}
		name, value, found := strings.Cut(line, "=")
		if found && strings.TrimSpace(name) == "cluster_id" {
			return strings.Trim(strings.TrimSpace(value), `"`)
		}
	}
	t.Fatal("config/default.toml has no [node_identity].cluster_id; the node's fallback cannot be compared against the ConfigMap")
	return ""
}

// 🔴 阶段 3a's REST switch is present in the mounted ConfigMap and is
// mandatory, not off.
//
// Two halves, and a reason for each: the key is declared so a lost ConfigMap
// is a value away from restoring, but the file ships it empty, because
// emptying it is not a rollback — `aenv-node` answers 404 on every user-facing REST route
// under any configuration, so `Config.Validate` refuses to load a gateway
// config with `rest_upstream_addr` empty from any source. Shipping the switch
// and shipping the traffic move used to be two different days; now there is
// only the one supported position.
func TestGatewayRestUpstreamIsDeclaredAndRequired(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(manifestDir, "config", "gateway.json"))
	if err != nil {
		t.Fatalf("reading the mounted gateway config failed: %v", err)
	}
	var mounted struct {
		Gateway map[string]json.RawMessage `json:"gateway"`
	}
	if err := json.Unmarshal(raw, &mounted); err != nil {
		t.Fatalf("the mounted gateway config is not valid JSON: %v", err)
	}
	if _, declared := mounted.Gateway["rest_upstream_addr"]; !declared {
		t.Fatalf("the mounted gateway config does not name rest_upstream_addr, so losing " +
			"api-upstream-config's ConfigMap means editing the manifest under incident pressure " +
			"rather than restoring a value")
	}

	t.Setenv("GATEWAY_REST_UPSTREAM_ADDR", "")
	if _, err := Load(filepath.Join(manifestDir, "config", "gateway.json"), "gateway"); err == nil {
		t.Fatal("the mounted gateway config loaded with no rest_upstream_addr from either the " +
			"file or the environment; it must refuse, the same way a deployed gateway would if " +
			"api-upstream-config were ever lost")
	}

	// Resolution: the same loader carries a real address through, from the
	// file and from the environment, so the refusal above is about the
	// mounted file's own empty default and not about the key being unreadable.
	path := filepath.Join(t.TempDir(), "gateway.json")
	if err := os.WriteFile(path, []byte(`{"gateway":{"rest_upstream_addr":"http://agentenv-api:8000"}}`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	cfg, err := Load(path, "gateway")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Gateway.RestUpstreamAddr != "http://agentenv-api:8000" {
		t.Fatalf("rest_upstream_addr in a config file came out as %q", cfg.Gateway.RestUpstreamAddr)
	}

	t.Setenv("GATEWAY_REST_UPSTREAM_ADDR", "http://agentenv-api-canary:8000")
	cfg, err = Load(filepath.Join(manifestDir, "config", "gateway.json"), "gateway")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Gateway.RestUpstreamAddr != "http://agentenv-api-canary:8000" {
		t.Fatalf("the environment did not override the manifest: got %q", cfg.Gateway.RestUpstreamAddr)
	}
}

// 🔴 Why the mounted file has to keep shipping the key empty, and not only
// the ConfigMap the Deployment reads through the environment.
//
// An environment variable set to the empty string is *ignored* by the loader —
// that is deliberate, and it is what makes `kubectl set env FOO=` a clear
// rather than a set. The consequence is one-directional and easy to miss: an
// operator who edits `rest_upstream_addr` in gateway.json to a real address
// can no longer clear it with `kubectl set env`, because no environment value
// can beat the file. If api-upstream-config were then lost, the gateway would
// fall back to that file's real address instead of refusing to start — the
// fail-fast this ConfigMap's ordering is meant to guarantee (see
// `TestGatewayRestUpstreamIsDeclaredAndRequired`) would quietly stop applying.
//
// So the key ships empty in the file and is turned on only through the
// environment, and this test pins the property that makes that rule necessary
// rather than merely stating it in a comment.
//
// It used to cover a second key, resume_addr, under identical reasoning. That
// address is deleted — the wake-up RPC rides gateway.scheduler_addr's
// connection — so one key is left to pin.
func TestAnEmptyEnvironmentValueCannotTurnTheApiUpstreamSwitchOff(t *testing.T) {
	path := filepath.Join(t.TempDir(), "gateway.json")
	if err := os.WriteFile(path, []byte(
		`{"gateway":{"rest_upstream_addr":"http://agentenv-api:8000"}}`,
	), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	t.Setenv("GATEWAY_REST_UPSTREAM_ADDR", "")
	cfg, err := Load(path, "gateway")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Gateway.RestUpstreamAddr == "" {
		t.Fatal("an empty environment value cleared a file value; if that ever " +
			"becomes true, the note in kustomization.yaml about where the off position lives is " +
			"wrong and should be rewritten rather than left to mislead")
	}

	// ...whereas a non-empty one does take, which is how 3a is turned on and —
	// given the file ships empty — turned back off.
	t.Setenv("GATEWAY_REST_UPSTREAM_ADDR", "http://elsewhere:8000")
	cfg, err = Load(path, "gateway")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Gateway.RestUpstreamAddr != "http://elsewhere:8000" {
		t.Fatalf("the environment did not override the file: got %q", cfg.Gateway.RestUpstreamAddr)
	}
}
