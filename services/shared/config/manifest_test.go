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
	t.Setenv("SCHEDULER_METRICS_LISTEN_ADDR", "")

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
		{
			service:    "scheduler",
			configFile: "config/scheduler.json",
			deployment: "scheduler-deployment.yaml",
			svc:        "scheduler-service.yaml",
			metricsOf:  func(c Config) string { return c.Scheduler.MetricsListenAddr },
		},
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

// 🔴 The node and the scheduler have to agree on which cluster they are, and
// neither can tell that they do not.
//
// The node stamps this id on every paused-sandbox row it writes; the scheduler
// scopes every registry read, every registry write and the reclamation timer to
// it. Two different values produce no error anywhere: the rows are written, the
// RPCs succeed, and the scheduler simply serves a cluster that has no rows in
// it while nobody reclaims the ones the nodes are leaving behind.
//
// It was worse than that before this test existed. The scheduler read the id
// from a key on the `agentenv-postgres` Secret that no manifest, script or
// helper ever created, so on a fresh cluster the write surface came up
// permanently cold — every registry RPC answered UNAVAILABLE, which from a
// node looks exactly like a scheduler that is down, while the process is
// healthy by every other measure and the only trace is one error line at
// startup. Somebody had to `kubectl patch secret` by hand to make it work.
//
// So: one value, in the base layer, read by both — and a cluster id is a name
// rather than a credential, so a Secret was never the right place for it.
func TestOneClusterIdentityReachesBothSidesOfTheRegistry(t *testing.T) {
	var node appsv1.DaemonSet
	decodeManifest(t, filepath.Join(manifestDir, "agentenv-daemonset.yaml"), &node)
	var scheduler appsv1.Deployment
	decodeManifest(t, filepath.Join(manifestDir, "scheduler-deployment.yaml"), &scheduler)

	nodeRef := clusterIDSource(t, "the node DaemonSet", node.Spec.Template.Spec.Containers, "AENV_CLUSTER_ID")
	schedulerRef := clusterIDSource(t, "the scheduler Deployment", scheduler.Spec.Template.Spec.Containers, "SCHEDULER_REGISTRY_CLUSTER_ID")

	if nodeRef.Name != schedulerRef.Name || nodeRef.Key != schedulerRef.Key {
		t.Fatalf("the two sides read their cluster id from different places: node %s/%s, scheduler %s/%s — "+
			"two places is two values to keep in step, and nothing reports it when they part",
			nodeRef.Name, nodeRef.Key, schedulerRef.Name, schedulerRef.Key)
	}

	value := generatedLiteral(t, nodeRef.Name, nodeRef.Key)
	if value == "" {
		t.Fatalf("%s/%s is generated empty; an empty cluster id leaves the scheduler's write surface "+
			"registered and cold, answering every registry RPC UNAVAILABLE", nodeRef.Name, nodeRef.Key)
	}

	// The file the node falls back to when the ConfigMap is absent. A fallback
	// that disagrees with the ConfigMap is the same split as above, reached by
	// deleting an object instead of by editing one.
	if fallback := nodeIdentityClusterID(t); fallback != value {
		t.Fatalf("%s/%s is %q but config/default.toml's [node_identity].cluster_id is %q; "+
			"a node that loses the ConfigMap would start writing rows into another cluster",
			nodeRef.Name, nodeRef.Key, value, fallback)
	}
}

// clusterIDSource returns the ConfigMap key an env var is read from, and fails
// if it is read from anywhere else. A Secret is the specific "anywhere else"
// this guards: that is where the scheduler's copy used to live, and putting a
// name behind a credential is what made it something no manifest supplied.
func clusterIDSource(t *testing.T, where string, containers []corev1.Container, env string) *corev1.ConfigMapKeySelector {
	t.Helper()

	if len(containers) != 1 {
		t.Fatalf("%s: expected one container, got %d", where, len(containers))
	}
	for _, candidate := range containers[0].Env {
		if candidate.Name != env {
			continue
		}
		if candidate.ValueFrom == nil || candidate.ValueFrom.ConfigMapKeyRef == nil {
			t.Fatalf("%s reads %s from something other than a ConfigMap key (%+v); the cluster id is a name, "+
				"not a credential, and both sides have to read the same one", where, env, candidate)
		}
		return candidate.ValueFrom.ConfigMapKeyRef
	}
	t.Fatalf("%s does not set %s at all; the side that does not get one does not agree with the side that does", where, env)
	return nil
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

// 🔴 阶段 3a's switch is present in the mounted ConfigMap and set to off.
//
// Two halves, and the second is the one worth having. That the key is *there*
// is what makes turning 3a on a value change and turning it back off a value
// change — seconds, and a gateway restart (`_sd-impl-phase3-role.md` §11.1,
// §11.2). A key that has to be added first makes the rollback a manifest edit
// under incident pressure, and the rollback is the thing 3a is staged behind.
//
// That it is set to *off* is the release decision: the shadow phase leaves
// every projection miss falling through to the scheduler, with the node the
// request lands on waking the sandbox itself, which is today's behaviour
// exactly. Turning it on is a deliberate act by an operator who has read the
// runbook, and never a default that arrived with an image.
func TestGatewayResumeAddrIsDeclaredAndOff(t *testing.T) {
	t.Setenv("GATEWAY_RESUME_ADDR", "")

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
	if _, declared := mounted.Gateway["resume_addr"]; !declared {
		t.Fatalf("the mounted gateway config does not name resume_addr, so enabling 阶段 3a " +
			"— or rolling it back — means editing the manifest rather than a value")
	}

	cfg, err := Load(filepath.Join(manifestDir, "config", "gateway.json"), "gateway")
	if err != nil {
		t.Fatalf("loading the mounted gateway config failed: %v", err)
	}
	if cfg.Gateway.ResumeAddr != "" {
		t.Fatalf("the mounted gateway config sends wake-ups to %q; the shadow phase ships with "+
			"this off", cfg.Gateway.ResumeAddr)
	}

	// Resolution: the same loader does carry a value through, so the empty
	// string above is the manifest's decision and not a key nothing reads.
	path := filepath.Join(t.TempDir(), "gateway.json")
	if err := os.WriteFile(path, []byte(`{"gateway":{"resume_addr":"agentenv-api:9090"}}`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	cfg, err = Load(path, "gateway")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Gateway.ResumeAddr != "agentenv-api:9090" {
		t.Fatalf("resume_addr in a config file came out as %q", cfg.Gateway.ResumeAddr)
	}

	// ...and so does the environment, which is how one gateway is flipped
	// without editing the ConfigMap every other gateway shares.
	t.Setenv("GATEWAY_RESUME_ADDR", "agentenv-api-canary:9090")
	cfg, err = Load(filepath.Join(manifestDir, "config", "gateway.json"), "gateway")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Gateway.ResumeAddr != "agentenv-api-canary:9090" {
		t.Fatalf("the environment did not override the manifest: got %q", cfg.Gateway.ResumeAddr)
	}
}

// 🔴 阶段 3a's REST switch is present in the mounted ConfigMap and set to off.
//
// The same two halves as the resume switch above, and the same reason for each.
// That the key is *there* makes turning 3a on a value change and turning it back
// off a value change — seconds, and a gateway roll, with the DaemonSet
// untouched. That it is *off* is the release decision: shipping the switch and
// shipping the traffic move are two different days.
func TestGatewayRestUpstreamIsDeclaredAndOff(t *testing.T) {
	t.Setenv("GATEWAY_REST_UPSTREAM_ADDR", "")

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
		t.Fatalf("the mounted gateway config does not name rest_upstream_addr, so enabling 阶段 3a " +
			"— or rolling it back — means editing the manifest rather than a value")
	}

	cfg, err := Load(filepath.Join(manifestDir, "config", "gateway.json"), "gateway")
	if err != nil {
		t.Fatalf("loading the mounted gateway config failed: %v", err)
	}
	if cfg.Gateway.RestUpstreamAddr != "" {
		t.Fatalf("the mounted gateway config sends user-facing REST to %q; the shadow phase ships "+
			"with this off", cfg.Gateway.RestUpstreamAddr)
	}

	// Resolution: the same loader carries a real address through, from the file
	// and from the environment, so the empty string above is the manifest's
	// decision rather than a key nothing reads.
	path := filepath.Join(t.TempDir(), "gateway.json")
	if err := os.WriteFile(path, []byte(`{"gateway":{"rest_upstream_addr":"http://agentenv-api:8000"}}`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	cfg, err = Load(path, "gateway")
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

// 🔴 Why the off position has to live in the mounted file, and not only in the
// ConfigMap the Deployment reads through the environment.
//
// An environment variable set to the empty string is *ignored* by the loader —
// that is deliberate, and it is what makes `kubectl set env FOO=` a clear rather
// than a set. The consequence is one-directional and easy to miss: an operator
// who enables 3a by editing `rest_upstream_addr` in gateway.json to a real
// address can no longer turn it off with `kubectl set env`, because no
// environment value can beat the file. The rollback would then be a manifest
// edit at the worst possible moment, which is the one thing 3a is staged to
// avoid.
//
// So both keys ship empty in the file and are turned on through the
// environment, and this test pins the property that makes that rule necessary
// rather than merely stating it in a comment.
func TestAnEmptyEnvironmentValueCannotTurnTheApiUpstreamSwitchesOff(t *testing.T) {
	path := filepath.Join(t.TempDir(), "gateway.json")
	if err := os.WriteFile(path, []byte(
		`{"gateway":{"rest_upstream_addr":"http://agentenv-api:8000","resume_addr":"agentenv-api:8002"}}`,
	), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	t.Setenv("GATEWAY_REST_UPSTREAM_ADDR", "")
	t.Setenv("GATEWAY_RESUME_ADDR", "")
	cfg, err := Load(path, "gateway")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Gateway.RestUpstreamAddr == "" || cfg.Gateway.ResumeAddr == "" {
		t.Fatalf("an empty environment value cleared a file value (rest=%q resume=%q); if that ever "+
			"becomes true, the note in kustomization.yaml about where the off position lives is "+
			"wrong and should be rewritten rather than left to mislead",
			cfg.Gateway.RestUpstreamAddr, cfg.Gateway.ResumeAddr)
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
