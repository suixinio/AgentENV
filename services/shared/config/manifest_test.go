package config

import (
	"net"
	"os"
	"path/filepath"
	"strconv"
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
