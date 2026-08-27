package config

import (
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

// The incarnation work's switches — routing projection, execution fencing, and
// 阶段 3a's REST upstream — as manifest assertions.
//
// 🔴 Every one of these was flipped on the live cluster with `kubectl patch`
// and left out of this repository, which is the same shape of hole
// `snapshot_catalog_manifest_test.go` was written to close and the same reason:
// an `apply -k` would have put all five back to the release's *starting*
// values, and not one of the five reports anything when it moves in that
// direction. Arbitration that stops enforcing routes to whichever node reported
// last. A gateway that stops refusing forwards what it should have rejected. A
// projection switched off is a gateway reading a routing table nobody is
// writing. And the REST upstream emptied while the DaemonSet holds `--role
// node` is not a rollback at all — it is 404 from every node in the fleet.
//
// So these tests pin the values, and each pin carries a control, because the
// easy version of this file — "the ConfigMap has these keys" — would pass just
// as green against a ConfigMap that shipped them all `off`.

const (
	fencingConfigMap    = "execution-fencing-config"
	projectionConfigMap = "routing-projection-config"
	upstreamConfigMap   = "api-upstream-config"
	pausedConfigMap     = "paused-registry-config"

	arbitrationEnv  = "SCHEDULER_ROUTING_EXECUTION_ARBITRATION"
	gatewayFenceEnv = "GATEWAY_ROUTING_EXECUTION_FENCING"
	writeFencingEnv = "SCHEDULER_REGISTRY_WRITE_FENCING"

	restUpstreamEnv = "GATEWAY_REST_UPSTREAM_ADDR"
	resumeAddrEnv   = "GATEWAY_RESUME_ADDR"
)

// projectionEnvs are the three environment variables the routing projection is
// carried by: two write-side switches in two different processes, and one read
// switch. Three and not one, because they are flipped in an order and a merged
// switch could not express it.
var projectionEnvs = []string{
	"SCHEDULER_ROUTING_PROJECTION_AUTHORITATIVE",
	"GATEWAY_ROUTING_PROJECTION_AUTHORITATIVE",
	"GATEWAY_ROUTING_PROJECTION_READ",
}

// 🔴 The values, and — for each — the value it must not have drifted back to.
//
// The "starting" column is not decoration. It is the exact state an `apply -k`
// produced for months, so a test that only asserted "the key is present" would
// have gone green throughout. Pinning the running value against the starting
// value is what makes this a regression test rather than an inventory.
func TestTheExecutionSwitchesShipTheStateTheClusterRuns(t *testing.T) {
	for _, tc := range []struct {
		configMap string
		key       string
		want      string
		starting  string
	}{
		// Flipped once heartbeat_legacy_roster_total reached zero.
		{configMap: fencingConfigMap, key: arbitrationEnv, want: "enforce", starting: "observe"},
		// Flipped after the node-side gate was verified.
		{configMap: fencingConfigMap, key: gatewayFenceEnv, want: "enforce", starting: "off"},
		// On from the moment the scheduler carrying it starts; it protects the
		// rows that cannot be reconstructed, so it never had a starting value
		// other than its end one.
		{configMap: fencingConfigMap, key: writeFencingEnv, want: "true", starting: "false"},
		{configMap: projectionConfigMap, key: projectionEnvs[0], want: "on", starting: "off"},
		{configMap: projectionConfigMap, key: projectionEnvs[1], want: "on", starting: "off"},
		{configMap: projectionConfigMap, key: projectionEnvs[2], want: "on", starting: "off"},
	} {
		t.Run(tc.key, func(t *testing.T) {
			got := generatedLiteral(t, tc.configMap, tc.key)
			if got == tc.starting {
				t.Fatalf("%s/%s is back at %q, the value this release *started* from. That is what "+
					"an `apply -k` produced for months while the cluster ran %q, and nothing "+
					"reports the difference — arbitration simply stops judging, the gateway simply "+
					"stops refusing, the projection simply stops being written",
					tc.configMap, tc.key, got, tc.want)
			}
			if got != tc.want {
				t.Fatalf("%s/%s is %q, want %q", tc.configMap, tc.key, got, tc.want)
			}
		})
	}
}

// Control for the values above: each one is a spelling the process actually
// accepts, checked through the same parser the process uses.
//
// 🔴 A typo here does not fall back to a default. `ParseRoutingProjectionSwitch`
// and `ParseSchedulerExecutionArbitration` both refuse an unrecognised value and
// stop the process — deliberately, because guessing would produce a rollout that
// reports success while the switch it was for never moved. So the failure is
// loud, but it is loud on every pod in the fleet at once and at whatever hour
// the apply happened.
func TestTheExecutionSwitchValuesAreSpellingsTheLoaderAccepts(t *testing.T) {
	for _, env := range projectionEnvs {
		on, err := ParseRoutingProjectionSwitch(generatedLiteral(t, projectionConfigMap, env))
		if err != nil {
			t.Fatalf("%s/%s: %v", projectionConfigMap, env, err)
		}
		if !on {
			t.Fatalf("%s/%s parses as off", projectionConfigMap, env)
		}
	}

	arbitration, err := ParseSchedulerExecutionArbitration(generatedLiteral(t, fencingConfigMap, arbitrationEnv))
	if err != nil {
		t.Fatalf("%s/%s: %v", fencingConfigMap, arbitrationEnv, err)
	}
	if arbitration != SchedulerExecutionArbitrationEnforce {
		t.Fatalf("%s parses as %q, want enforce", arbitrationEnv, arbitration)
	}

	fencing, err := ParseGatewayExecutionFencing(generatedLiteral(t, fencingConfigMap, gatewayFenceEnv))
	if err != nil {
		t.Fatalf("%s/%s: %v", fencingConfigMap, gatewayFenceEnv, err)
	}
	if fencing != GatewayExecutionFencingEnforce {
		t.Fatalf("%s parses as %q, want enforce", gatewayFenceEnv, fencing)
	}

	// The parser's own control. If it accepted anything at all, the three
	// assertions above would be measuring nothing.
	if _, err := ParseRoutingProjectionSwitch("enforce"); err == nil {
		t.Fatal("ParseRoutingProjectionSwitch accepted \"enforce\"; it is supposed to refuse " +
			"anything outside on/off, and the checks above lean on that")
	}
}

// 🔴 The two ConfigMaps stand in opposite relations to the code, and that is
// the point of checking them together.
//
// The projection defaults **off** in code and is switched on here: `off` is the
// only safe code default, because turning the write side on gives
// ReportSandboxEvent — which every node already sends — the power to delete
// routing records, and a cluster that merely rolled an image must not acquire
// that. So these three literals are load-bearing: delete the ConfigMap and the
// projection is off.
//
// Execution fencing defaults **enforce** in code and is set to enforce here:
// the release feared a cluster parked in `observe` with nobody aware it had
// never been flipped, so the code was made to arrive at the end state on its
// own. These literals are therefore *not* what holds the cluster there — they
// are the seam that keeps a rollback a value change.
//
// Checked as one test because each is the other's control. A helper that
// returned zero values, or a defaultConfig that had quietly stopped defaulting,
// would have to satisfy "differs from the default" and "equals the default" at
// the same time, and cannot.
func TestTheTwoSwitchConfigMapsStandInOppositeRelationsToTheCode(t *testing.T) {
	clearProjectionEnv(t)
	t.Setenv(arbitrationEnv, "")
	t.Setenv(gatewayFenceEnv, "")

	gateway, err := Load("", "gateway")
	if err != nil {
		t.Fatalf("load gateway defaults: %v", err)
	}
	scheduler, err := Load("", "scheduler")
	if err != nil {
		t.Fatalf("load scheduler defaults: %v", err)
	}

	// Load("") is the code's own opinion, with no file and no environment.
	if gateway.Gateway.Routing.ProjectionRead || gateway.Gateway.Routing.ProjectionAuthoritative ||
		scheduler.Scheduler.Routing.ProjectionAuthoritative {
		t.Fatal("the routing projection no longer defaults off in code. That default is what stops " +
			"a cluster acquiring record-deleting powers by rolling an image; if it has moved " +
			"deliberately, this test and the three literals it guards need deciding on together")
	}
	for _, env := range projectionEnvs {
		if value := generatedLiteral(t, projectionConfigMap, env); value == "off" {
			t.Fatalf("%s/%s is %q, which is also the code default — these literals would then be "+
				"doing nothing, and losing the ConfigMap would be invisible rather than a change",
				projectionConfigMap, env, value)
		}
	}

	if got := scheduler.Scheduler.Routing.ExecutionArbitration; got != SchedulerExecutionArbitrationEnforce {
		t.Fatalf("scheduler arbitration defaults to %q, want enforce; the ConfigMap literal was set "+
			"to enforce precisely because the code arrives there on its own, and if that has "+
			"changed then losing %s is now a silent downgrade", got, fencingConfigMap)
	}
	if got := gateway.Gateway.Routing.ExecutionFencing; got != GatewayExecutionFencingEnforce {
		t.Fatalf("gateway fencing defaults to %q, want enforce; same as above", got)
	}
	if got := generatedLiteral(t, fencingConfigMap, arbitrationEnv); got != string(SchedulerExecutionArbitrationEnforce) {
		t.Fatalf("%s/%s is %q while the code default is enforce; the ConfigMap is now weaker than "+
			"the code, so losing it would *strengthen* the cluster and keeping it holds the "+
			"cluster back — decide which was meant", fencingConfigMap, arbitrationEnv, got)
	}
	if got := generatedLiteral(t, fencingConfigMap, gatewayFenceEnv); got != string(GatewayExecutionFencingEnforce) {
		t.Fatalf("%s/%s is %q while the code default is enforce; same as above",
			fencingConfigMap, gatewayFenceEnv, got)
	}
}

// 🔴 The mounted files must not be weaker than the ConfigMap.
//
// Every one of these switches is read from a ConfigMap key with `optional:
// true`, so a cluster that lost the ConfigMap falls through to the file the
// Deployment mounts. If that file names the *starting* value, losing the
// ConfigMap is a silent downgrade rather than a fall back to the code's own
// end-state default — the file beats the default, and only the environment
// beats the file.
//
// `write_enabled` is here for a sharper reason: it defaults **off** in code and
// is deliberately kept that way, because switching it on makes the scheduler the
// owner of a table the nodes are still writing. The live cluster ran it on
// through an environment literal that no manifest produced, so an apply would
// have taken the write surface — the migration, the PausedRegistry service and
// the reclamation timer — down without a word.
func TestTheMountedFilesDoNotUndoTheSwitches(t *testing.T) {
	clearProjectionEnv(t)
	t.Setenv(arbitrationEnv, "")
	t.Setenv(gatewayFenceEnv, "")
	t.Setenv("SCHEDULER_REGISTRY_WRITE_ENABLED", "")
	t.Setenv(writeFencingEnv, "")

	scheduler, err := Load(filepath.Join(manifestDir, "config", "scheduler.json"), "scheduler")
	if err != nil {
		t.Fatalf("loading the mounted scheduler config failed: %v", err)
	}
	if got := scheduler.Scheduler.Routing.ExecutionArbitration; got != SchedulerExecutionArbitrationEnforce {
		t.Fatalf("the mounted scheduler config arbitrates %q; a cluster that lost %s would fall to "+
			"this file, and this file would hold it back", got, fencingConfigMap)
	}
	if !scheduler.Scheduler.Registry.WriteFencing {
		t.Fatal("the mounted scheduler config turns registry write fencing off")
	}
	if !scheduler.Scheduler.Registry.WriteEnabled {
		t.Fatal("the mounted scheduler config does not enable the registry write surface. It " +
			"defaults off in code and this cluster has run it on since the changeover, through an " +
			"environment literal no manifest produced — which is exactly how it would come back " +
			"off on the next apply, taking the migration, the PausedRegistry service and the " +
			"reclamation timer with it and reporting nothing")
	}

	gateway, err := Load(filepath.Join(manifestDir, "config", "gateway.json"), "gateway")
	if err != nil {
		t.Fatalf("loading the mounted gateway config failed: %v", err)
	}
	if got := gateway.Gateway.Routing.ExecutionFencing; got != GatewayExecutionFencingEnforce {
		t.Fatalf("the mounted gateway config fences %q; same as above", got)
	}

	// Control: this loader does read these files rather than silently handing
	// back defaults. `scheduler_addr` is a value only the file can supply.
	if gateway.Gateway.SchedulerAddr == "" {
		t.Fatal("the mounted gateway config came back with no scheduler address, so the " +
			"assertions above are reading defaults rather than the file")
	}
}

// 🔴 The gateway's REST upstream is not optional, because the nodes never serve
// user-facing REST.
//
// `aenv-node` answers 404 on every user-facing REST route
// (`src/api/role_gate.rs`), and there is no argument, environment variable or
// ConfigMap key that changes that — it is which binary the DaemonSet's image
// runs. The gateway only sends REST somewhere that answers when
// `GATEWAY_REST_UPSTREAM_ADDR` names the api half. Empty that key and every
// REST call in the cluster 404s.
//
// 🔴 This used to be a *conditional*: the DaemonSet passed `aenv-node`, and
// the assertion fired only while it did, because emptying the key was a
// legitimate rollback of 阶段 3a as long as the DaemonSet went back to
// `--role all` in the same apply. That pair no longer exists — rolling the
// nodes back is an image-tag change, which this manifest cannot express as an
// argument, and an apply against the current image with these keys empty is an
// outage with no matching half. So the requirement is unconditional now.
func TestTheRestUpstreamIsAlwaysSetBecauseNodesNeverServeRest(t *testing.T) {
	upstream := generatedLiteral(t, upstreamConfigMap, restUpstreamEnv)
	resume := generatedLiteral(t, upstreamConfigMap, resumeAddrEnv)

	if upstream == "" || resume == "" {
		t.Fatalf("%s ships REST upstream %q and resume %q. The node DaemonSet runs aenv-node, "+
			"which answers 404 on the sandboxes routes whatever it is passed, so a gateway with "+
			"no api upstream has nowhere to send user-facing REST — emptying these two is not a "+
			"rollback of anything, it is an outage",
			upstreamConfigMap, upstream, resume)
	}

	// Both addresses have to be ones the loader can use, and they have to name
	// the ports the api Service actually publishes. A REST upstream the gateway
	// cannot parse stops the process; one that parses and names the wrong port
	// does not.
	parsed, err := ParseRestUpstream(upstream)
	if err != nil {
		t.Fatalf("%s/%s does not parse: %v", upstreamConfigMap, restUpstreamEnv, err)
	}
	if parsed == "" {
		t.Fatalf("%s/%s parses to an empty upstream", upstreamConfigMap, restUpstreamEnv)
	}

	http, grpc := apiServicePorts(t)
	assertPort(t, restUpstreamEnv, upstream, http)
	assertPort(t, resumeAddrEnv, resume, grpc)
}

// assertPort checks that an address — a URL or a bare host:port — names the
// given port.
func assertPort(t *testing.T, env, addr string, want int32) {
	t.Helper()

	hostport := addr
	if strings.Contains(addr, "://") {
		parsed, err := url.Parse(addr)
		if err != nil {
			t.Fatalf("%s is %q, which is not a URL: %v", env, addr, err)
		}
		hostport = parsed.Host
	}
	_, port, found := strings.Cut(hostport, ":")
	if !found {
		t.Fatalf("%s is %q and names no port; the api Service publishes %d", env, addr, want)
	}
	got, err := strconv.Atoi(port)
	if err != nil {
		t.Fatalf("%s is %q, whose port is not a number", env, addr)
	}
	if int32(got) != want {
		t.Fatalf("%s is %q, but the api Service publishes that traffic on %d. The gateway would "+
			"dial a port nothing serves", env, addr, want)
	}
}

// apiServicePorts returns the api Service's http and grpc ports.
func apiServicePorts(t *testing.T) (int32, int32) {
	t.Helper()

	var service corev1.Service
	decodeManifest(t, filepath.Join(manifestDir, "agentenv-api-service.yaml"), &service)

	var http, grpc int32
	for _, port := range service.Spec.Ports {
		switch port.Name {
		case "http":
			http = port.Port
		case "grpc":
			grpc = port.Port
		}
	}
	if http == 0 || grpc == 0 {
		t.Fatalf("the api Service publishes http=%d grpc=%d; this test cannot check an address "+
			"against a port it could not read", http, grpc)
	}
	return http, grpc
}

// 🔴 Every ConfigMap the base layer *reads* is a ConfigMap the base layer
// *generates*.
//
// This is the drift that hides best, because it produces no error at either
// end. `paused-registry-config` was referenced by the node DaemonSet from the
// day it was written and generated by nothing, so the reference resolved to
// nothing on every cluster that ever applied this tree. `optional: true` — which
// is right, and which every one of these references carries — is what made it
// silent: the node fell back to a node-local paused registry and pauses simply
// stopped being recoverable anywhere else. The live cluster had a `central`
// literal patched onto it by hand to compensate, which no manifest produced and
// the next apply would have removed.
//
// A reference with nothing behind it is not a default, it is a value nobody
// chose. So the scan is over the whole base layer rather than the one workload
// that had the bug.
func TestEveryConfigMapTheBaseLayerReadsIsOneItGenerates(t *testing.T) {
	generated := generatedConfigMapNames(t)
	if len(generated) < 5 {
		t.Fatalf("the base layer generates %d ConfigMaps (%v); that is too few for this scan to be "+
			"reading the generator at all", len(generated), generated)
	}

	entries, err := os.ReadDir(manifestDir)
	if err != nil {
		t.Fatalf("reading the base layer failed: %v", err)
	}

	references := 0
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".yaml") || entry.Name() == "kustomization.yaml" {
			continue
		}
		path := filepath.Join(manifestDir, entry.Name())
		for _, workload := range podWorkloads(t, path) {
			for _, container := range workload.containers {
				for _, env := range container.Env {
					if env.ValueFrom == nil || env.ValueFrom.ConfigMapKeyRef == nil {
						continue
					}
					references++
					name := env.ValueFrom.ConfigMapKeyRef.Name
					if !generated[name] {
						t.Fatalf("%s reads %s from the ConfigMap %q, which nothing in the base "+
							"layer generates. The reference resolves to nothing, and because these "+
							"references are optional the process falls back to a code default "+
							"without saying so",
							workload.name, env.Name, name)
					}
				}
			}
		}
	}

	if references < 7 {
		t.Fatalf("the scan found %d configMapKeyRef entries; the base layer carries at least the "+
			"cluster id, the sandbox proxy domains, the paused registry backend, the two snapshot "+
			"catalog keys and the fencing and projection switches, so it is not seeing the tree",
			references)
	}

	// Control: the scan can tell a generated name from an ungenerated one. If
	// `generated` answered true for everything, the walk above would pass
	// against any tree at all.
	if generated["a-config-map-nothing-generates"] {
		t.Fatal("the generator lookup answers true for a name that is not in it; the scan above " +
			"is measuring the lookup rather than the manifests")
	}
	if !generated[pausedConfigMap] {
		t.Fatalf("%s is not generated by the base layer. It is read by the node DaemonSet, and a "+
			"reference with nothing behind it silently means `local` — pauses that only the "+
			"machine that made them can resume", pausedConfigMap)
	}
	if got := generatedLiteral(t, pausedConfigMap, "AENV_PAUSED_REGISTRY_BACKEND"); got != "central" {
		t.Fatalf("%s carries backend %q, want \"central\"; the cluster has run the shared registry "+
			"since the changeover and `local` is the value that loses cross-node recovery without "+
			"reporting anything", pausedConfigMap, got)
	}
}

// generatedConfigMapNames returns the names of every ConfigMap the base layer's
// configMapGenerator produces, whether from literals or from files.
func generatedConfigMapNames(t *testing.T) map[string]bool {
	t.Helper()

	var kustomization struct {
		ConfigMapGenerator []struct {
			Name string `json:"name"`
		} `json:"configMapGenerator"`
	}
	decodeManifest(t, filepath.Join(manifestDir, "kustomization.yaml"), &kustomization)

	names := map[string]bool{}
	for _, generator := range kustomization.ConfigMapGenerator {
		names[generator.Name] = true
	}
	return names
}
