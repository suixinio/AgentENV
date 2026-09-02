package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The incarnation work's switches still owned by a running process — the
// gateway's execution fencing and routing projection — plus 阶段 3a's REST
// upstream, as manifest assertions.
//
// 🔴 The scheduler's own half of this work (SCHEDULER_ROUTING_EXECUTION_ARBITRATION,
// SCHEDULER_REGISTRY_WRITE_FENCING, SCHEDULER_ROUTING_PROJECTION_AUTHORITATIVE) is
// gone from this file along with services/scheduler: those settings have no
// reader left to protect, and asserting a ConfigMap literal nothing consumes is
// not a regression test, it is archaeology. Two of the three literals
// (SCHEDULER_REGISTRY_WRITE_FENCING and SCHEDULER_ROUTING_EXECUTION_ARBITRATION)
// have since been deleted from `execution-fencing-config` in
// deploy/k8s/base/kustomization.yaml too. SCHEDULER_ROUTING_PROJECTION_AUTHORITATIVE
// still sits in `routing-projection-config` — untouched, out of that pass's
// scope — but this file only asserts what a live process still reads.
//
// 🔴 Every one of the surviving switches was flipped on the live cluster with
// `kubectl patch` and left out of this repository at the time, which is the
// same shape of hole `snapshot_catalog_manifest_test.go` was written to close
// and the same reason: an `apply -k` would have put them back to the release's
// *starting* values, and none of them reports anything when it moves in that
// direction. A gateway that stops refusing forwards what it should have
// rejected. A projection switched off is a gateway reading a routing table
// nobody is writing. And the REST upstream emptied while the DaemonSet holds
// `--role node` is not a rollback at all — it is 404 from every node in the
// fleet.
//
// So these tests pin the values, and each pin carries a control, because the
// easy version of this file — "the ConfigMap has these keys" — would pass just
// as green against a ConfigMap that shipped them all `off`.

const (
	fencingConfigMap    = "execution-fencing-config"
	projectionConfigMap = "routing-projection-config"

	gatewayFenceEnv = "GATEWAY_ROUTING_EXECUTION_FENCING"
)

// projectionEnvs are the environment variables the gateway's half of the
// routing projection is carried by: the read switch.
var projectionEnvs = []string{
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
		// Flipped after the node-side gate was verified.
		{configMap: fencingConfigMap, key: gatewayFenceEnv, want: "enforce", starting: "off"},
		{configMap: projectionConfigMap, key: projectionEnvs[0], want: "on", starting: "off"},
	} {
		t.Run(tc.key, func(t *testing.T) {
			got := generatedLiteral(t, tc.configMap, tc.key)
			if got == tc.starting {
				t.Fatalf("%s/%s is back at %q, the value this release *started* from. That is what "+
					"an `apply -k` produced for months while the cluster ran %q, and nothing "+
					"reports the difference — the gateway simply stops refusing, the projection "+
					"simply stops being written",
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
// and `ParseGatewayExecutionFencing` both refuse an unrecognised value and
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
// that. So these literals are load-bearing: delete the ConfigMap and the
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
	t.Setenv(gatewayFenceEnv, "")

	gateway, err := Load("")
	if err != nil {
		t.Fatalf("load gateway defaults: %v", err)
	}

	// Load("") is the code's own opinion, with no file and no environment.
	if gateway.Gateway.Routing.ProjectionRead {
		t.Fatal("the routing projection no longer defaults off in code. That default is what stops " +
			"a cluster acquiring record-deleting powers by rolling an image; if it has moved " +
			"deliberately, this test and the literals it guards need deciding on together")
	}
	for _, env := range projectionEnvs {
		if value := generatedLiteral(t, projectionConfigMap, env); value == "off" {
			t.Fatalf("%s/%s is %q, which is also the code default — these literals would then be "+
				"doing nothing, and losing the ConfigMap would be invisible rather than a change",
				projectionConfigMap, env, value)
		}
	}

	if got := gateway.Gateway.Routing.ExecutionFencing; got != GatewayExecutionFencingEnforce {
		t.Fatalf("gateway fencing defaults to %q, want enforce; the ConfigMap literal was set "+
			"to enforce precisely because the code arrives there on its own, and if that has "+
			"changed then losing %s is now a silent downgrade", got, fencingConfigMap)
	}
	if got := generatedLiteral(t, fencingConfigMap, gatewayFenceEnv); got != string(GatewayExecutionFencingEnforce) {
		t.Fatalf("%s/%s is %q while the code default is enforce; the ConfigMap is now weaker than "+
			"the code, so losing it would *strengthen* the cluster and keeping it holds the "+
			"cluster back — decide which was meant", fencingConfigMap, gatewayFenceEnv, got)
	}
}

// 🔴 The mounted file must not be weaker than the ConfigMap.
//
// This switch is read from a ConfigMap key with `optional: true`, so a cluster
// that lost the ConfigMap falls through to the file the Deployment mounts. If
// that file names the *starting* value, losing the ConfigMap is a silent
// downgrade rather than a fall back to the code's own end-state default — the
// file beats the default, and only the environment beats the file.
func TestTheMountedFilesDoNotUndoTheSwitches(t *testing.T) {
	clearProjectionEnv(t)
	t.Setenv(gatewayFenceEnv, "")

	gateway, err := Load(filepath.Join(manifestDir, "config", "gateway.json"))
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

// 🔴 Every ConfigMap the base layer *reads* is a ConfigMap the base layer
// *generates*.
//
// This is the drift that hides best, because it produces no error at either
// end. `paused-registry-config` used to be exactly this bug: referenced by the
// node DaemonSet from the day it was written and generated by nothing, so the
// reference resolved to nothing on every cluster that ever applied this tree.
// `optional: true` — which is right, and which every one of these references
// carries — is what made it silent. That specific ConfigMap is gone now
// (`AENV_PAUSED_REGISTRY_BACKEND` on the DaemonSet never did anything either
// way — the node warns and ignores any value other than `local` — so the fix
// was deletion, not generation), but the shape of bug it caught is not
// specific to it.
//
// A reference with nothing behind it is not a default, it is a value nobody
// chose. So the scan is over the whole base layer rather than one workload.
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
			"cluster id, the sandbox proxy domains, the snapshot repository backend and the "+
			"fencing and projection switches, so it is not seeing the tree",
			references)
	}

	// Control: the scan can tell a generated name from an ungenerated one. If
	// `generated` answered true for everything, the walk above would pass
	// against any tree at all.
	if generated["a-config-map-nothing-generates"] {
		t.Fatal("the generator lookup answers true for a name that is not in it; the scan above " +
			"is measuring the lookup rather than the manifests")
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
