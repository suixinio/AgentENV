package config

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	k8syaml "k8s.io/apimachinery/pkg/util/yaml"
)

// The manifests 阶段 3a adds. Read from Go because this is the only package in
// the tree that already parses them, and because every property below is one
// that a reviewer would have to hold two files in their head to check.

func apiDeployment(t *testing.T) appsv1.Deployment {
	t.Helper()
	var deployment appsv1.Deployment
	decodeManifest(t, filepath.Join(manifestDir, "agentenv-api-deployment.yaml"), &deployment)
	return deployment
}

func nodeDaemonSet(t *testing.T) appsv1.DaemonSet {
	t.Helper()
	var daemonset appsv1.DaemonSet
	decodeManifest(t, filepath.Join(manifestDir, "agentenv-daemonset.yaml"), &daemonset)
	return daemonset
}

func onlyContainer(t *testing.T, where string, containers []corev1.Container) corev1.Container {
	t.Helper()
	if len(containers) != 1 {
		t.Fatalf("%s: expected one container, got %d", where, len(containers))
	}
	return containers[0]
}

func envValue(container corev1.Container, name string) (corev1.EnvVar, bool) {
	for _, candidate := range container.Env {
		if candidate.Name == name {
			return candidate, true
		}
	}
	return corev1.EnvVar{}, false
}

// 🔴 The api half runs no sandboxes, so it must hold none of the privileges the
// node needs to run them.
//
// Every assertion here is an absence, and an absence proves nothing on its own —
// a test that read the wrong file, or a decoder that silently produced a zero
// value, would pass all of them. So each is paired with the DaemonSet's
// opposite, read through the same decoder in the same test: "the api half is not
// privileged" is evidence exactly because "the node is" is checked beside it.
func TestTheApiHalfIsDeployedWithoutTheNodesPrivileges(t *testing.T) {
	api := apiDeployment(t)
	node := nodeDaemonSet(t)

	apiContainer := onlyContainer(t, "the api Deployment", api.Spec.Template.Spec.Containers)
	nodeContainer := onlyContainer(t, "the node DaemonSet", node.Spec.Template.Spec.Containers)

	// Privilege.
	if nodeContainer.SecurityContext == nil || nodeContainer.SecurityContext.Privileged == nil || !*nodeContainer.SecurityContext.Privileged {
		t.Fatal("the node DaemonSet is not privileged, so this test's control is gone: either the " +
			"node stopped needing /dev/kvm, or this test is reading the wrong file")
	}
	if apiContainer.SecurityContext != nil && apiContainer.SecurityContext.Privileged != nil && *apiContainer.SecurityContext.Privileged {
		t.Fatal("the api Deployment is privileged; it touches no /dev/kvm, creates no netns and " +
			"opens no ublk device, and a control-plane Pod with a machine's privileges is the " +
			"exact shape §0 of the split proposal exists to refuse")
	}

	// Host state. The node mounts the machine's local sandbox directory and its
	// /dev; neither may appear here in any form.
	nodeHostPaths := hostPathVolumes(node.Spec.Template.Spec.Volumes)
	if len(nodeHostPaths) == 0 {
		t.Fatal("the node DaemonSet mounts no hostPath at all, so the check below has no control")
	}
	if apiHostPaths := hostPathVolumes(api.Spec.Template.Spec.Volumes); len(apiHostPaths) != 0 {
		t.Fatalf("the api Deployment mounts host paths %v; that hands a process which owns the "+
			"cluster's view of every sandbox write access to one machine's local state", apiHostPaths)
	}

	// The drain budget. 3600 seconds is the node's, and it is there to let a
	// machine pause and persist every sandbox on it before it goes.
	if node.Spec.Template.Spec.TerminationGracePeriodSeconds == nil || *node.Spec.Template.Spec.TerminationGracePeriodSeconds != 3600 {
		t.Fatalf("the node's grace period is no longer 3600s (%v); this test's control is gone",
			node.Spec.Template.Spec.TerminationGracePeriodSeconds)
	}
	grace := api.Spec.Template.Spec.TerminationGracePeriodSeconds
	if grace == nil {
		t.Fatal("the api Deployment names no terminationGracePeriodSeconds; it takes the 30s default, " +
			"which is a choice worth making on purpose")
	}
	if *grace > 300 {
		t.Fatalf("the api Deployment waits %ds to shut down; it has no sandboxes to drain, and a "+
			"copy of the node's 3600 makes every api rollout take an hour for no reason", *grace)
	}

	// Two replicas, because the half whose ledger is shared is the half that
	// can have more than one.
	if api.Spec.Replicas == nil || *api.Spec.Replicas != 2 {
		t.Fatalf("the api Deployment declares %v replicas, want 2", api.Spec.Replicas)
	}
}

func hostPathVolumes(volumes []corev1.Volume) []string {
	paths := make([]string, 0, 2)
	for _, volume := range volumes {
		if volume.HostPath != nil {
			paths = append(paths, volume.Name+"="+volume.HostPath.Path)
		}
	}
	return paths
}

// 🔴 §0's second hard constraint of the split proposal: the `node` role holds no
// shared-storage credentials. Every machine that runs user code would otherwise
// carry the address of — and the way into — the cluster's own ledger.
//
// The pairing is the test. "The DaemonSet has no store configuration" is true of
// a cluster that has none anywhere; it means something only next to "the api
// Deployment has all of it".
func TestOnlyTheApiHalfIsToldWhereTheClusterStoreIs(t *testing.T) {
	apiContainer := onlyContainer(t, "the api Deployment", apiDeployment(t).Spec.Template.Spec.Containers)
	nodeContainer := onlyContainer(t, "the node DaemonSet", nodeDaemonSet(t).Spec.Template.Spec.Containers)

	backend, ok := envValue(apiContainer, "AENV_ORCHESTRATOR_STORE_BACKEND")
	if !ok || backend.Value != "redis" {
		t.Fatalf("the api Deployment does not select the cluster store (%q); `--role api` refuses to "+
			"start on the in-memory one rather than run a replica whose ledger no other replica "+
			"can see", backend.Value)
	}
	if url, ok := envValue(apiContainer, "AENV_ORCHESTRATOR_STORE_REDIS_URL"); !ok || strings.TrimSpace(url.Value) == "" {
		t.Fatal("the api Deployment selects the Redis store and does not say where it is")
	}

	for _, forbidden := range []string{
		"AENV_ORCHESTRATOR_STORE_BACKEND",
		"AENV_ORCHESTRATOR_STORE_REDIS_URL",
		"AENV_REDIS_ADDR",
	} {
		if _, ok := envValue(nodeContainer, forbidden); ok {
			t.Fatalf("the node DaemonSet carries %s; the shared store's addressing belongs to the "+
				"deciding half alone, and putting it on every machine that runs user code undoes "+
				"the constraint the split is built on", forbidden)
		}
	}
	// Broader than the three names above, because the failure is a value
	// arriving under a name nobody thought to forbid.
	for _, candidate := range nodeContainer.Env {
		if strings.Contains(strings.ToLower(candidate.Value), "redis") {
			t.Fatalf("the node DaemonSet's %s names redis (%q)", candidate.Name, candidate.Value)
		}
	}
}

// 🔴 AENV_STARTUP_RECLAIM_ENABLED=true turns a process into one that sweeps the
// host for another process's leftover VMs. That is right for `--role node`,
// where it is the default, and destructive anywhere two servers can share a
// machine — which is exactly what 3a is: a DaemonSet still on `--role all`
// alongside whatever else lands on that host.
//
// The scan's own control is at the bottom: the same walk over the same files
// finds an environment variable that *is* there, so "found nothing" is a fact
// about the manifests rather than about the walk.
func TestNoManifestSwitchesOnTheStartupHostSweep(t *testing.T) {
	const forbidden = "AENV_STARTUP_RECLAIM_ENABLED"
	const control = "AENV_PAUSED_REGISTRY_BACKEND"

	entries, err := os.ReadDir(manifestDir)
	if err != nil {
		t.Fatalf("reading the base layer failed: %v", err)
	}

	scanned := 0
	controlSeen := false
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".yaml") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(manifestDir, entry.Name()))
		if err != nil {
			t.Fatalf("reading %s failed: %v", entry.Name(), err)
		}
		scanned++
		text := string(raw)
		// The name may appear in prose saying why it is absent. What must not
		// appear is a manifest setting it.
		for _, line := range strings.Split(text, "\n") {
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "#") {
				continue
			}
			if strings.Contains(trimmed, forbidden) {
				t.Fatalf("%s sets %s (%q); it makes a process tear down VMs it did not start, and "+
					"during 阶段 3a every machine still runs a `--role all` node that did",
					entry.Name(), forbidden, trimmed)
			}
		}
		if strings.Contains(text, control) {
			controlSeen = true
		}
	}

	if scanned < 5 {
		t.Fatalf("only %d manifests were scanned; this test is reading the wrong directory", scanned)
	}
	if !controlSeen {
		t.Fatalf("the scan never found %s either, so finding no %s says nothing about the manifests",
			control, forbidden)
	}
}

// The two addresses the gateway is pointed at have to exist, resolve to the
// ports the process actually listens on, and select the right Pods. Each of
// those is a separate way for the switch to be flipped onto nothing.
func TestTheApiServiceCarriesTheTwoAddressesTheGatewayIsPointedAt(t *testing.T) {
	deployment := apiDeployment(t)
	container := onlyContainer(t, "the api Deployment", deployment.Spec.Template.Spec.Containers)

	var service corev1.Service
	decodeManifest(t, filepath.Join(manifestDir, "agentenv-api-service.yaml"), &service)

	for _, tc := range []struct {
		portName string
		env      string
		what     string
	}{
		{portName: "http", env: "API_ADDR", what: "user-facing REST (gateway.rest_upstream_addr)"},
		{portName: "grpc", env: "AENV_API_GRPC_ADDR", what: "the wake-up surface (gateway.resume_addr)"},
	} {
		t.Run(tc.portName, func(t *testing.T) {
			listen, ok := envValue(container, tc.env)
			if !ok {
				t.Fatalf("the api Deployment does not set %s, so the port it listens on for %s is "+
					"whatever the code defaults to and nothing here can check it", tc.env, tc.what)
			}
			wantPort := listenPort(t, listen.Value)

			declared := int32(0)
			for _, port := range container.Ports {
				if port.Name == tc.portName {
					declared = port.ContainerPort
				}
			}
			if declared != wantPort {
				t.Fatalf("%s listens on %d but the container declares port %q as %d",
					tc.what, wantPort, tc.portName, declared)
			}

			served := false
			for _, port := range service.Spec.Ports {
				if port.TargetPort.StrVal == tc.portName || int32(port.TargetPort.IntValue()) == wantPort {
					served = true
				}
			}
			if !served {
				t.Fatalf("the api Service does not expose %q, so %s is unreachable by name",
					tc.portName, tc.what)
			}
		})
	}

	// A Service whose selector matches nothing answers with no endpoints, and a
	// gateway pointed at it fails every REST call with a connection refused.
	labels := deployment.Spec.Template.ObjectMeta.Labels
	for key, want := range service.Spec.Selector {
		if labels[key] != want {
			t.Fatalf("the api Service selects %s=%q and the Pods carry %q", key, want, labels[key])
		}
	}
	if len(service.Spec.Selector) == 0 {
		t.Fatal("the api Service has no selector at all")
	}

	var budget policyv1.PodDisruptionBudget
	decodeManifest(t, filepath.Join(manifestDir, "agentenv-api-pdb.yaml"), &budget)
	if budget.Spec.Selector == nil || len(budget.Spec.Selector.MatchLabels) == 0 {
		t.Fatal("the api PodDisruptionBudget selects nothing, so it protects nothing")
	}
	for key, want := range budget.Spec.Selector.MatchLabels {
		if labels[key] != want {
			t.Fatalf("the api PodDisruptionBudget selects %s=%q and the Pods carry %q", key, want, labels[key])
		}
	}
}

// 🔴 3a is "the DaemonSet stays on `--role all`, and an api half comes up beside
// it". Both halves of that sentence are checked here, because the expensive
// mistake is doing 3b's half by accident: a DaemonSet that acquires `--role
// node` stops serving REST on every machine at once, and putting that back is a
// serial roll with an hour of grace per node.
func TestTheApiDeploymentNamesItsRoleAndTheDaemonSetStillNamesNone(t *testing.T) {
	apiContainer := onlyContainer(t, "the api Deployment", apiDeployment(t).Spec.Template.Spec.Containers)

	if got := strings.Join(apiContainer.Args, " "); !strings.Contains(got, "--role api") {
		t.Fatalf("the api Deployment's args are %q; without `--role api` this Pod assembles "+
			"`--role all`, which reaches for /dev/kvm on a container that has none", got)
	}

	nodeContainer := onlyContainer(t, "the node DaemonSet", nodeDaemonSet(t).Spec.Template.Spec.Containers)
	if got := strings.Join(nodeContainer.Args, " "); strings.Contains(got, "--role") {
		t.Fatalf("the node DaemonSet passes %q. During 阶段 3a it stays on `--role all`: it keeps "+
			"serving REST the whole time, which is what makes rolling 3a back a gateway value "+
			"change instead of a fleet-wide roll", got)
	}
	if role, ok := envValue(nodeContainer, "AENV_ROLE"); ok && strings.TrimSpace(role.Value) != "all" {
		t.Fatalf("the node DaemonSet sets AENV_ROLE=%q, which is 3b and not 3a", role.Value)
	}
}

// The gateway's end of both switches: declared on the Deployment, sourced from a
// ConfigMap the base layer generates, and generated empty.
//
// 🔴 Declared-but-empty and absent are different states, and only the first one
// makes each direction of the flip a value change. A key that has to be added to
// this list before it can be set turns enabling 3a into a manifest edit, and —
// the half that actually costs something — turns rolling it back into a manifest
// edit during an incident.
func TestTheGatewayCanBeFlippedToTheApiHalfWithoutEditingAManifest(t *testing.T) {
	var gateway appsv1.Deployment
	decodeManifest(t, filepath.Join(manifestDir, "gateway-deployment.yaml"), &gateway)
	container := onlyContainer(t, "the gateway Deployment", gateway.Spec.Template.Spec.Containers)

	for _, name := range []string{"GATEWAY_REST_UPSTREAM_ADDR", "GATEWAY_RESUME_ADDR"} {
		t.Run(name, func(t *testing.T) {
			declared, ok := envValue(container, name)
			if !ok {
				t.Fatalf("the gateway Deployment does not declare %s, so flipping 阶段 3a — in "+
					"either direction — means editing this manifest", name)
			}
			if declared.ValueFrom == nil || declared.ValueFrom.ConfigMapKeyRef == nil {
				t.Fatalf("%s is not read from a ConfigMap key (%+v); it is an address rather than a "+
					"credential, and an operator has to be able to change it in one place", name, declared)
			}
			ref := declared.ValueFrom.ConfigMapKeyRef
			if ref.Optional == nil || !*ref.Optional {
				t.Fatalf("%s is a required ConfigMap key; a cluster that has not created that "+
					"ConfigMap would fail to start its gateway over a switch that is off", name)
			}
			if value := generatedLiteral(t, ref.Name, ref.Key); value != "" {
				t.Fatalf("%s/%s is generated as %q; both switches ship off, and turning them on is "+
					"a deliberate act by an operator who has read the runbook rather than something "+
					"that arrives with an image", ref.Name, ref.Key, value)
			}
		})
	}

	// The control for the two empties above: generatedLiteral does read values
	// out of that file, and does tell "generated empty" from "not generated at
	// all". Without this, both assertions would also pass against a lookup that
	// silently returned "" for everything.
	if value := generatedLiteral(t, "cluster-identity-config", "CLUSTER_ID"); value == "" {
		t.Fatal("the generator lookup returns empty for a literal that is not empty; the two " +
			"assertions above are measuring the lookup rather than the manifests")
	}
}

// 🔴 A manifest that has never been parsed is not a manifest.
//
// Every file this phase adds or edits is decoded here into the typed object the
// cluster would build from it, with unknown fields rejected. That is the check
// `kubectl apply --dry-run=client` would give, in a form that runs in CI rather
// than once on somebody's laptop — and it is the one that catches the failure
// this batch is most exposed to: a misspelled field is valid YAML, applies
// without complaint, and simply does not do the thing it was written for.
// `terminationGracePeriod` instead of `terminationGracePeriodSeconds` is a
// 30-second grace period and no error anywhere.
func TestTheManifestsThisPhaseTouchesParseStrictly(t *testing.T) {
	for _, tc := range []struct {
		file string
		into func() any
	}{
		{"agentenv-api-deployment.yaml", func() any { return &appsv1.Deployment{} }},
		{"agentenv-api-service.yaml", func() any { return &corev1.Service{} }},
		{"agentenv-api-pdb.yaml", func() any { return &policyv1.PodDisruptionBudget{} }},
		{"gateway-deployment.yaml", func() any { return &appsv1.Deployment{} }},
		{"agentenv-daemonset.yaml", func() any { return &appsv1.DaemonSet{} }},
	} {
		t.Run(tc.file, func(t *testing.T) {
			raw, err := os.ReadFile(filepath.Join(manifestDir, tc.file))
			if err != nil {
				t.Fatalf("reading %s failed: %v", tc.file, err)
			}
			if err := k8syaml.UnmarshalStrict(raw, tc.into()); err != nil {
				t.Fatalf("%s does not decode into the object the cluster would build: %v", tc.file, err)
			}
		})
	}

	// The control. Without it, every case above would also pass against a
	// decoder that was not strict at all — which is the state the rest of this
	// file's `decodeManifest` is in, deliberately, since it only reads fields it
	// names.
	misspelled := []byte("apiVersion: apps/v1\nkind: Deployment\nspec:\n  terminationGracePeriod: 60\n")
	if err := k8syaml.UnmarshalStrict(misspelled, &appsv1.Deployment{}); err == nil {
		t.Fatal("the strict decoder accepted a field that does not exist; the checks above are " +
			"measuring nothing")
	}
}

// 🔴 Both ends of the node gRPC hop, read out of two files.
//
// The api half dials a machine by substituting its own AENV_NODE_SERVICE_PORT
// into the address the scheduler gave it — the Pod's own IP, from the
// agentenv-nodes EndpointSlice. Nothing in between resolves or corrects that
// number: no Service, no DNS name. If it disagrees with what the node listens
// on, every call the api half makes is a connection refused, on every machine,
// starting the moment 3a is switched on.
func TestTheApiHalfDialsThePortTheNodeListensOn(t *testing.T) {
	nodeContainer := onlyContainer(t, "the node DaemonSet", nodeDaemonSet(t).Spec.Template.Spec.Containers)
	apiContainer := onlyContainer(t, "the api Deployment", apiDeployment(t).Spec.Template.Spec.Containers)

	listen, ok := envValue(nodeContainer, "AENV_NODE_SERVICE_ADDR")
	if !ok {
		t.Fatal("the node DaemonSet does not say where its node service listens, so the port the " +
			"api half dials has nothing to be checked against")
	}
	listens := listenPort(t, listen.Value)

	dial, ok := envValue(apiContainer, "AENV_NODE_SERVICE_PORT")
	if !ok {
		t.Fatal("the api Deployment does not set AENV_NODE_SERVICE_PORT; it would take the code " +
			"default, and nothing here could tell whether that matched the node's listener")
	}
	dials, err := strconv.Atoi(strings.TrimSpace(dial.Value))
	if err != nil {
		t.Fatalf("the api Deployment's AENV_NODE_SERVICE_PORT is not a number: %q", dial.Value)
	}

	if int32(dials) != listens {
		t.Fatalf("the api half dials %d and the node listens on %d", dials, listens)
	}

	// ...and the node declares it as a container port, so the listener is
	// visible to somebody reading the workload rather than only to somebody
	// reading its environment.
	declared := false
	for _, port := range nodeContainer.Ports {
		if port.ContainerPort == listens {
			declared = true
		}
	}
	if !declared {
		t.Fatalf("the node DaemonSet listens on %d and declares ports %v", listens, nodeContainer.Ports)
	}
}

// 🔴 The envd access-token seed is the one value in this tree whose absence has
// to stop a Pod from starting.
//
// Tokens are HMAC(seed, sandbox_id). A replica with no seed configured invents a
// node-local one, and the two api replicas then disagree about what every
// sandbox's token is: the user is handed one by whichever replica the load
// balancer picked, and it stops working the moment the other answers — no error,
// no log, no metric (`_sd-impl-phase3-role.md` §9.2). `--role api` refuses to
// start without it (`src/sandbox/access.rs`), and `optional: false` is what
// makes the Pod stop before the process even gets to say so.
//
// The DaemonSet's `optional: true` is this test's control, and it is not a
// weaker version of the same assertion: a node's managed seed is node-local
// state and always has been, so a single-machine deployment with no Secret at
// all still boots. Read through the same decoder in the same test, "the api half
// requires it" means something exactly because "the node half does not" is
// checked beside it — and both must name the same Secret and the same key, or
// one value does not cover the cluster.
func TestOnlyTheApiHalfRefusesToStartWithoutTheSharedAccessTokenSeed(t *testing.T) {
	const (
		envName    = "AENV_SANDBOX_ACCESS_TOKEN_HASH_SEED"
		secretName = "agentenv-runtime-secrets"
		secretKey  = "sandbox-access-token-hash-seed"
	)

	apiContainer := onlyContainer(t, "the api Deployment", apiDeployment(t).Spec.Template.Spec.Containers)
	nodeContainer := onlyContainer(t, "the node DaemonSet", nodeDaemonSet(t).Spec.Template.Spec.Containers)

	apiRef := secretKeyRef(t, "the api Deployment", apiContainer, envName)
	nodeRef := secretKeyRef(t, "the node DaemonSet", nodeContainer, envName)

	for _, tc := range []struct {
		where string
		ref   *corev1.SecretKeySelector
	}{
		{"the api Deployment", apiRef},
		{"the node DaemonSet", nodeRef},
	} {
		if tc.ref.Name != secretName || tc.ref.Key != secretKey {
			t.Fatalf("%s reads %s from %s/%s, want %s/%s; the two halves must read one value or "+
				"they derive different tokens from different seeds",
				tc.where, envName, tc.ref.Name, tc.ref.Key, secretName, secretKey)
		}
	}

	if apiRef.Optional == nil || *apiRef.Optional {
		t.Fatalf("the api Deployment reads %s with optional=%s; a missing key would let the replica "+
			"invent a seed its sibling cannot derive, and that fault is invisible until a user's "+
			"token stops working", envName, describeOptional(apiRef.Optional))
	}
	if nodeRef.Optional == nil || !*nodeRef.Optional {
		t.Fatalf("the node DaemonSet reads %s with optional=%s; that is this test's control, and "+
			"without it a cluster that keeps every seed node-local no longer starts", envName,
			describeOptional(nodeRef.Optional))
	}
}

func describeOptional(optional *bool) string {
	if optional == nil {
		return "unset"
	}
	return strconv.FormatBool(*optional)
}

func secretKeyRef(t *testing.T, where string, container corev1.Container, name string) *corev1.SecretKeySelector {
	t.Helper()
	entry, ok := envValue(container, name)
	if !ok {
		t.Fatalf("%s does not set %s at all", where, name)
	}
	if entry.ValueFrom == nil || entry.ValueFrom.SecretKeyRef == nil {
		t.Fatalf("%s sets %s to a literal (%q) rather than reading it from a Secret", where, name, entry.Value)
	}
	return entry.ValueFrom.SecretKeyRef
}

// The claim the api Deployment's own comment makes — "the one place in the tree
// where a missing key must stop a Pod from starting" — as an assertion.
//
// It is a scan for an absence everywhere but one file, so it carries its control
// with it twice: the walk has to find the one `optional: false` it expects, and
// it has to find `optional: true` somewhere as well. Without the second, a walk
// that read no manifests at all would report the same "nothing else requires a
// Secret" this test is meant to establish.
func TestNothingElseInTheTreeMakesASecretMandatory(t *testing.T) {
	entries, err := os.ReadDir(manifestDir)
	if err != nil {
		t.Fatalf("reading the base layer failed: %v", err)
	}

	required := map[string]int{}
	optionalSeen := 0
	scanned := 0
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".yaml") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(manifestDir, entry.Name()))
		if err != nil {
			t.Fatalf("reading %s failed: %v", entry.Name(), err)
		}
		scanned++
		// Prose explaining why a reference is or is not optional is not a
		// reference; only a line that sets the field counts.
		for _, line := range strings.Split(string(raw), "\n") {
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "#") {
				continue
			}
			switch trimmed {
			case "optional: false":
				required[entry.Name()]++
			case "optional: true":
				optionalSeen++
			}
		}
	}

	if scanned < 5 {
		t.Fatalf("only %d manifests were scanned; this test is reading the wrong directory", scanned)
	}
	if optionalSeen == 0 {
		t.Fatal("the scan found no `optional: true` either, so finding one `optional: false` says " +
			"nothing about what the manifests declare")
	}

	want := map[string]int{"agentenv-api-deployment.yaml": 1}
	if len(required) != len(want) || required["agentenv-api-deployment.yaml"] != want["agentenv-api-deployment.yaml"] {
		t.Fatalf("mandatory Secret/ConfigMap references are %v, want %v; a second one means some "+
			"other workload now refuses to start on a missing key, and whoever added it should say "+
			"so here", required, want)
	}
}
