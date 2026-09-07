package config

import (
	"os"
	"testing"
)

// The egress broker is a per-node DaemonSet reached over a Unix socket. The
// cluster-level transport is gone: no endpoint, no transport CA, no HMAC key,
// no skew window, no replay cache, no server certificate. A node build refuses
// `mode = "remote"` outright, but a manifest that still declares one of these
// variables comes up healthy while describing a broker that does not exist.
//
// It anchors on structure, the same way the gateway walk does: a
// declaration is an entry under a workload's `env:`, a configMapGenerator
// `literals:` item, a ConfigMap `data:` key or a compose `environment:` key.
// A comment naming one is not a declaration.
var removedEgressEnvs = []string{
	"AENV_EGRESS_BROKER_ENDPOINT",
	"AENV_EGRESS_BROKER_CA_CERT_PATH",
	"AENV_EGRESS_BROKER_SHARED_SECRET",
	"AENV_EGRESS_LISTEN",
	"AENV_EGRESS_MAX_SKEW_MS",
	"AENV_EGRESS_REPLAY_CAPACITY",
	"AENV_EGRESS_TLS_CERT_PATH",
	"AENV_EGRESS_TLS_KEY_PATH",
}

func TestNoManifestDeclaresARemovedEgressKey(t *testing.T) {
	paths := deployManifests(t)
	if len(paths) < 6 {
		t.Fatalf("only %d manifests to scan; this test is reading the wrong directory", len(paths))
	}
	// The control comes first: an absence assertion passes on a tree the
	// walk cannot read, so the same walk must find the variable that still
	// selects the broker mode — once as an env entry, once as a literal.
	live := map[string]int{}
	for _, path := range paths {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("reading %s failed: %v", path, err)
		}
		for _, hit := range envDeclarations(t, path, raw, []string{"AENV_EGRESS_BROKER_MODE"}) {
			live[hit]++
		}
	}
	if live["env: AENV_EGRESS_BROKER_MODE"] == 0 || live["literals: AENV_EGRESS_BROKER_MODE"] == 0 {
		t.Fatalf("the walk found %v for AENV_EGRESS_BROKER_MODE, want an env entry and a "+
			"generator literal; the absence assertion below would prove nothing", live)
	}

	for _, path := range paths {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("reading %s failed: %v", path, err)
		}
		if found := envDeclarations(t, path, raw, removedEgressEnvs); len(found) > 0 {
			t.Fatalf("%s still declares %v. The broker is reached over a node-local Unix "+
				"socket and no build reads these, so the declaration describes a broker "+
				"that does not exist; delete it", path, found)
		}
	}
}

// Every syntax the guard anchors on, one declaration each.
const removedEgressKeyDeclarations = `
apiVersion: apps/v1
kind: Deployment
metadata:
  name: fixture
spec:
  template:
    spec:
      containers:
        - name: aenv-node
          env:
            - name: AENV_EGRESS_BROKER_ENDPOINT
              value: aenv-egress:8443
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: fixture
data:
  AENV_EGRESS_MAX_SKEW_MS: "30000"
---
configMapGenerator:
  - name: fixture
    literals:
      - AENV_EGRESS_BROKER_CA_CERT_PATH=/etc/agentenv/egress/ca.crt
---
services:
  aenv-node:
    environment:
      AENV_EGRESS_TLS_CERT_PATH: /etc/aenv-egress/server/tls.crt
  other:
    environment:
      - AENV_EGRESS_BROKER_SHARED_SECRET=hunter2
`

func TestTheRemovedEgressKeyWalkFindsEverySyntax(t *testing.T) {
	found := envDeclarations(t, "fixture", []byte(removedEgressKeyDeclarations), removedEgressEnvs)
	want := []string{
		"data: AENV_EGRESS_MAX_SKEW_MS",
		"env: AENV_EGRESS_BROKER_ENDPOINT",
		"environment: AENV_EGRESS_BROKER_SHARED_SECRET",
		"environment: AENV_EGRESS_TLS_CERT_PATH",
		"literals: AENV_EGRESS_BROKER_CA_CERT_PATH",
	}
	if len(found) != len(want) {
		t.Fatalf("the walk found %v, want %v", found, want)
	}
	for i := range want {
		if found[i] != want[i] {
			t.Fatalf("the walk found %v, want %v", found, want)
		}
	}
}

// The same names, in every place that is not a declaration.
const removedEgressKeyMentions = `
apiVersion: apps/v1
kind: Deployment
metadata:
  name: fixture
spec:
  template:
    spec:
      containers:
        - name: aenv-node
          # - name: AENV_EGRESS_BROKER_ENDPOINT
          env:
            # - name: AENV_EGRESS_BROKER_SHARED_SECRET
            - name: AENV_EGRESS_BROKER_MODE
              value: "AENV_EGRESS_BROKER_ENDPOINT is gone"
---
configMapGenerator:
  - name: fixture
    literals:
      # - AENV_EGRESS_TLS_KEY_PATH=/etc/aenv-egress/server/tls.key
      - AENV_EGRESS_BROKER_MODE=local
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: fixture
data:
  # AENV_EGRESS_MAX_SKEW_MS: "30000"
  NOTE: AENV_EGRESS_REPLAY_CAPACITY
`

func TestTheRemovedEgressKeyWalkIgnoresMentions(t *testing.T) {
	if found := envDeclarations(t, "fixture", []byte(removedEgressKeyMentions), removedEgressEnvs); len(found) > 0 {
		t.Fatalf("a mention was counted as a declaration: %v", found)
	}
	live := envDeclarations(t, "fixture", []byte(removedEgressKeyMentions), []string{"AENV_EGRESS_BROKER_MODE"})
	if len(live) != 2 {
		t.Fatalf("the fixture was not read: the live variable was found %d times, want 2 (%v)", len(live), live)
	}
}
