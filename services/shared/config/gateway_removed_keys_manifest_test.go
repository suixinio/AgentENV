package config

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	k8syaml "k8s.io/apimachinery/pkg/util/yaml"
)

// The gateway's rollback window is closed. GATEWAY_REST_UPSTREAM_ADDR,
// GATEWAY_COLD_LOOKUP_TIMEOUT and GATEWAY_ROUTING_PROJECTION_AUTHORITATIVE are
// read by no build, and the loader ignores them (rollback_window_test.go), so
// a manifest that still declares one comes up healthy while describing a
// gateway that does not exist. Nothing else in the tree fails when one
// reappears: this walk is the whole guard.
//
// 🔴 It anchors on structure, never on a substring. A declaration is an entry
// under a workload's `env:`, a configMapGenerator `literals:` item, a
// ConfigMap `data:` key or a compose `environment:` key, as the YAML parser
// sees them. A comment or a value that merely names the variable is not one,
// and TestTheRemovedGatewayKeyWalkIgnoresMentions holds the walk to that.

var removedGatewayEnvs = []string{
	"GATEWAY_REST_UPSTREAM_ADDR",
	"GATEWAY_COLD_LOOKUP_TIMEOUT",
	"GATEWAY_ROUTING_PROJECTION_AUTHORITATIVE",
}

const composeManifest = "../../../deploy/docker-compose.yml"

// envDeclarations lists, as "<syntax>: <VAR>", every structural declaration
// of one of names in a YAML file: workload env entries, configMapGenerator
// literals, ConfigMap data keys and compose environment keys.
func envDeclarations(t *testing.T, path string, raw []byte, names []string) []string {
	t.Helper()
	wanted := map[string]bool{}
	for _, name := range names {
		wanted[name] = true
	}
	var found []string
	decoder := k8syaml.NewYAMLOrJSONDecoder(bytes.NewReader(raw), 4096)
	for {
		var doc any
		if err := decoder.Decode(&doc); err == io.EOF {
			break
		} else if err != nil {
			t.Fatalf("decoding %s failed: %v", path, err)
		}
		found = append(found, declarationsUnder("", doc, wanted)...)
	}
	sort.Strings(found)
	return found
}

func declarationsUnder(parent string, node any, wanted map[string]bool) []string {
	var found []string
	switch value := node.(type) {
	case map[string]any:
		if parent == "data" || parent == "environment" {
			for key := range value {
				if wanted[key] {
					found = append(found, parent+": "+key)
				}
			}
		}
		for key, child := range value {
			found = append(found, declarationsUnder(key, child, wanted)...)
		}
	case []any:
		for _, item := range value {
			switch parent {
			case "env":
				if entry, ok := item.(map[string]any); ok {
					if name, ok := entry["name"].(string); ok && wanted[name] {
						found = append(found, "env: "+name)
					}
				}
			case "literals", "environment":
				if literal, ok := item.(string); ok {
					if name, _, _ := strings.Cut(literal, "="); wanted[name] {
						found = append(found, parent+": "+name)
					}
				}
			}
			found = append(found, declarationsUnder(parent, item, wanted)...)
		}
	}
	return found
}

func deployManifests(t *testing.T) []string {
	t.Helper()
	entries, err := os.ReadDir(manifestDir)
	if err != nil {
		t.Fatalf("reading the base layer failed: %v", err)
	}
	paths := []string{composeManifest}
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".yaml") {
			paths = append(paths, filepath.Join(manifestDir, entry.Name()))
		}
	}
	return paths
}

func TestNoManifestDeclaresARemovedGatewayKey(t *testing.T) {
	paths := deployManifests(t)
	if len(paths) < 6 {
		t.Fatalf("only %d manifests to scan; this test is reading the wrong directory", len(paths))
	}
	// 🔴 The control comes first: an absence assertion passes on a tree the
	// walk cannot read, so the same walk must find, in the same files, a
	// variable the gateway still reads — once as an env entry on its
	// Deployment and once as a generator literal.
	live := map[string]int{}
	for _, path := range paths {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("reading %s failed: %v", path, err)
		}
		for _, hit := range envDeclarations(t, path, raw, []string{"GATEWAY_ROUTING_PROJECTION_READ"}) {
			live[hit]++
		}
	}
	if live["env: GATEWAY_ROUTING_PROJECTION_READ"] == 0 || live["literals: GATEWAY_ROUTING_PROJECTION_READ"] == 0 {
		t.Fatalf("the walk found %v for GATEWAY_ROUTING_PROJECTION_READ, want an env entry and a "+
			"generator literal; the absence assertion below would prove nothing", live)
	}

	for _, path := range paths {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("reading %s failed: %v", path, err)
		}
		if found := envDeclarations(t, path, raw, removedGatewayEnvs); len(found) > 0 {
			t.Fatalf("%s still declares %v. No gateway build reads these and the loader ignores "+
				"them, so the declaration describes a gateway that does not exist; delete it",
				path, found)
		}
	}
}

// Every syntax the guard anchors on, one declaration each, and the count says
// none of them is invisible to the walk.
const removedKeyDeclarations = `
apiVersion: apps/v1
kind: Deployment
metadata:
  name: fixture
spec:
  template:
    spec:
      containers:
        - name: gateway
          env:
            - name: GATEWAY_REST_UPSTREAM_ADDR
              value: http://agentenv-api:8000
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: fixture
data:
  GATEWAY_COLD_LOOKUP_TIMEOUT: "3s"
---
configMapGenerator:
  - name: fixture
    literals:
      - GATEWAY_ROUTING_PROJECTION_AUTHORITATIVE=on
---
services:
  gateway:
    environment:
      GATEWAY_REST_UPSTREAM_ADDR: http://agentenv-api:8000
  other:
    environment:
      - GATEWAY_COLD_LOOKUP_TIMEOUT=3s
`

func TestTheRemovedGatewayKeyWalkFindsEverySyntax(t *testing.T) {
	found := envDeclarations(t, "fixture", []byte(removedKeyDeclarations), removedGatewayEnvs)
	want := []string{
		"data: GATEWAY_COLD_LOOKUP_TIMEOUT",
		"env: GATEWAY_REST_UPSTREAM_ADDR",
		"environment: GATEWAY_COLD_LOOKUP_TIMEOUT",
		"environment: GATEWAY_REST_UPSTREAM_ADDR",
		"literals: GATEWAY_ROUTING_PROJECTION_AUTHORITATIVE",
	}
	if strings.Join(found, "\n") != strings.Join(want, "\n") {
		t.Fatalf("the walk found %v, want %v", found, want)
	}
}

// The same names, in every place that is not a declaration: comments in each
// of the four syntaxes, a value, a data key's value. None of it counts, and
// the live variable beside them proves the fixture was parsed rather than
// skipped.
const removedKeyMentions = `
apiVersion: apps/v1
kind: Deployment
metadata:
  name: fixture
spec:
  template:
    spec:
      containers:
        - name: gateway
          # - name: GATEWAY_REST_UPSTREAM_ADDR
          env:
            # - name: GATEWAY_COLD_LOOKUP_TIMEOUT
            - name: GATEWAY_ROUTING_PROJECTION_READ
              value: "GATEWAY_ROUTING_PROJECTION_AUTHORITATIVE is gone"
---
configMapGenerator:
  - name: fixture
    literals:
      # - GATEWAY_REST_UPSTREAM_ADDR=http://agentenv-api:8000
      - GATEWAY_ROUTING_PROJECTION_READ=on
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: fixture
data:
  # GATEWAY_COLD_LOOKUP_TIMEOUT: "3s"
  NOTE: GATEWAY_COLD_LOOKUP_TIMEOUT
---
services:
  gateway:
    environment:
      # GATEWAY_REST_UPSTREAM_ADDR: http://agentenv-api:8000
      GATEWAY_SCHEDULER_ADDR: GATEWAY_REST_UPSTREAM_ADDR
`

func TestTheRemovedGatewayKeyWalkIgnoresMentions(t *testing.T) {
	if found := envDeclarations(t, "fixture", []byte(removedKeyMentions), removedGatewayEnvs); len(found) > 0 {
		t.Fatalf("a mention was counted as a declaration: %v", found)
	}
	live := envDeclarations(t, "fixture", []byte(removedKeyMentions), []string{"GATEWAY_ROUTING_PROJECTION_READ"})
	if len(live) != 2 {
		t.Fatalf("the fixture was not read: the live variable was found %d times, want 2 (%v)", len(live), live)
	}
}
