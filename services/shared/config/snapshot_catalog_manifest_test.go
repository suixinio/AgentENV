package config

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	k8syaml "k8s.io/apimachinery/pkg/util/yaml"
)

// 阶段 2c — the snapshot catalog moving out of object storage and into the
// control plane's PostgreSQL — is finished, and the two environment variables
// that steered it are gone from the code.
//
// 🔴 What this file used to hold: three tests pinning
// AENV_SNAPSHOT_CATALOG_WRITE / _READ onto both workloads, from one shared
// ConfigMap, at the running value. There is nothing left for them to pin —
// `[snapshot.catalog]` declares neither key, `SnapshotCatalogWrite` and
// `SnapshotCatalogRead` do not exist, and object storage is not a catalog.
//
// What replaces them is the inverse assertion, because the risk inverted with
// them: both binaries now REFUSE TO START when either variable is set
// (`refuse_removed_catalog_env_vars`, src/cfg.rs), so a manifest that still
// carries one does not quietly do nothing — it CrashLoops the workload. The
// walk below is the manifest-side half of that guard.

const (
	catalogWriteEnv = "AENV_SNAPSHOT_CATALOG_WRITE"
	catalogReadEnv  = "AENV_SNAPSHOT_CATALOG_READ"
	catalogPathEnv  = "AENV_SNAPSHOT_CATALOG_MIRROR_PATH"
)

// The image prefixes that mean "this workload runs an AgentENV server binary".
// One per half, since the two halves are two crates and two images.
var agentenvImagePrefixes = []string{"agentenv-runtime:", "agentenv-api:"}

// 🔴 An "all of them" assertion, and "all of them" over an empty set is true —
// which is the shape this test has to defend against, because the thing it
// checks for is *absence*. A walk that decoded nothing would report the same
// clean pass as a compliant tree. So it states what it must find before it is
// allowed to conclude anything: manifests, pod-bearing workloads, both AgentENV
// workloads among them, and — separately — the Deployment that lives in
// redis.yaml behind three other documents. That last one is not padding: the
// obvious way to write this walk is with the decodeManifest helper the rest of
// this package uses, which unmarshals a single document and silently returns
// the first one; against a four-document file it finds the
// PersistentVolumeClaim and stops.
func TestNoAgentenvWorkloadDeclaresTheRemovedSnapshotCatalogSwitches(t *testing.T) {
	entries, err := os.ReadDir(manifestDir)
	if err != nil {
		t.Fatalf("reading the base layer failed: %v", err)
	}

	removed := []string{catalogWriteEnv, catalogReadEnv, catalogPathEnv}
	scanned := 0
	workloads := 0
	agentenv := map[string]bool{}
	behindOtherDocuments := 0
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".yaml") || entry.Name() == "kustomization.yaml" {
			continue
		}
		scanned++
		for _, workload := range podWorkloads(t, filepath.Join(manifestDir, entry.Name())) {
			workloads++
			if workload.documentIndex > 0 {
				behindOtherDocuments++
			}
			runsAgentenv := false
			for _, container := range workload.containers {
				for _, prefix := range agentenvImagePrefixes {
					if strings.HasPrefix(container.Image, prefix) {
						runsAgentenv = true
					}
				}
			}
			if !runsAgentenv {
				continue
			}
			agentenv[workload.name] = true
			for _, container := range workload.containers {
				for _, env := range removed {
					if entry, ok := envValue(container, env); ok {
						t.Fatalf("%s still declares %s (%+v); this build removed that setting and "+
							"refuses to start when it is present, so applying this manifest "+
							"CrashLoops the workload. The snapshot catalog is PostgreSQL — "+
							"configure [pg], and delete this variable", workload.name, env, entry)
					}
				}
			}
		}
	}

	if scanned < 5 {
		t.Fatalf("only %d manifests were scanned; this test is reading the wrong directory", scanned)
	}
	if workloads < 4 {
		t.Fatalf("only %d pod-bearing workloads were decoded; the base layer carries the node, the "+
			"api half, the gateway, the scheduler and redis, so this walk is not seeing the tree",
			workloads)
	}
	if len(agentenv) < 2 {
		t.Fatalf("the walk found %d workloads running one of %v (%v); it must find at least "+
			"the node DaemonSet and the api Deployment, or \"none of them declare it\" is a claim "+
			"about nothing", len(agentenv), agentenvImagePrefixes, agentenv)
	}
	if behindOtherDocuments == 0 {
		t.Fatal("every workload the walk decoded was the first document in its file, so nothing " +
			"here shows it reads past one. redis.yaml puts a Deployment behind three other " +
			"objects; a single-document decoder would find the PersistentVolumeClaim, report " +
			"plausible counts, and quietly stop checking any file shaped like that")
	}
}

// 🔴 The mutation control for the walk above. A scan for absence passes on a
// tree it cannot read, so this proves the same walk *does* find an environment
// variable that is genuinely there — AENV_PAUSED_REGISTRY_BACKEND, which both
// halves still declare — using the same decode, the same population and the
// same lookup.
func TestTheRemovedSwitchWalkCanStillFindAnEnvironmentVariable(t *testing.T) {
	const stillDeclared = "AENV_PAUSED_REGISTRY_BACKEND"

	found := 0
	entries, err := os.ReadDir(manifestDir)
	if err != nil {
		t.Fatalf("reading the base layer failed: %v", err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".yaml") || entry.Name() == "kustomization.yaml" {
			continue
		}
		for _, workload := range podWorkloads(t, filepath.Join(manifestDir, entry.Name())) {
			for _, container := range workload.containers {
				runsAgentenv := false
				for _, prefix := range agentenvImagePrefixes {
					if strings.HasPrefix(container.Image, prefix) {
						runsAgentenv = true
					}
				}
				if !runsAgentenv {
					continue
				}
				if _, ok := envValue(container, stillDeclared); ok {
					found++
				}
			}
		}
	}

	if found < 2 {
		t.Fatalf("the walk found %s on %d AgentENV containers, want both halves; the absence "+
			"assertion in this file therefore proves nothing — it would pass on a tree this "+
			"walk cannot read", stillDeclared, found)
	}
}

type podWorkload struct {
	name string
	// Where in its file the workload was found. Zero for the single-object
	// manifests; non-zero only for one that a single-document decoder would
	// never have reached.
	documentIndex int
	containers    []corev1.Container
}

// podWorkloads decodes every Deployment and DaemonSet in a manifest file.
// Document-by-document because redis.yaml is four objects in one file, and a
// decoder that stopped at the first would quietly shrink this test's population.
func podWorkloads(t *testing.T, path string) []podWorkload {
	t.Helper()

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s failed: %v", path, err)
	}

	var found []podWorkload
	decoder := k8syaml.NewYAMLOrJSONDecoder(bytes.NewReader(raw), 4096)
	for index := 0; ; index++ {
		var doc struct {
			Kind     string `json:"kind"`
			Metadata struct {
				Name string `json:"name"`
			} `json:"metadata"`
			Spec struct {
				Template struct {
					Spec struct {
						Containers []corev1.Container `json:"containers"`
					} `json:"spec"`
				} `json:"template"`
			} `json:"spec"`
		}
		if err := decoder.Decode(&doc); err == io.EOF {
			break
		} else if err != nil {
			t.Fatalf("decoding %s failed: %v", path, err)
		}
		if doc.Kind != "Deployment" && doc.Kind != "DaemonSet" {
			continue
		}
		found = append(found, podWorkload{
			name:          doc.Metadata.Name + " (" + filepath.Base(path) + ")",
			documentIndex: index,
			containers:    doc.Spec.Template.Spec.Containers,
		})
	}
	return found
}

func configMapKeyRefFor(t *testing.T, where string, container corev1.Container, name string) *corev1.ConfigMapKeySelector {
	t.Helper()

	entry, ok := envValue(container, name)
	if !ok {
		t.Fatalf("%s does not set %s at all; unset is the object_store default, and this cluster "+
			"has not been on it since 2c", where, name)
	}
	if entry.ValueFrom == nil || entry.ValueFrom.ConfigMapKeyRef == nil {
		t.Fatalf("%s sets %s to a literal (%q) rather than reading it from the shared ConfigMap; "+
			"a literal here is how these two ended up living only on the live objects",
			where, name, entry.Value)
	}
	return entry.ValueFrom.ConfigMapKeyRef
}
