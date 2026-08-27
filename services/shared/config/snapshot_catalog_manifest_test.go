package config

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode"

	corev1 "k8s.io/api/core/v1"
	k8syaml "k8s.io/apimachinery/pkg/util/yaml"
)

// 阶段 2c — the snapshot catalog moving out of object storage and into the
// control plane's PostgreSQL — as manifest assertions.
//
// 🔴 The two keys below ran in production for the whole of 2c without being in
// this repository: they were put on the live DaemonSet with `kubectl set env`,
// and the api Deployment got the same treatment when 3a went out. An
// environment variable set that way is a literal on the live object and nothing
// else, so the next `apply -k` removed it. Removing it does not fail: the
// process falls back to `[snapshot.catalog]`'s object_store/object_store
// defaults in src/cfg.rs and serves reads out of object storage while the
// scheduler goes on treating PostgreSQL as authoritative. Two catalogs, one
// cluster, and the disagreement surfaces as snapshots that "do not exist" —
// which callers act on by deleting artifacts and refusing resumes.
//
// So these tests pin three separate things, because three separate mistakes
// produce that outcome: the keys being absent, the keys carrying the *default*
// value rather than the running one, and the two halves reading them from
// places that can be edited apart.

const (
	catalogWriteEnv  = "AENV_SNAPSHOT_CATALOG_WRITE"
	catalogReadEnv   = "AENV_SNAPSHOT_CATALOG_READ"
	catalogConfigMap = "snapshot-catalog-config"
)

// 🔴 Both halves answer snapshot lookups, so both must answer them out of the
// same catalog — and the only way to guarantee that is for both to read one
// value rather than two values somebody keeps in step by hand.
//
// The control is the env var immediately above these two in both files.
// AENV_PAUSED_REGISTRY_BACKEND is a key the two halves deliberately source
// *differently* — the node reads a ConfigMap and may legitimately fall back to
// `local`, the api half hard-codes `central` because a node-local paused
// registry there would be a registry only one replica can see. Read through the
// same helpers in the same test, "these two agree" means something exactly
// because "that one does not" is checked beside it: a test that read one file
// twice, or a decoder that flattened both into zero values, would report
// agreement everywhere and would fail here.
func TestBothHalvesReadTheSnapshotCatalogFromOneSwitch(t *testing.T) {
	apiContainer := onlyContainer(t, "the api Deployment", apiDeployment(t).Spec.Template.Spec.Containers)
	nodeContainer := onlyContainer(t, "the node DaemonSet", nodeDaemonSet(t).Spec.Template.Spec.Containers)

	for _, env := range []string{catalogWriteEnv, catalogReadEnv} {
		apiRef := configMapKeyRefFor(t, "the api Deployment", apiContainer, env)
		nodeRef := configMapKeyRefFor(t, "the node DaemonSet", nodeContainer, env)

		if apiRef.Name != nodeRef.Name || apiRef.Key != nodeRef.Key {
			t.Fatalf("the two halves read %s from different places: api %s/%s, node %s/%s — two "+
				"places is two values to keep in step, and a cluster whose halves read different "+
				"catalogs reports nothing, it just answers \"no such snapshot\" on one side",
				env, apiRef.Name, apiRef.Key, nodeRef.Name, nodeRef.Key)
		}
		if apiRef.Name != catalogConfigMap || apiRef.Key != env {
			t.Fatalf("%s is read from %s/%s, want %s/%s", env, apiRef.Name, apiRef.Key, catalogConfigMap, env)
		}
		// Both optional, and together. Losing the whole ConfigMap drops the pair
		// back to object_store/object_store, which is legal and merely old;
		// losing only the write key leaves write=object_store with
		// read=postgres, which `AppConfig::validate_snapshot_catalog` refuses at
		// startup. Neither half of that is quiet. `optional: false` would be a
		// second mandatory reference in a tree that pins itself at one.
		if apiRef.Optional == nil || !*apiRef.Optional {
			t.Fatalf("the api Deployment reads %s with optional=%s, want true", env, describeOptional(apiRef.Optional))
		}
		if nodeRef.Optional == nil || !*nodeRef.Optional {
			t.Fatalf("the node DaemonSet reads %s with optional=%s, want true", env, describeOptional(nodeRef.Optional))
		}
	}

	// The control. If this ever starts agreeing, the test above has stopped
	// distinguishing "both halves read one value" from "this test cannot tell
	// the two files apart".
	const pausedRegistry = "AENV_PAUSED_REGISTRY_BACKEND"
	apiPaused, ok := envValue(apiContainer, pausedRegistry)
	if !ok {
		t.Fatalf("the api Deployment no longer sets %s; this test's control is gone", pausedRegistry)
	}
	if apiPaused.ValueFrom != nil {
		t.Fatalf("the api Deployment now reads %s from a reference (%+v) rather than writing it out; "+
			"that may well be right, but it was this test's proof that it can tell a shared switch "+
			"from a per-half one, and something else has to be", pausedRegistry, apiPaused.ValueFrom)
	}
	nodePaused, ok := envValue(nodeContainer, pausedRegistry)
	if !ok {
		t.Fatalf("the node DaemonSet no longer sets %s; this test's control is gone", pausedRegistry)
	}
	if nodePaused.ValueFrom == nil || nodePaused.ValueFrom.ConfigMapKeyRef == nil {
		t.Fatalf("the node DaemonSet now writes %s out as a literal (%q) rather than reading it; "+
			"same as above — the control this test leans on is gone", pausedRegistry, nodePaused.Value)
	}
}

// 🔴 The values, and the reason the values are the point.
//
// It would be easy to write a test that says "the ConfigMap carries these two
// keys" and have it pass on a ConfigMap that shipped them `off`. That version
// would be worse than no test: it would certify, on every run, the exact state
// this whole change exists to prevent — a manifest whose apply silently returns
// the cluster to a catalog nobody has been writing to as the sole authority.
//
// So the assertion is the running value, and it carries its own control: each
// value is also checked *against the code default it overrides*. That is what
// makes the pin load-bearing rather than decorative. If somebody "simplifies"
// this ConfigMap to the defaults, the equality below fails; if somebody changes
// the defaults in src/cfg.rs to match the ConfigMap, the inequality below fails
// and says so — because at that moment these manifest lines stop being what
// keeps the cluster on PostgreSQL, and whoever moved the default should be the
// one to decide what happens to them.
func TestTheSnapshotCatalogShipsTheStateTheClusterRuns(t *testing.T) {
	for _, tc := range []struct {
		env  string
		want string
		enum string
	}{
		// 2c's accepted end state: both catalogs written, the central one
		// answering reads. `write` stays at `both` and not `postgres` — that is
		// what keeps object storage a complete copy, and the rollback a config
		// change rather than a backfill.
		{env: catalogWriteEnv, want: "both", enum: "SnapshotCatalogWrite"},
		{env: catalogReadEnv, want: "postgres", enum: "SnapshotCatalogRead"},
	} {
		t.Run(tc.env, func(t *testing.T) {
			got := generatedLiteral(t, catalogConfigMap, tc.env)
			if got != tc.want {
				t.Fatalf("%s/%s is %q, want %q; 2c's read side has been served from PostgreSQL "+
					"since it was accepted, and a manifest that says otherwise makes the next "+
					"`apply -k` a silent rollback to object storage",
					catalogConfigMap, tc.env, got, tc.want)
			}

			// Control one: the value is not the code default. If it were, these
			// manifest lines would be doing nothing and their absence would be
			// harmless — which is precisely the belief that left them out of
			// the repository in the first place.
			fallback := rustConfigDefault(t, tc.env)
			if got == fallback {
				t.Fatalf("%s/%s is %q, which is also what src/cfg.rs defaults to. Either the "+
					"ConfigMap has been reset to the default — an apply would then hand the "+
					"cluster back to object storage — or the default has moved, in which case "+
					"decide deliberately whether this ConfigMap is still the thing holding the "+
					"cluster on PostgreSQL", catalogConfigMap, tc.env, got)
			}

			// Control two: the value is a spelling the process accepts. A typo
			// here does not fall back to the default, it fails the config load
			// on every Pod in the fleet at once — loud, but loud on a Sunday.
			accepted := rustEnumValues(t, tc.enum)
			if len(accepted) < 2 {
				t.Fatalf("src/cfg.rs's %s yielded %v; this test cannot judge a value domain it "+
					"could not read", tc.enum, accepted)
			}
			found := false
			for _, candidate := range accepted {
				if candidate == got {
					found = true
				}
			}
			if !found {
				t.Fatalf("%s/%s is %q, which %s does not accept (%v)",
					catalogConfigMap, tc.env, got, tc.enum, accepted)
			}
		})
	}
}

// The claim as a scan: every workload in the base layer that runs an AgentENV
// binary declares both switches. Two do today, and the number matters — this is
// an "all of them" assertion, and "all of them" over an empty set is true. A
// walk that decoded nothing, or that decoded workloads with no containers in
// them, would report the same clean pass as a compliant tree.
//
// So the walk states what it must find before it is allowed to conclude
// anything: manifests, pod-bearing workloads, both AgentENV workloads among
// them, and — separately — the Deployment that lives in redis.yaml behind three
// other documents. That last one is not padding. The obvious way to write this
// walk is with the decodeManifest helper the rest of this package uses, which
// unmarshals a single document and silently returns the first one; against a
// four-document file it finds the PersistentVolumeClaim and stops. Nothing about
// that reads as a bug — the counts still look plausible — and it is exactly the
// error that would let a future AgentENV workload sharing a file with something
// else slip past this test.
// The image prefixes that mean "this workload runs an AgentENV server binary".
// One per half, since the two halves are two crates and two images.
var agentenvImagePrefixes = []string{"agentenv-runtime:", "agentenv-api:"}

func TestEveryAgentenvWorkloadDeclaresTheSnapshotCatalogSwitches(t *testing.T) {
	entries, err := os.ReadDir(manifestDir)
	if err != nil {
		t.Fatalf("reading the base layer failed: %v", err)
	}

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
			// 🔴 Two prefixes, not one. The crate split gave the api half its
			// own binary and its own image (`agentenv-api`, built from
			// `crates/aenv-api`) while the node half kept `agentenv-runtime`;
			// a scan that still named only the runtime image found one
			// workload where the assertion below needs two, and said so —
			// which is the whole reason the count is asserted.
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
				for _, env := range []string{catalogWriteEnv, catalogReadEnv} {
					entry, ok := envValue(container, env)
					if !ok {
						t.Fatalf("%s runs the AgentENV binary and does not declare %s; on the next "+
							"apply it falls back to object storage while the rest of the cluster "+
							"reads PostgreSQL", workload.name, env)
					}
					if entry.ValueFrom == nil || entry.ValueFrom.ConfigMapKeyRef == nil ||
						entry.ValueFrom.ConfigMapKeyRef.Name != catalogConfigMap {
						t.Fatalf("%s declares %s but not from %s (%+v); a second source is a second "+
							"value", workload.name, env, catalogConfigMap, entry)
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
			"the node DaemonSet and the api Deployment, or \"all of them declare it\" is a claim "+
			"about nothing", len(agentenv), agentenvImagePrefixes, agentenv)
	}
	if behindOtherDocuments == 0 {
		t.Fatal("every workload the walk decoded was the first document in its file, so nothing " +
			"here shows it reads past one. redis.yaml puts a Deployment behind three other " +
			"objects; a single-document decoder would find the PersistentVolumeClaim, report " +
			"plausible counts, and quietly stop checking any file shaped like that")
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

// rustConfigDefault reads the `default = "…"` off the `#[config(…)]` attribute
// that binds an environment variable in src/cfg.rs. Scanned rather than parsed,
// the way nodeIdentityClusterID scans the TOML: this package has no business
// carrying a Rust parser, and the one line it needs is a single attribute.
func rustConfigDefault(t *testing.T, env string) string {
	t.Helper()

	for _, line := range strings.Split(readRustConfig(t), "\n") {
		if !strings.Contains(line, `env = "`+env+`"`) {
			continue
		}
		_, after, found := strings.Cut(line, `default = "`)
		if !found {
			t.Fatalf("src/cfg.rs binds %s on a line with no inline default (%q); this test can no "+
				"longer tell the shipped value from the fallback it overrides", env, strings.TrimSpace(line))
		}
		value, _, _ := strings.Cut(after, `"`)
		return value
	}
	t.Fatalf("src/cfg.rs binds nothing to %s; either the switch is gone from the code and these "+
		"manifest lines are dead, or this test is reading the wrong file", env)
	return ""
}

// rustEnumValues returns the serde snake_case spellings a `#[serde(rename_all =
// "snake_case")]` enum in src/cfg.rs accepts.
func rustEnumValues(t *testing.T, name string) []string {
	t.Helper()

	_, after, found := strings.Cut(readRustConfig(t), "pub enum "+name+" {")
	if !found {
		t.Fatalf("src/cfg.rs declares no enum %s", name)
	}
	block, _, found := strings.Cut(after, "\n}")
	if !found {
		t.Fatalf("the declaration of %s in src/cfg.rs does not close", name)
	}

	var values []string
	for _, line := range strings.Split(block, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasSuffix(line, ",") {
			continue
		}
		variant := strings.TrimSuffix(line, ",")
		if variant == "" || !isRustIdentifier(variant) {
			continue
		}
		values = append(values, toSnakeCase(variant))
	}
	return values
}

func readRustConfig(t *testing.T) string {
	t.Helper()

	raw, err := os.ReadFile(filepath.Join(manifestDir, "..", "..", "..", "src", "cfg.rs"))
	if err != nil {
		t.Fatalf("reading src/cfg.rs failed: %v", err)
	}
	return string(raw)
}

func isRustIdentifier(s string) bool {
	for i, r := range s {
		if unicode.IsLetter(r) || r == '_' || (i > 0 && unicode.IsDigit(r)) {
			continue
		}
		return false
	}
	return s != ""
}

func toSnakeCase(camel string) string {
	var out strings.Builder
	for i, r := range camel {
		if unicode.IsUpper(r) {
			if i > 0 {
				out.WriteByte('_')
			}
			out.WriteRune(unicode.ToLower(r))
			continue
		}
		out.WriteRune(r)
	}
	return out.String()
}
