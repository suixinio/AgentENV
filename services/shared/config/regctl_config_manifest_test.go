package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

// Which registries `regctl` may reach over plain HTTP, as manifest assertions.
//
// 🔴 The live cluster carried a `regctl-config` ConfigMap, a volume mounting it
// at `/root/.regctl/config.json` on `ds/agentenv-node` and a `HOME=/root` env
// var making `regctl` look there — all three real, all three visible in
// `ds/agentenv-node`'s own `kubectl.kubernetes.io/last-applied-configuration`,
// and none of the three ever committed to this repository. That is not merely
// undocumented: under three-way merge, a field present in `last-applied` and
// live but absent from a new render is exactly what the next `apply -k`
// deletes. `regctl` takes no `--tls-insecure` flag from AgentENV's own code
// (`src/setup/deps.rs`, `src/image/oci_image.rs`) — its only source of truth
// for "this host speaks plain HTTP" is that file — so losing it silently turns
// every manifest fetch, blob/layer download and tools-drive pull against a
// plain-HTTP registry into a TLS handshake failure.
//
// 🔴 `config/regctl.json` in base itself carries an empty `hosts` map: the
// pve-sg cluster this file used to name directly (`10.10.10.204:5000`) was
// decommissioned, and there is no longer a single "the" live cluster's
// registry to hardcode at the base layer. `deploy/k8s/overlays/pve-mf` is
// where the current live cluster's real, plain-HTTP registry
// (`10.1.0.201:5000`) is tracked instead, via its own `regctl-config`
// `configMapGenerator` override (`behavior: replace`). This file has no test
// reading that overlay — it only ever reads `manifestDir` (base) — so it
// checks base ships the do-nothing-safe empty default, not any one cluster's
// value.
const (
	regctlConfigMap   = "regctl-config"
	regctlTrackedFile = "config/regctl.json"
	regctlMountPath   = "/root/.regctl/config.json"
	regctlSubPath     = "config.json"
	regctlHomeEnv     = "HOME"
	regctlHomeValue   = "/root"
)

// 🔴 The generator, and that it is sourced from a tracked file rather than an
// inline literal. `config/regctl.json` carries no credential — a registry
// hostname and a TLS setting, the same shape as `config/oss-overlay.toml` — so
// there is no reason for it to live only on the live object, and every reason
// for it to be reviewable the way `oss-overlay.toml`'s endpoint is.
func TestRegctlConfigIsGeneratedFromTheTrackedFile(t *testing.T) {
	files, ok := generatorFilesIfPresent(t, regctlConfigMap)
	if !ok {
		t.Fatalf("the base layer generates no %s ConfigMap; without it `regctl` finds no config at "+
			"$HOME/.regctl/config.json and defaults to TLS for every registry, including this "+
			"cluster's own plain-HTTP one", regctlConfigMap)
	}

	want := "config.json=" + regctlTrackedFile
	found := false
	for _, file := range files {
		if file == want {
			found = true
		}
	}
	if !found {
		t.Fatalf("the %s generator's files are %v, want an entry %q; the DaemonSet mounts this "+
			"ConfigMap with subPath %q, so the generated key must be named exactly that",
			regctlConfigMap, files, want, regctlSubPath)
	}

	if _, err := os.Stat(filepath.Join(manifestDir, regctlTrackedFile)); err != nil {
		t.Fatalf("the generator names %s but the file is not readable: %v", regctlTrackedFile, err)
	}
}

// 🔴 The value, not just the key's presence — but for base, the value this
// test expects is emptiness. base is a shared layer with no one cluster's
// registry to bake in; the moment it names a real plain-HTTP host again, that
// host silently becomes every overlay's default unless the overlay also
// overrides this generator, which is exactly the kind of drift
// `deploy/k8s/overlays/pve-mf`'s own `behavior: replace` override exists to
// own instead. This test's predecessor pinned a live cluster's registry
// (pve-sg, `10.10.10.204:5000`) directly in base; once that cluster was
// decommissioned, base moved to this empty, do-nothing-safe default and this
// test's job flipped from "assert the real value is here" to "assert base
// never silently regains one."
func TestTheTrackedRegctlConfigShipsNoClusterSpecificRegistry(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(manifestDir, regctlTrackedFile))
	if err != nil {
		t.Fatalf("reading the tracked regctl config failed: %v", err)
	}

	var parsed struct {
		Hosts map[string]struct {
			Hostname string `json:"hostname"`
			TLS      string `json:"tls"`
		} `json:"hosts"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		t.Fatalf("%s is not valid JSON: %v", regctlTrackedFile, err)
	}

	if len(parsed.Hosts) != 0 {
		t.Fatalf("%s carries hosts %v, want none — base ships the do-nothing-safe empty default; "+
			"a cluster-specific plain-HTTP registry belongs in that cluster's own overlay "+
			"(configMapGenerator behavior: replace), not in the shared base layer",
			regctlTrackedFile, parsed.Hosts)
	}
}

// 🔴 Both halves of the fix, checked in one test the way the credential-file
// pair in agentenv-daemonset.yaml is: a volume with no env var pointing
// `regctl` at it is as inert as an env var with no file mounted where it
// points.
func TestTheNodeMountsRegctlConfigWhereRegctlLooksForIt(t *testing.T) {
	node := nodeDaemonSet(t)
	container := onlyContainer(t, "the node DaemonSet", node.Spec.Template.Spec.Containers)

	volume, ok := volumeNamed(node.Spec.Template.Spec.Volumes, regctlConfigMap)
	if !ok {
		t.Fatalf("the node DaemonSet declares no %q volume", regctlConfigMap)
	}
	if volume.ConfigMap == nil || volume.ConfigMap.Name != regctlConfigMap {
		t.Fatalf("the %q volume is not sourced from the %q ConfigMap (%+v)", regctlConfigMap, regctlConfigMap, volume.VolumeSource)
	}

	mount, ok := volumeMountAt(container, regctlMountPath)
	if !ok {
		t.Fatalf("the node container mounts nothing at %s, which is the only place `regctl` (with "+
			"HOME=%s) looks for its config", regctlMountPath, regctlHomeValue)
	}
	if mount.Name != regctlConfigMap {
		t.Fatalf("the mount at %s comes from volume %q, want %q", regctlMountPath, mount.Name, regctlConfigMap)
	}
	if mount.SubPath != regctlSubPath {
		t.Fatalf("the mount at %s has subPath %q, want %q — the generator's key name and this "+
			"subPath have to agree or the mount resolves to a directory, not the file", regctlMountPath, mount.SubPath, regctlSubPath)
	}
	if !mount.ReadOnly {
		t.Fatalf("the mount at %s is not read-only", regctlMountPath)
	}

	home, ok := envValue(container, regctlHomeEnv)
	if !ok {
		t.Fatalf("the node container sets no %s; the image runs as root with no HOME of its own, "+
			"so without this `regctl` cannot resolve $HOME/.regctl/config.json at all", regctlHomeEnv)
	}
	if home.ValueFrom != nil || home.Value != regctlHomeValue {
		t.Fatalf("%s is %+v, want a literal %q — it names a path inside this container image, not "+
			"anything cluster-specific that would belong in a ConfigMap", regctlHomeEnv, home, regctlHomeValue)
	}
}

// 🔴 The control. `aenv-api` never calls `regctl`
// (`crates/aenv-node/src/bin/aenv-node.rs::assemble_api`'s own doc comment: "installs no regctl —
// by design, that is a node's tooling"; `POST /sandboxes-cold` ships the
// unresolved image reference to a node instead of resolving one, and a template
// build dispatches to a node too — neither resolves an image in this process),
// so giving this half the same volume and env var as the node would cost
// nothing and fix nothing. Checked here so that "the node" and "not the api
// half" stay two different, both-verified claims rather than one assumption.
func TestTheApiHalfCarriesNoRegctlConfig(t *testing.T) {
	api := apiDeployment(t)
	container := onlyContainer(t, "the api Deployment", api.Spec.Template.Spec.Containers)

	if _, ok := volumeNamed(api.Spec.Template.Spec.Volumes, regctlConfigMap); ok {
		t.Fatalf("the api Deployment declares a %q volume; aenv-api never calls regctl, so this "+
			"volume unblocks nothing", regctlConfigMap)
	}
	if _, ok := volumeMountAt(container, regctlMountPath); ok {
		t.Fatalf("the api Deployment mounts something at %s; aenv-api never calls regctl", regctlMountPath)
	}
	if _, ok := envValue(container, regctlHomeEnv); ok {
		t.Fatalf("the api Deployment sets %s; it exists only to steer regctl, which this half never calls", regctlHomeEnv)
	}
}

// generatorFilesIfPresent reads the `files` list off one base-layer
// configMapGenerator entry. Mirrors generatorLiteralIfPresent, which reads
// `literals` off the same struct — kept separate because the two are read by
// different tests and a shared decoder that flattened both would let a
// generator that moved from `literals` to `files` (or back) pass either check
// by accident.
func generatorFilesIfPresent(t *testing.T, configMap string) ([]string, bool) {
	t.Helper()

	var kustomization struct {
		ConfigMapGenerator []struct {
			Name  string   `json:"name"`
			Files []string `json:"files"`
		} `json:"configMapGenerator"`
	}
	decodeManifest(t, filepath.Join(manifestDir, "kustomization.yaml"), &kustomization)

	for _, generator := range kustomization.ConfigMapGenerator {
		if generator.Name == configMap {
			return generator.Files, true
		}
	}
	return nil, false
}

func volumeNamed(volumes []corev1.Volume, name string) (corev1.Volume, bool) {
	for _, volume := range volumes {
		if volume.Name == name {
			return volume, true
		}
	}
	return corev1.Volume{}, false
}

// volumeMountAt finds a volumeMount by its container-side path rather than by
// volume name, because one volume (agentenv-config, here regctl-config is its
// own) can legitimately be mounted more than once under different subPaths —
// matching on path is what a wrong subPath on a right-named mount would still
// be caught by.
func volumeMountAt(container corev1.Container, mountPath string) (corev1.VolumeMount, bool) {
	for _, mount := range container.VolumeMounts {
		if mount.MountPath == mountPath {
			return mount, true
		}
	}
	return corev1.VolumeMount{}, false
}
