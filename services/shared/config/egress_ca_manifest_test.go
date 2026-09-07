package config

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
)

// The signing half of the egress CA, and the one workload that may hold it.
//
// Brokers sign leaves with the root the guests trust — one layer, no
// intermediate — so `ca.key` has to reach the broker DaemonSet and nothing
// else. A key that also reached the api half would be a second copy of the
// material with no caller, and a node that mounted it would put a signing key
// on the machine the sandboxes run on.
const (
	egressCaSecret    = "egress-ca"
	egressCaMountPath = "/etc/aenv-egress/ca"
	egressCaConfigRel = "config/aenv-egress.toml"
)

var egressCaConfigPath = regexp.MustCompile(`(?m)^(cert_path|key_path)\s*=\s*"([^"]+)"`)

func TestOnlyTheBrokerHoldsTheEgressSigningKey(t *testing.T) {
	var broker appsv1.DaemonSet
	decodeManifest(t, filepath.Join(manifestDir, egressDaemonSetFile), &broker)

	volume, ok := secretVolume(broker.Spec.Template.Spec.Volumes, egressCaSecret)
	if !ok {
		t.Fatalf("%s mounts no %q Secret; the broker signs every leaf with the root in it "+
			"and refuses to start without both files", egressDaemonSetFile, egressCaSecret)
	}
	if len(volume.Items) != 0 {
		t.Fatalf("%s projects only %v out of %q; the broker needs ca.key as well as ca.crt",
			egressDaemonSetFile, volume.Items, egressCaSecret)
	}
	// 🔴 `fsGroup` gives the files to root and the Pod's group, and the broker
	// runs as neither root nor the owner. 0400 would leave `ca.key` unreadable
	// by the one process that has to read it, and the failure surfaces only
	// when a Pod starts on a cluster.
	if volume.DefaultMode == nil {
		t.Fatalf("%s mounts %q with no defaultMode; kubelet's own default does not grant the "+
			"group the read the broker needs", egressDaemonSetFile, egressCaSecret)
	}
	if *volume.DefaultMode&0o040 == 0 {
		t.Fatalf("%s mounts %q with mode %04o; the broker reads ca.key through the Pod's group",
			egressDaemonSetFile, egressCaSecret, *volume.DefaultMode)
	}
}

func TestTheApiHalfMountsNoEgressCa(t *testing.T) {
	var api appsv1.Deployment
	decodeManifest(t, filepath.Join(manifestDir, "agentenv-api-deployment.yaml"), &api)

	if _, ok := secretVolume(api.Spec.Template.Spec.Volumes, egressCaSecret); ok {
		t.Fatalf("agentenv-api-deployment.yaml mounts %q; this half issues no certificates "+
			"and holding the key would be a copy with no caller", egressCaSecret)
	}
}

// A node hands its guests the root to trust and signs nothing itself, so it
// takes the certificate half and leaves the key behind.
func TestTheNodeTakesTheCertificateAndNotTheKey(t *testing.T) {
	var node appsv1.DaemonSet
	decodeManifest(t, filepath.Join(manifestDir, "agentenv-daemonset.yaml"), &node)

	volume, ok := secretVolume(node.Spec.Template.Spec.Volumes, egressCaSecret)
	if !ok {
		t.Fatalf("agentenv-daemonset.yaml mounts no %q; guests would trust no root",
			egressCaSecret)
	}
	if len(volume.Items) == 0 {
		t.Fatalf("agentenv-daemonset.yaml mounts the whole %q Secret; `items` is what keeps "+
			"ca.key off the machine the sandboxes run on", egressCaSecret)
	}
	for _, item := range volume.Items {
		if item.Key != "ca.crt" {
			t.Fatalf("agentenv-daemonset.yaml projects %q out of %q; only ca.crt belongs here",
				item.Key, egressCaSecret)
		}
	}
}

// The broker reads its pair by path, and the path is written in a file the
// manifest above only mounts. Disagree and the broker refuses to start.
func TestTheBrokerConfigNamesThePairItMounts(t *testing.T) {
	var broker appsv1.DaemonSet
	decodeManifest(t, filepath.Join(manifestDir, egressDaemonSetFile), &broker)
	container := brokerContainer(t, broker.Spec.Template.Spec.Containers)

	mounted := ""
	for _, mount := range container.VolumeMounts {
		if mount.Name == egressCaSecret {
			mounted = mount.MountPath
		}
	}
	if mounted != egressCaMountPath {
		t.Fatalf("the broker container mounts %q at %q, want %q",
			egressCaSecret, mounted, egressCaMountPath)
	}

	raw, err := os.ReadFile(filepath.Join(manifestDir, egressCaConfigRel))
	if err != nil {
		t.Fatalf("reading the broker's config failed: %v", err)
	}
	found := map[string]string{}
	for _, match := range egressCaConfigPath.FindAllStringSubmatch(string(raw), -1) {
		found[match[1]] = match[2]
	}
	for _, key := range []string{"cert_path", "key_path"} {
		path, ok := found[key]
		if !ok {
			t.Fatalf("%s declares no [ca].%s; the broker signs with the root it is given "+
				"and has no other source", egressCaConfigRel, key)
		}
		if !strings.HasPrefix(path, egressCaMountPath+"/") {
			t.Fatalf("[ca].%s = %q is not under %q, which is where the Secret lands",
				key, path, egressCaMountPath)
		}
	}
}

func secretVolume(volumes []corev1.Volume, name string) (*corev1.SecretVolumeSource, bool) {
	for _, volume := range volumes {
		if volume.Secret != nil && volume.Secret.SecretName == name {
			return volume.Secret, true
		}
	}
	return nil, false
}

func brokerContainer(t *testing.T, containers []corev1.Container) corev1.Container {
	t.Helper()
	for _, candidate := range containers {
		if candidate.Name == "aenv-egress" {
			return candidate
		}
	}
	t.Fatalf("no `aenv-egress` container on the broker DaemonSet")
	return corev1.Container{}
}
