package config

import (
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
)

// The gid that owns `/run/aenv-egress`, in the three places that have to
// agree on it.
//
// The broker's init container chowns the shared hostPath to this gid; the
// broker itself runs under it and binds a socket there; the node checks the
// directory carries it before it will start in `local` mode. Set any one of
// them apart and nothing fails loudly at apply time — the broker binds and
// the node then refuses to start, on every machine at once.
//
// The Rust default is read out of the confique attribute that declares it, so
// a comment naming the number matches nothing here.
var socketGroupAttr = regexp.MustCompile(
	`(?s)#\[config\(\s*default\s*=\s*(\d+)u32\s*,\s*env\s*=\s*"AENV_EGRESS_BROKER_SOCKET_GROUP"\s*\)\]`,
)

const (
	egressDaemonSetFile   = "aenv-egress-daemonset.yaml"
	socketDirInitName     = "socket-dir"
	socketGroupEnv        = "BROKER_GID"
	egressBrokerConfigRel = "../../../src/cfg/egress_broker.rs"
)

func TestTheEgressSocketGroupIsTheSameInAllThreePlaces(t *testing.T) {
	var broker appsv1.DaemonSet
	decodeManifest(t, filepath.Join(manifestDir, egressDaemonSetFile), &broker)

	pod := broker.Spec.Template.Spec
	if pod.SecurityContext == nil || pod.SecurityContext.RunAsGroup == nil {
		t.Fatalf("%s declares no pod-level runAsGroup; the broker's own gid is what the "+
			"other two follow", egressDaemonSetFile)
	}
	runAsGroup := *pod.SecurityContext.RunAsGroup

	init := initContainer(t, pod.InitContainers, socketDirInitName)
	gid, ok := envValue(init, socketGroupEnv)
	if !ok {
		t.Fatalf("the %s init container declares no %s; the gid it chowns to has to be "+
			"readable here, not buried in a shell string", socketDirInitName, socketGroupEnv)
	}
	chowned, err := strconv.ParseInt(gid.Value, 10, 64)
	if err != nil {
		t.Fatalf("%s = %q is not a gid: %v", socketGroupEnv, gid.Value, err)
	}
	if chowned != runAsGroup {
		t.Fatalf("the %s init container chowns the socket directory to gid %d and the broker "+
			"runs as gid %d; the broker would fail to bind", socketDirInitName, chowned, runAsGroup)
	}

	raw, err := os.ReadFile(filepath.Join(manifestDir, egressBrokerConfigRel))
	if err != nil {
		t.Fatalf("reading the node's egress config failed: %v", err)
	}
	match := socketGroupAttr.FindSubmatch(raw)
	if match == nil {
		t.Fatalf("no `#[config(default = <gid>u32, env = \"AENV_EGRESS_BROKER_SOCKET_GROUP\")]` " +
			"in src/cfg/egress_broker.rs; this guard is reading the wrong shape")
	}
	nodeDefault, err := strconv.ParseInt(string(match[1]), 10, 64)
	if err != nil {
		t.Fatalf("[egress_broker].socket_group default %q is not a gid: %v", match[1], err)
	}
	if nodeDefault != runAsGroup {
		t.Fatalf("[egress_broker].socket_group defaults to %d and the broker runs as gid %d; "+
			"every node would refuse to start in local mode", nodeDefault, runAsGroup)
	}
}

// The init container also has to be the only thing that prepares the
// directory: a node that chowned it too would hide a disagreement above by
// fixing it on whichever machine the node reached first.
func TestOnlyTheBrokerPreparesTheSocketDirectory(t *testing.T) {
	var broker appsv1.DaemonSet
	decodeManifest(t, filepath.Join(manifestDir, egressDaemonSetFile), &broker)
	init := initContainer(t, broker.Spec.Template.Spec.InitContainers, socketDirInitName)

	if init.SecurityContext == nil || init.SecurityContext.RunAsUser == nil ||
		*init.SecurityContext.RunAsUser != 0 {
		t.Fatalf("the %s init container does not run as root; nothing else on this Pod can "+
			"chown the hostPath", socketDirInitName)
	}
	if !mountsBrokerSocket(init) {
		t.Fatalf("the %s init container does not mount the socket hostPath", socketDirInitName)
	}
}

func initContainer(t *testing.T, containers []corev1.Container, name string) corev1.Container {
	t.Helper()
	for _, candidate := range containers {
		if candidate.Name == name {
			return candidate
		}
	}
	t.Fatalf("no init container named %q on the broker DaemonSet", name)
	return corev1.Container{}
}

func mountsBrokerSocket(container corev1.Container) bool {
	for _, mount := range container.VolumeMounts {
		if mount.MountPath == "/run/aenv-egress" && !mount.ReadOnly {
			return true
		}
	}
	return false
}
