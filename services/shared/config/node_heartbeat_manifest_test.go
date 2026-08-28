package config

import (
	"testing"
)

// Where every node's heartbeat reporter dials the scheduler, as manifest
// assertions.
//
// 🔴 This value used to be a bare literal on `agentenv-daemonset.yaml` — not
// even the ConfigMap indirection every other cluster-specific value in that
// file already gets — so changing it meant editing the DaemonSet inline and
// rolling every node: a serial, hour-long-grace-period roll
// (`docs/proposals/2026-08-20-service-decomposition.md`'s phase four section
// names this exact roll as the thing a rollback must not depend on). 阶段四
// 切片 0 fixes that by giving `src/observability/reporter.rs`'s
// `SchedulerChannelSource` a file to watch, mounted from the ConfigMap this
// file pins.
//
// The property that actually matters, and the one most likely to regress
// silently, is the mount's *shape*: it has to be a whole-ConfigMap mount, not
// a `subPath` mount. `subPath` mounts are bind-mounts of one file baked in at
// Pod creation, and kubelet never refreshes them — an operator could edit
// `node-heartbeat-config` correctly and roll the ConfigMap and nothing would
// ever reach the running Pod, only the next Pod. A test that only checked
// "the file is mounted somewhere" would pass that regression exactly as
// happily as the working manifest, so the assertion below is specifically
// that `SubPath` is empty.
const (
	nodeHeartbeatConfigMap  = "node-heartbeat-config"
	nodeHeartbeatEndpoint   = "AENV_OBSERVABILITY_SCHEDULER_ENDPOINT"
	nodeHeartbeatFileEnv    = "AENV_CLUSTER_SCHEDULER_ENDPOINT_FILE"
	nodeHeartbeatMountPath  = "/etc/agentenv/heartbeat"
	nodeHeartbeatFilePath   = nodeHeartbeatMountPath + "/scheduler-endpoint"
	nodeHeartbeatVolumeName = "heartbeat-config"
	nodeHeartbeatItemKey    = nodeHeartbeatEndpoint
	nodeHeartbeatItemPath   = "scheduler-endpoint"
)

// 🔴 The bootstrap env var is read from the ConfigMap, not a literal.
//
// This is the half that existed before this slice: the value the process
// reads once at startup. Pinning that it comes from `node-heartbeat-config`
// rather than an inline `value:` is what makes "the endpoint has one source
// of truth" a checked property instead of a comment — a literal here would
// silently reintroduce a second place this value can live, disagreeing with
// whatever the mounted file (below) says.
func TestNodeHeartbeatEndpointEnvIsReadFromTheConfigMap(t *testing.T) {
	node := nodeDaemonSet(t)
	container := onlyContainer(t, "the node DaemonSet", node.Spec.Template.Spec.Containers)

	ref := configMapKeyRefFor(t, "the node DaemonSet", container, nodeHeartbeatEndpoint)
	if ref.Name != nodeHeartbeatConfigMap || ref.Key != nodeHeartbeatEndpoint {
		t.Fatalf("%s is read from %s/%s, want %s/%s", nodeHeartbeatEndpoint, ref.Name, ref.Key,
			nodeHeartbeatConfigMap, nodeHeartbeatEndpoint)
	}
	if ref.Optional == nil || !*ref.Optional {
		t.Fatalf("the node DaemonSet reads %s with optional=%s, want true — losing this ConfigMap "+
			"must not fail the Pod, only stop heartbeat reporting", nodeHeartbeatEndpoint,
			describeOptional(ref.Optional))
	}

	value := generatedLiteral(t, nodeHeartbeatConfigMap, nodeHeartbeatEndpoint)
	if value == "" {
		t.Fatalf("%s/%s is generated empty; an empty scheduler endpoint disables heartbeat "+
			"reporting entirely (ReporterConfig::resolve in src/observability/reporter.rs)",
			nodeHeartbeatConfigMap, nodeHeartbeatEndpoint)
	}
}

// 🔴 The hot-reloadable half: a literal env var naming a path, and a real
// mount at that exact path, sourced from the same ConfigMap key as the
// bootstrap env var above. Either half missing makes the other inert — a path
// nothing reads, or a file nothing is pointed at — the same failure mode the
// control-plane-credential pair guards against in
// `TestTheNodeMountsRegctlConfigWhereRegctlLooksForIt`'s sibling tests.
func TestNodeHeartbeatEndpointFileIsMountedWithoutSubPath(t *testing.T) {
	node := nodeDaemonSet(t)
	container := onlyContainer(t, "the node DaemonSet", node.Spec.Template.Spec.Containers)

	fileEnv, ok := envValue(container, nodeHeartbeatFileEnv)
	if !ok {
		t.Fatalf("the node DaemonSet does not set %s; without it "+
			"src/observability/reporter.rs never looks for a hot-reloadable endpoint and silently "+
			"behaves as if this whole mechanism did not exist", nodeHeartbeatFileEnv)
	}
	if fileEnv.ValueFrom != nil || fileEnv.Value != nodeHeartbeatFilePath {
		t.Fatalf("%s is %+v, want a literal %q — it names a path inside this container image, not "+
			"anything that varies per cluster", nodeHeartbeatFileEnv, fileEnv, nodeHeartbeatFilePath)
	}

	mount, ok := volumeMountAt(container, nodeHeartbeatMountPath)
	if !ok {
		t.Fatalf("the node container mounts nothing at %s, which is exactly where %s points",
			nodeHeartbeatMountPath, nodeHeartbeatFileEnv)
	}
	if mount.Name != nodeHeartbeatVolumeName {
		t.Fatalf("the mount at %s comes from volume %q, want %q", nodeHeartbeatMountPath, mount.Name,
			nodeHeartbeatVolumeName)
	}
	if !mount.ReadOnly {
		t.Fatalf("the mount at %s is not read-only", nodeHeartbeatMountPath)
	}

	// The property this test exists for. A subPath mount here is not a
	// smaller version of the hot-reload mechanism, it is the mechanism
	// disabled: kubelet bind-mounts one file at Pod creation and never
	// refreshes it, exactly like every `subPath` mount already in this
	// DaemonSet (`agentenv-config`'s two uses of it), and an operator editing
	// `node-heartbeat-config` afterwards would change nothing until the next
	// Pod.
	if mount.SubPath != "" {
		t.Fatalf("the mount at %s has subPath %q; a subPath mount is never refreshed by kubelet, "+
			"which silently turns the hot-reload mechanism this slice adds into dead weight — "+
			"the ConfigMap can be edited forever and no running Pod will ever see it",
			nodeHeartbeatMountPath, mount.SubPath)
	}

	volume, ok := volumeNamed(node.Spec.Template.Spec.Volumes, nodeHeartbeatVolumeName)
	if !ok {
		t.Fatalf("the node DaemonSet declares no %q volume", nodeHeartbeatVolumeName)
	}
	if volume.ConfigMap == nil || volume.ConfigMap.Name != nodeHeartbeatConfigMap {
		t.Fatalf("the %q volume is not sourced from the %q ConfigMap (%+v)", nodeHeartbeatVolumeName,
			nodeHeartbeatConfigMap, volume.VolumeSource)
	}
	if volume.ConfigMap.Optional == nil || !*volume.ConfigMap.Optional {
		t.Fatalf("the %q volume has optional=%s, want true — losing this ConfigMap must not fail "+
			"the Pod", nodeHeartbeatVolumeName, describeOptional(volume.ConfigMap.Optional))
	}

	items := volume.ConfigMap.Items
	if len(items) != 1 {
		t.Fatalf("the %q volume projects %d items, want exactly 1 — projecting the whole ConfigMap "+
			"would silently start mounting any future unrelated key added to it as a file here too",
			nodeHeartbeatVolumeName, len(items))
	}
	if items[0].Key != nodeHeartbeatItemKey || items[0].Path != nodeHeartbeatItemPath {
		t.Fatalf("the %q volume projects key %q to path %q, want key %q to path %q",
			nodeHeartbeatVolumeName, items[0].Key, items[0].Path, nodeHeartbeatItemKey, nodeHeartbeatItemPath)
	}

	// The two halves must be the *same* ConfigMap key, or a value change could
	// update one without the other and the bootstrap env var and the
	// hot-reloadable file would name two different schedulers.
	envRef := configMapKeyRefFor(t, "the node DaemonSet", container, nodeHeartbeatEndpoint)
	if envRef.Name != volume.ConfigMap.Name || envRef.Key != items[0].Key {
		t.Fatalf("the bootstrap env var reads %s/%s but the mounted file reads %s/%s; these must "+
			"be the same key or the two can drift apart",
			envRef.Name, envRef.Key, volume.ConfigMap.Name, items[0].Key)
	}
}
