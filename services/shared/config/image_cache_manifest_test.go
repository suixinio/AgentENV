package config

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// The two node-local image-cache budgets, as manifest assertions.
//
// 🔴 These ran on the live cluster for months without being in this repository.
// The live `agentenv-k8s-config` ConfigMap carried `capacity_gb = 24` /
// `remote_blocks.max_size_gb = 12` with a note explaining the sizing — but
// `deploy/k8s/run.sh` copies config/default.toml over config/agentenv.toml on
// every apply (run.sh:30), and default.toml ships 100 / 100. So every
// `make k8s-apply` raised both budgets and nothing said so.
//
// 🔴 The regression is not "the cache is allowed to get bigger". It is that
// capacity eviction stops happening at all. A node may put capacity_gb +
// remote_blocks.max_size_gb + [backend.oss].cache_max_size_gb on its root disk,
// and eviction does not begin until `capacity_gb * high_watermark_ratio`. At
// 100/100/8 the ceiling is 208 GiB and the trip point 95 GiB, both past the
// 96 GiB root disk on aenv-master-01 — the disk fills first, the watermark
// never trips, and a budget that cannot be reached reports nothing.
//
// So the tests below pin the values, pin where both halves read them from, and
// — the part that actually matters — pin that they still *differ* from what a
// checkout ships. Two of those three would pass a manifest that had quietly
// stopped protecting anything.

const (
	imageCacheCapacityEnv     = "AENV_IMAGE_CACHE_CAPACITY_GB"
	imageCacheRemoteBlocksEnv = "AENV_IMAGE_CACHE_REMOTE_BLOCKS_MAX_SIZE_GB"
	imageCacheConfigMap       = "image-cache-config"

	// The ConfigMap these two keys must stay out of — see the control in
	// TestBothHalvesReadTheImageCacheBudgetFromOneSwitch.
	snapshotStorageConfigMap = "snapshot-storage-config"

	// The smallest root disk in the fleet, in GiB — aenv-master-01. Both nodes
	// mount one ConfigMap and `image.cache.*` has no per-node override, so this
	// is the machine the budgets are sized for.
	smallestRootDiskGB = 96
)

// 🔴 Both halves resolve images, so both carry the same budget — and, as with
// the snapshot catalog, the only way to guarantee that is one value read twice
// rather than two values kept in step by hand.
//
// The control is snapshot-storage-config, the ConfigMap immediately above this
// one in kustomization.yaml. These two keys must NOT be there, and that absence
// is checked here rather than left implied: the two objects have deliberately
// different rollback semantics — clearing AENV_CONFIG_OVERLAY_PATH is meant to
// fail startup loudly, clearing these two is meant to fall back to 100/100 and
// start fine — and merging them would mean an operator rolling back the overlay
// could not avoid also resetting the disk budgets. A test that only looked for
// "some ConfigMap carries these keys" would pass the merged arrangement.
func TestBothHalvesReadTheImageCacheBudgetFromOneSwitch(t *testing.T) {
	apiContainer := onlyContainer(t, "the api Deployment", apiDeployment(t).Spec.Template.Spec.Containers)
	nodeContainer := onlyContainer(t, "the node DaemonSet", nodeDaemonSet(t).Spec.Template.Spec.Containers)

	for _, env := range []string{imageCacheCapacityEnv, imageCacheRemoteBlocksEnv} {
		apiRef := configMapKeyRefFor(t, "the api Deployment", apiContainer, env)
		nodeRef := configMapKeyRefFor(t, "the node DaemonSet", nodeContainer, env)

		if apiRef.Name != nodeRef.Name || apiRef.Key != nodeRef.Key {
			t.Fatalf("the two halves read %s from different places: api %s/%s, node %s/%s — two "+
				"places is two values to keep in step, and a node that evicts on a different "+
				"budget than the half that resolved the image reports nothing either way",
				env, apiRef.Name, apiRef.Key, nodeRef.Name, nodeRef.Key)
		}
		if apiRef.Name != imageCacheConfigMap || apiRef.Key != env {
			t.Fatalf("%s is read from %s/%s, want %s/%s", env, apiRef.Name, apiRef.Key, imageCacheConfigMap, env)
		}
		// Optional on both, together. Losing the whole ConfigMap drops the pair
		// back to default.toml's 100/100 — the old regression, not an outage —
		// so a mandatory reference here would trade a silent oversize cache for
		// a fleet that will not start. The tree pins itself at one mandatory
		// reference and this is not it.
		if apiRef.Optional == nil || !*apiRef.Optional {
			t.Fatalf("the api Deployment reads %s with optional=%s, want true", env, describeOptional(apiRef.Optional))
		}
		if nodeRef.Optional == nil || !*nodeRef.Optional {
			t.Fatalf("the node DaemonSet reads %s with optional=%s, want true", env, describeOptional(nodeRef.Optional))
		}
	}

	// The control. If snapshot-storage-config ever starts carrying these, the
	// assertions above have stopped distinguishing "the budgets have their own
	// switch" from "the budgets are set somewhere".
	for _, env := range []string{imageCacheCapacityEnv, imageCacheRemoteBlocksEnv} {
		if literal, ok := generatorLiteralIfPresent(t, snapshotStorageConfigMap, env); ok {
			t.Fatalf("snapshot-storage-config carries %s=%s; the image-cache budgets belong to "+
				"their own ConfigMap because the two roll back differently — clearing the overlay "+
				"path is meant to fail startup, clearing these is meant to be survivable",
				env, literal)
		}
	}
}

// 🔴 The values themselves, and the only assertion in this file that would
// notice somebody "tidying up" the numbers.
func TestTheImageCacheBudgetIsSizedForTheSmallestDisk(t *testing.T) {
	capacity := generatorLiteralInt(t, imageCacheConfigMap, imageCacheCapacityEnv)
	remoteBlocks := generatorLiteralInt(t, imageCacheConfigMap, imageCacheRemoteBlocksEnv)
	ossCache := ossOverlayCacheMaxSizeGB(t)

	if capacity != 24 {
		t.Fatalf("%s is %d, want 24 — the value this cluster runs", imageCacheCapacityEnv, capacity)
	}
	if remoteBlocks != 12 {
		t.Fatalf("%s is %d, want 12 — the value this cluster runs", imageCacheRemoteBlocksEnv, remoteBlocks)
	}

	// What the three budgets together may put on a root disk, against the
	// smallest one in the fleet. This is the property the numbers exist for, so
	// it is checked as a property and not only as two constants: a future pair
	// that still summed past the disk would satisfy "is it 24" only by accident.
	ceiling := capacity + remoteBlocks + ossCache
	if ceiling >= smallestRootDiskGB {
		t.Fatalf("the three local-disk budgets sum to %d GiB (capacity %d + remote blocks %d + "+
			"oss cache %d) on a %d GiB root disk; the disk fills before capacity eviction's high "+
			"watermark trips and nothing reports it",
			ceiling, capacity, remoteBlocks, ossCache, smallestRootDiskGB)
	}

	// And the trip point has to be reachable, which is a stricter thing than
	// the ceiling fitting: eviction begins at capacity_gb * high_watermark_ratio
	// (0.95), so a capacity that fits the disk but leaves no room beside the
	// other two budgets still never evicts.
	tripPoint := capacity * 95 / 100
	if tripPoint+remoteBlocks+ossCache >= smallestRootDiskGB {
		t.Fatalf("capacity eviction trips at %d GiB, which with the other two budgets (%d + %d) "+
			"needs %d GiB of a %d GiB disk — it would never trip",
			tripPoint, remoteBlocks, ossCache, tripPoint+remoteBlocks+ossCache, smallestRootDiskGB)
	}
}

// 🔴 The control face for the whole file, and the assertion that would survive
// somebody deleting the other two.
//
// This ConfigMap protects nothing unless the value it carries differs from the
// one `deploy/k8s/run.sh` writes into config/agentenv.toml on every apply. If
// config/default.toml were ever "helpfully" changed to 24/12 to match, every
// other test here would still pass — the manifest would still say 24, both
// halves would still read it from one place — while the thing being defended
// against quietly became unreachable, and the next person to restore
// default.toml's shipping values would silently re-arm the regression with no
// test failing.
//
// So the assertion is that they disagree, and that the checkout's side is the
// larger one.
func TestTheManifestBudgetStillDiffersFromWhatACheckoutShips(t *testing.T) {
	for _, tc := range []struct {
		env     string
		section string
		key     string
	}{
		{imageCacheCapacityEnv, "image.cache", "capacity_gb"},
		{imageCacheRemoteBlocksEnv, "image.cache.remote_blocks", "max_size_gb"},
	} {
		manifest := generatorLiteralInt(t, imageCacheConfigMap, tc.env)
		shipped := defaultTomlInt(t, tc.section, tc.key)

		if manifest == shipped {
			t.Fatalf("%s is %d in %s and [%s].%s in config/default.toml is the same; this "+
				"ConfigMap exists precisely because run.sh overwrites that file on every apply, "+
				"so a value that agrees with it is defending against nothing — and restoring "+
				"default.toml later would re-arm the regression with no test noticing",
				tc.env, manifest, imageCacheConfigMap, tc.section, tc.key)
		}
		if shipped <= manifest {
			t.Fatalf("[%s].%s in config/default.toml is %d, which is not larger than the %d this "+
				"cluster pins; the regression this file guards is the apply raising the budget, "+
				"so a checkout that ships a smaller one means these tests are describing a "+
				"situation that no longer exists",
				tc.section, tc.key, shipped, manifest)
		}
	}
}

// generatorLiteralIfPresent is generatedLiteral without the t.Fatalf, for the
// absence checks above: "no such ConfigMap" and "that ConfigMap does not carry
// this key" are both a plain false here rather than a failed test.
func generatorLiteralIfPresent(t *testing.T, configMap, key string) (string, bool) {
	t.Helper()

	var kustomization struct {
		ConfigMapGenerator []struct {
			Name     string   `json:"name"`
			Literals []string `json:"literals"`
		} `json:"configMapGenerator"`
	}
	decodeManifest(t, filepath.Join(manifestDir, "kustomization.yaml"), &kustomization)

	for _, generator := range kustomization.ConfigMapGenerator {
		if generator.Name != configMap {
			continue
		}
		for _, literal := range generator.Literals {
			name, value, found := strings.Cut(literal, "=")
			if found && name == key {
				return value, true
			}
		}
	}
	return "", false
}

func generatorLiteralInt(t *testing.T, configMap, key string) int {
	t.Helper()

	raw := generatedLiteral(t, configMap, key)
	value, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		t.Fatalf("%s/%s is %q, which is not a number: %v", configMap, key, raw, err)
	}
	return value
}

// defaultTomlInt reads one integer assignment out of one table in
// config/default.toml. Scanned rather than parsed, the way nodeIdentityClusterID
// is: this package has no TOML dependency, and the lines it needs are plain
// assignments. Section-aware because `capacity_gb` and `max_size_gb` live in
// two different tables and both names recur in comments.
func defaultTomlInt(t *testing.T, section, key string) int {
	t.Helper()

	raw, err := os.ReadFile(runtimeConfigPath)
	if err != nil {
		t.Fatalf("reading the runtime config failed: %v", err)
	}

	current := ""
	for _, line := range strings.Split(string(raw), "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "#") {
			continue
		}
		if strings.HasPrefix(trimmed, "[") && strings.HasSuffix(trimmed, "]") {
			current = strings.Trim(trimmed, "[]")
			continue
		}
		if current != section {
			continue
		}
		name, value, found := strings.Cut(trimmed, "=")
		if !found || strings.TrimSpace(name) != key {
			continue
		}
		parsed, err := strconv.Atoi(strings.TrimSpace(value))
		if err != nil {
			t.Fatalf("[%s].%s in config/default.toml is %q, which is not a number: %v", section, key, value, err)
		}
		return parsed
	}
	t.Fatalf("config/default.toml has no [%s].%s; this test reads it to prove the manifest "+
		"disagrees with it, so it cannot pass by not finding it", section, key)
	return 0
}

// ossOverlayCacheMaxSizeGB reads the third local-disk budget out of the tracked
// overlay — the one file of the three that an apply does not rewrite.
func ossOverlayCacheMaxSizeGB(t *testing.T) int {
	t.Helper()

	raw, err := os.ReadFile(filepath.Join(manifestDir, "config", "oss-overlay.toml"))
	if err != nil {
		t.Fatalf("reading the tracked oss overlay failed: %v", err)
	}

	for _, line := range strings.Split(string(raw), "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "#") {
			continue
		}
		name, value, found := strings.Cut(trimmed, "=")
		if !found || strings.TrimSpace(name) != "cache_max_size_gb" {
			continue
		}
		parsed, err := strconv.Atoi(strings.TrimSpace(value))
		if err != nil {
			t.Fatalf("cache_max_size_gb in the oss overlay is %q, which is not a number: %v", value, err)
		}
		return parsed
	}
	t.Fatalf("the tracked oss overlay sets no cache_max_size_gb; it is one of the three budgets " +
		"that share the root disk and this test cannot check the sum without it")
	return 0
}
