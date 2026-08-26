package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Which snapshot repository the node keeps, and where its object-storage
// credentials are allowed to live.
//
// 🔴 `deploy/k8s/run.sh` copies `config/default.toml` over the cluster's
// `agentenv-k8s-config` ConfigMap on every apply (run.sh:30). The pve-sg
// cluster runs `repository_backend = "oss"` against a RustFS bucket holding
// ~2.7k objects, and it runs it because somebody applied from a working tree
// whose `config/default.toml` had been edited locally — credentials and all.
// That edit was never committed, so the repository's own answer to "which
// backend does this cluster use" is `posix_fs`, and every `apply -k` since has
// been one command away from making that the cluster's answer too.
//
// `posix_fs` is the dangerous direction precisely because it *works*: the node
// starts, serves an empty local snapshot store, and reports nothing. The rows
// in the catalog still name artifacts, and the process can no longer fetch one
// of them.
//
// This file pins the two facts that keep that recoverable, and one that keeps
// the credentials out of the repository for good.

const runtimeConfigPath = manifestDir + "/../../../config/default.toml"

// tomlAssignments returns the uncommented `name = value` assignments in a TOML
// file, keyed by the section they appear under.
//
// Scanned rather than parsed, the way `nodeIdentityClusterID` is and for the
// same reason: this package has no TOML dependency and the lines it is after
// are plain assignments. Comment lines are skipped deliberately —
// `config/default.toml` documents `[backend.oss]` as a commented-out example,
// and an example is not a setting.
func tomlAssignments(t *testing.T, raw string) map[string]map[string]string {
	t.Helper()

	out := map[string]map[string]string{}
	section := ""
	for _, line := range strings.Split(raw, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			section = strings.Trim(line, "[]")
			continue
		}
		name, value, found := strings.Cut(line, "=")
		if !found {
			continue
		}
		if out[section] == nil {
			out[section] = map[string]string{}
		}
		out[section][strings.TrimSpace(name)] = strings.Trim(strings.TrimSpace(value), `"`)
	}
	return out
}

// 🔴 No credential ever reaches this repository.
//
// The live ConfigMap carries a plaintext RustFS access key and secret. They got
// there through `kubectl apply` on 2026-08-17 from a tree whose
// `config/default.toml` had been edited by hand — so the shape of the mistake
// is not "somebody typed a secret into a manifest", it is "somebody edited the
// file run.sh copies, and applied". The same edit made one more time, committed
// this time, is a credential in git history forever.
//
// The cluster already keeps the identical pair in the `rustfs-credentials`
// Secret, so nothing here needs the literals; this scan makes sure nothing
// starts to.
func TestNoObjectStorageCredentialIsCommittedToTheRepository(t *testing.T) {
	// 🔴 The non-empty half, ahead of the scan. A scan whose whole result is an
	// absence proves nothing until the scanner is shown to find the thing when
	// it is there — otherwise a typo in the key names below reports exactly the
	// same clean result as a clean tree.
	planted := tomlAssignments(t, strings.Join([]string{
		"[backend.oss]",
		`access_key_id = "aenvSOMETHINGSOMETHING"`,
		`# access_key_secret = "this one is a comment and must not count"`,
	}, "\n"))
	if got := planted["backend.oss"]["access_key_id"]; got != "aenvSOMETHINGSOMETHING" {
		t.Fatalf("the scanner cannot see a credential assignment it is meant to catch: got %q", got)
	}
	if _, found := planted["backend.oss"]["access_key_secret"]; found {
		t.Fatal("the scanner counted a commented-out example as a setting; config/default.toml " +
			"documents [backend.oss] that way and would trip this test forever")
	}

	// 🔴 The second control, and the one that keeps this test usable. The
	// scanner must stay *blind* to a commented-out example: `config/default.toml`
	// documents `[backend.oss]` as one, and a scanner that counted it would be
	// red on a clean tree from the day it was written — which is a scanner
	// somebody deletes rather than one that catches anything.
	runtimeRaw, err := os.ReadFile(runtimeConfigPath)
	if err != nil {
		t.Fatalf("reading the runtime config failed: %v", err)
	}
	if !strings.Contains(string(runtimeRaw), "# access_key_id") {
		t.Fatal("config/default.toml no longer documents [backend.oss] with a commented-out " +
			"access_key_id. That example is this test's negative control: without it, nothing " +
			"proves the scan below distinguishes documentation from a setting.")
	}

	credentialKeys := []string{"access_key_id", "access_key_secret", "security_token"}

	// 🔴 Everything under `config/` and `deploy/`, not just
	// `deploy/k8s/base`. The credential that reached the cluster came through
	// `config/default.toml`, one directory outside what this scan used to
	// read, and `deploy/` also holds `docker-compose.yml`, the Docker image
	// config and the kustomize overlays — every one of them a file somebody
	// could reasonably paste an endpoint and a key into.
	//
	// 🔴 What it deliberately does not read: source and documentation. Both
	// contain `[backend.oss]` blocks that are *supposed* to look like this —
	// `src/cfg.rs` builds one in a test fixture and
	// `docs/src/getting-started/on-demand-loading.md` shows one with
	// `YOUR_ACCESS_KEY_ID` in it — and a scan that flagged them would be red on
	// a clean tree, which is a scan that gets deleted rather than one that
	// catches anything. Both were checked when this was widened; neither holds
	// a real value. The failure this guards is a credential in a file that gets
	// *applied*, and those all live under the two roots below.
	repoRoot := filepath.Join(manifestDir, "..", "..", "..")
	roots := []string{
		filepath.Join(repoRoot, "config"),
		filepath.Join(repoRoot, "deploy"),
	}
	scanned := 0
	walk := func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if !info.Mode().IsRegular() || info.Size() > 512*1024 {
			return nil
		}
		raw, readErr := os.ReadFile(path)
		if readErr != nil {
			return nil
		}
		scanned++
		for section, assignments := range tomlAssignments(t, string(raw)) {
			if !strings.HasPrefix(section, "backend.") {
				continue
			}
			for _, key := range credentialKeys {
				if value, found := assignments[key]; found && value != "" {
					t.Errorf("%s carries a committed credential: [%s].%s. "+
						"Credentials belong in the `rustfs-credentials` Secret, reaching the node "+
						"as the overlay file AENV_CONFIG_OVERLAY_PATH names — never in a file this "+
						"repository tracks, and never in the ConfigMap run.sh generates from one.",
						path, section, key)
				}
			}
		}
		return nil
	}
	for _, root := range roots {
		if err := filepath.Walk(root, walk); err != nil {
			t.Fatalf("walking %s failed: %v", root, err)
		}
	}
	if scanned < 20 {
		t.Fatalf("the credential scan read only %d files; it is not looking at config/ and "+
			"deploy/ and its clean result means nothing", scanned)
	}
}

// 🔴 `[pg].dsn` is the same shape of hazard as `[backend.oss]`'s credentials,
// and it needs its own scan: the walk above only looks at sections prefixed
// `backend.`, so a `dsn` committed under `[pg]` would sail straight past it.
//
// Same design as `[backend.oss]`: `config/default.toml` documents `dsn` as a
// commented-out example (`# dsn = "postgres://user:password@host:5432/dbname"`)
// and that line is this test's negative control, the same way
// `# access_key_id` is the object-storage scan's.
//
// 🔴 The scan checks every section's `dsn` key, not only `assignments["pg"]`.
// `deploy/k8s/base/config/pg-overlay.toml` documents `[pg]` as a *fully
// commented* block, header included — unlike `[backend.oss]` in
// `config/default.toml`, whose header is live and only the example
// assignments are commented. The most likely hand-edit of that file is
// uncommenting the `dsn = "..."` line alone and leaving `# [pg]` on the line
// above it untouched: `tomlAssignments` then has no live section to file the
// assignment under (the header is still a comment), so it lands under
// whichever section — commonly the empty one — was open before the comment
// block. A scan that only ever looks at `assignments["pg"]["dsn"]` never sees
// that: the credential sails through under a `dsn` key it simply never
// checks.
func TestNoPgDsnIsCommittedToTheRepository(t *testing.T) {
	// 🔴 The non-empty half, ahead of the scan — see the identical note on
	// TestNoObjectStorageCredentialIsCommittedToTheRepository for why this
	// control has to come first.
	planted := tomlAssignments(t, strings.Join([]string{
		"[pg]",
		`dsn = "postgres://aenv:SOMETHINGSOMETHING@host:5432/aenv"`,
	}, "\n"))
	if got := planted["pg"]["dsn"]; got != "postgres://aenv:SOMETHINGSOMETHING@host:5432/aenv" {
		t.Fatalf("the scanner cannot see a dsn assignment it is meant to catch: got %q", got)
	}

	// 🔴 The blind-spot control: `[pg]`'s header commented out, only `dsn`
	// uncommented — exactly the hand-edit of `pg-overlay.toml` described
	// above. The assignment must still be visible *somewhere* in the parsed
	// sections, or the walk below (which now checks every section, not just
	// "pg") has nothing to find either.
	blindSpot := tomlAssignments(t, strings.Join([]string{
		"# [pg]",
		`dsn = "postgres://aenv:SOMETHINGSOMETHING@host:5432/aenv"`,
	}, "\n"))
	foundBlindSpot := false
	for _, assignments := range blindSpot {
		if dsn := assignments["dsn"]; dsn != "" {
			foundBlindSpot = true
		}
	}
	if !foundBlindSpot {
		t.Fatal("the scanner cannot see a dsn assignment that lands outside the [pg] section " +
			"because the section header itself is commented out — this is the exact shape a " +
			"hand-edit of pg-overlay.toml produces by uncommenting only the dsn line and leaving " +
			"\"# [pg]\" alone on the line above it")
	}

	runtimeRaw, err := os.ReadFile(runtimeConfigPath)
	if err != nil {
		t.Fatalf("reading the runtime config failed: %v", err)
	}
	if !strings.Contains(string(runtimeRaw), "# dsn = ") {
		t.Fatal("config/default.toml no longer documents [pg] with a commented-out dsn example. " +
			"That example is this test's negative control: without it, nothing proves the scan " +
			"below distinguishes documentation from a setting.")
	}

	repoRoot := filepath.Join(manifestDir, "..", "..", "..")
	roots := []string{
		filepath.Join(repoRoot, "config"),
		filepath.Join(repoRoot, "deploy"),
	}
	scanned := 0
	walk := func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if !info.Mode().IsRegular() || info.Size() > 512*1024 {
			return nil
		}
		raw, readErr := os.ReadFile(path)
		if readErr != nil {
			return nil
		}
		scanned++
		// 🔴 Every section, not just "pg" — see the func doc above for why a
		// `dsn` that landed outside a live `[pg]` header still has to be
		// caught.
		for section, assignments := range tomlAssignments(t, string(raw)) {
			if dsn, found := assignments["dsn"]; found && dsn != "" {
				t.Errorf("%s carries a committed credential: [%s].dsn. Credentials belong in the "+
					"`agentenv-postgres` Secret, reaching the process as the overlay file "+
					"AENV_CONFIG_OVERLAY_PATH names — never in a file this repository tracks, and "+
					"never in the ConfigMap run.sh generates from one.", path, section)
			}
		}
		return nil
	}
	for _, root := range roots {
		if err := filepath.Walk(root, walk); err != nil {
			t.Fatalf("walking %s failed: %v", root, err)
		}
	}
	if scanned < 20 {
		t.Fatalf("the credential scan read only %d files; it is not looking at config/ and "+
			"deploy/ and its clean result means nothing", scanned)
	}
}

// 🔴 The repository ships the backend that is safe to fall back to, and says so
// where the fallback is decided.
//
// This is the value an `apply -k` restores, and the value a node uses when
// nothing in its environment says otherwise. It is deliberately *not* `oss`:
// `oss` without a `[backend.oss]` section does not start, and a checkout has no
// business carrying one cluster's bucket.
func TestTheRuntimeConfigShipsThePosixFallback(t *testing.T) {
	raw, err := os.ReadFile(runtimeConfigPath)
	if err != nil {
		t.Fatalf("reading the runtime config failed: %v", err)
	}
	assignments := tomlAssignments(t, string(raw))

	got, found := assignments["snapshot"]["repository_backend"]
	if !found {
		t.Fatal("config/default.toml has no [snapshot].repository_backend; the value an apply " +
			"restores cannot be compared against anything")
	}
	if got != "posix_fs" {
		t.Errorf("[snapshot].repository_backend is %q, want \"posix_fs\": this is the value every "+
			"`make k8s-apply` writes into the cluster ConfigMap, so a checkout must carry the "+
			"backend that is safe to land on rather than one cluster's choice", got)
	}
}

// The env var every AENV_CONFIG_OVERLAY_PATH-reading workload except
// agentenv-api-deployment.yaml is set from.
//
// 🔴 Not the literal string `agentenv-api-deployment.yaml` itself reads,
// because that one is the one place the overlay chain diverges: `--role
// api`/`--role all` is the only role that may ever hold `[pg]`, so it needs
// two extra segments (`pg-overlay.toml`, `pg-dsn.toml`) that
// agentenv-daemonset.yaml must never mount — see the note on
// AENV_API_CONFIG_OVERLAY_PATH under snapshot-storage-config in
// kustomization.yaml. [`apiConfigOverlayLiteral`] is its own key for that
// reason: two ConfigMap literals, resolved separately below, rather than one
// list every reader is assumed to share.
const sharedConfigOverlayLiteral = "AENV_CONFIG_OVERLAY_PATH="

// The env var agentenv-api-deployment.yaml alone is set from — see
// [`sharedConfigOverlayLiteral`]'s own doc.
const apiConfigOverlayLiteral = "AENV_API_CONFIG_OVERLAY_PATH="

// The paths listed in the manifests against `literal` (one of
// [`sharedConfigOverlayLiteral`] or [`apiConfigOverlayLiteral`]), in the
// order they are applied.
//
// Empty segments are dropped, the way the loader drops them: a manifest is
// allowed to write "$(A):$(B)" and clear one half.
func overlayPathsFromManifestsFor(t *testing.T, literal string) []string {
	t.Helper()

	var out []string
	err := filepath.Walk(manifestDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		raw, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		for _, line := range strings.Split(string(raw), "\n") {
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "#") {
				continue
			}
			_, value, found := strings.Cut(trimmed, literal)
			if !found {
				continue
			}
			for _, segment := range strings.Split(strings.TrimSpace(value), ":") {
				segment = strings.TrimSpace(segment)
				if segment != "" {
					out = append(out, segment)
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking %s failed: %v", manifestDir, err)
	}
	return out
}

// The shared overlay chain — [`sharedConfigOverlayLiteral`] — which is what
// every caller before AENV_API_CONFIG_OVERLAY_PATH existed meant by "the"
// overlay path. Kept as the default for callers that are about
// [backend.oss] specifically: both workloads still read that section from
// this exact chain, api's included, since AENV_API_CONFIG_OVERLAY_PATH's
// value starts with the same two segments.
func overlayPathsFromManifests(t *testing.T) []string {
	t.Helper()
	return overlayPathsFromManifestsFor(t, sharedConfigOverlayLiteral)
}

// True when any manifest sets AENV_SNAPSHOT_REPOSITORY_BACKEND to oss.
func manifestsSetTheOssBackend(t *testing.T) bool {
	t.Helper()

	set := false
	scanned := 0
	err := filepath.Walk(manifestDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		raw, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		scanned++
		for _, line := range strings.Split(string(raw), "\n") {
			trimmed := strings.TrimSpace(line)
			// Comment lines are not settings — the explanation of why this is
			// dangerous is allowed to name the variable.
			if strings.HasPrefix(trimmed, "#") || !strings.Contains(trimmed, ossBackendEnv) {
				continue
			}
			if strings.Contains(trimmed, "oss") {
				set = true
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking %s failed: %v", manifestDir, err)
	}
	if scanned == 0 {
		t.Fatal("the manifest scan read no files at all; its result means nothing")
	}
	return set
}

const ossBackendEnv = "AENV_SNAPSHOT_REPOSITORY_BACKEND"

// The `[backend.oss]` assignments an overlay path resolves to in this
// repository, and whether it resolves to a repository file at all.
//
// A container path under `/workspace/config/` is a `subPath` of the
// `agentenv-k8s-config` ConfigMap, whose files live in
// `deploy/k8s/base/config/`. Anything else — `/etc/agentenv/backend/...` — is
// projected from a Secret and has no tracked copy, which is the entire point of
// it.
func overlaySectionFromRepository(t *testing.T, containerPath string) (map[string]string, bool) {
	t.Helper()

	if !strings.HasPrefix(containerPath, "/workspace/config/") {
		return nil, false
	}
	local := filepath.Join(manifestDir, "config", filepath.Base(containerPath))
	raw, err := os.ReadFile(local)
	if err != nil {
		t.Fatalf("AENV_CONFIG_OVERLAY_PATH names %s, which should be %s in this repository, and "+
			"it is not there: %v", containerPath, local, err)
	}
	return tomlAssignments(t, string(raw))["backend.oss"], true
}

// 🔴 The switch is only half of itself, and the halves must move together.
//
// `AENV_SNAPSHOT_REPOSITORY_BACKEND` survives an apply; `[backend.oss]` cannot
// be set from the environment at all. confique reaches a field from the
// environment only through `#[config(nested)]`, and `nested` may not be
// `Option<_>` — so `backend.oss`, which is `Option<OssBackendConfig>`, is
// deserialized from a file and nothing else. No `secretKeyRef` can reach the
// endpoint, the bucket or the credentials.
// `no_new_env_binding_is_declared_where_confique_cannot_read_it` in
// `src/cfg.rs` pins that from the Rust side.
//
// A manifest that sets the backend to `oss` while no file the same manifests
// mount carries the section produces a node that refuses to start:
// "backend.oss config is required when repository_backend = oss". Loud, and
// therefore survivable — but it is a fleet-wide outage on the next pod restart,
// and it is the exact half-finished state somebody lands in by wiring the
// ConfigMap and calling it done.
//
// 🔴 What changed since this was written: the section no longer has to be in
// `config/default.toml`, and must not be — that file is one file for every
// cluster, and run.sh copies it over the ConfigMap. It may instead come from a
// file named by AENV_CONFIG_OVERLAY_PATH. A file that exists in the tree but is
// named by no manifest does not count.
func TestTheOssBackendSwitchIsNeverSetWithoutTheSectionItNeeds(t *testing.T) {
	// The non-empty half: prove the scan can see the setting and does not count
	// the comment that explains it.
	for _, probe := range []struct {
		line string
		want bool
	}{
		{line: "      - " + ossBackendEnv + "=oss", want: true},
		{line: "  " + ossBackendEnv + ": oss", want: true},
		{line: "            # " + ossBackendEnv + "=oss would need the section too", want: false},
	} {
		trimmed := strings.TrimSpace(probe.line)
		saw := !strings.HasPrefix(trimmed, "#") &&
			strings.Contains(trimmed, ossBackendEnv) &&
			strings.Contains(trimmed, "oss")
		if saw != probe.want {
			t.Fatalf("the scan reads %q as set=%v, want %v; its result about the real manifests "+
				"cannot be trusted until it reads these correctly", probe.line, saw, probe.want)
		}
	}

	if !manifestsSetTheOssBackend(t) {
		return
	}

	// Everywhere the section is allowed to come from: the runtime config, and
	// every overlay the manifests mount, in order.
	raw, readErr := os.ReadFile(runtimeConfigPath)
	if readErr != nil {
		t.Fatalf("reading the runtime config failed: %v", readErr)
	}
	merged := map[string]string{}
	for key, value := range tomlAssignments(t, string(raw))["backend.oss"] {
		merged[key] = value
	}

	overlays := overlayPathsFromManifests(t)
	if len(overlays) == 0 {
		t.Fatal("a manifest sets " + ossBackendEnv + " to oss and no manifest sets " +
			"AENV_CONFIG_OVERLAY_PATH. [backend.oss] cannot come from anywhere else: it is " +
			"Option<OssBackendConfig> and confique never reads a non-nested Option from the " +
			"environment, and config/default.toml is overwritten by every apply.")
	}
	trackedOverlays := 0
	for _, overlay := range overlays {
		section, tracked := overlaySectionFromRepository(t, overlay)
		if !tracked {
			continue
		}
		trackedOverlays++
		for key, value := range section {
			merged[key] = value
		}
	}
	if trackedOverlays == 0 {
		t.Fatal("every overlay the manifests name comes from a Secret. The endpoint, bucket and " +
			"region are not credentials and belong in a file this repository tracks, where they " +
			"can be reviewed and where losing the Secret does not lose them too.")
	}

	for _, required := range []string{"endpoint", "bucket"} {
		if merged[required] == "" {
			t.Fatalf("a manifest sets %s to oss, but neither config/default.toml nor any overlay "+
				"the manifests mount carries [backend.oss].%s. A node applied in this state fails "+
				"startup with \"backend.oss config is required when repository_backend = oss\".",
				ossBackendEnv, required)
		}
	}
}

// 🔴 Every workload that reads the backend also mounts the files it is told to
// read, and both halves of them.
//
// The overlay list is a promise about the filesystem: a path that is named and
// not on disk stops the process, which is deliberate — the alternative is a
// node that quietly starts on `posix_fs` with an empty snapshot store. So a
// manifest that sets AENV_CONFIG_OVERLAY_PATH and forgets one of the two mounts
// is a workload that will not start, and nothing but this notices before the
// apply.
func TestEveryWorkloadReadingTheBackendMountsTheOverlaysItNames(t *testing.T) {
	// A container path counts as mounted when the workload either mounts that
	// exact file or mounts the directory holding it and projects that name into
	// it — which is how a Secret key with a different name arrives.
	mounts := func(manifest, containerPath string) bool {
		if strings.Contains(manifest, "mountPath: "+containerPath+"\n") {
			return true
		}
		dir := filepath.Dir(containerPath)
		return strings.Contains(manifest, "mountPath: "+dir+"\n") &&
			strings.Contains(manifest, "path: "+filepath.Base(containerPath)+"\n")
	}

	// 🔴 The counter-face, on planted manifests, before any verdict about the
	// real ones. Three shapes: the file mounted directly, the file projected
	// into a mounted directory, and neither.
	for _, probe := range []struct {
		name     string
		manifest string
		want     bool
	}{
		{"direct", "            - mountPath: /etc/x/y.toml\n              subPath: y.toml\n", true},
		{"projected", "            - mountPath: /etc/x\n              items:\n              - key: k\n                path: y.toml\n", true},
		{"directory only", "            - mountPath: /etc/x\n", false},
		{"nothing", "            - mountPath: /workspace\n", false},
	} {
		if got := mounts(probe.manifest, "/etc/x/y.toml"); got != probe.want {
			t.Fatalf("the mount check reads the %q shape as %v, want %v; its verdict on the real "+
				"manifests means nothing until it reads these correctly", probe.name, got, probe.want)
		}
	}

	// 🔴 Each workload against its *own* overlay chain, not one shared list —
	// see the note on apiConfigOverlayLiteral for why agentenv-api-deployment.yaml
	// reads a different ConfigMap key than agentenv-daemonset.yaml does: it is
	// the only workload that may ever hold `[pg]`, and its chain carries two
	// segments (pg-overlay.toml, pg-dsn.toml) the DaemonSet must never mount.
	workloads := []struct {
		manifest string
		literal  string
	}{
		{"agentenv-daemonset.yaml", sharedConfigOverlayLiteral},
		{"agentenv-api-deployment.yaml", apiConfigOverlayLiteral},
	}
	for _, w := range workloads {
		overlays := overlayPathsFromManifestsFor(t, w.literal)
		if len(overlays) == 0 {
			t.Errorf("no manifest sets %s, so %s's own overlay chain cannot be checked",
				w.literal, w.manifest)
			continue
		}

		raw, err := os.ReadFile(filepath.Join(manifestDir, w.manifest))
		if err != nil {
			t.Fatalf("reading %s failed: %v", w.manifest, err)
		}
		manifest := string(raw)

		if !strings.Contains(manifest, "key: "+ossBackendEnv+"\n") {
			t.Errorf("%s does not read %s. Both halves must resolve snapshots out of the same "+
				"repository; one on posix_fs while the other is on oss answers \"no such "+
				"snapshot\" for artifacts that are sitting in the bucket, and reports nothing.",
				w.manifest, ossBackendEnv)
		}
		literalKey := strings.TrimSuffix(w.literal, "=")
		if !strings.Contains(manifest, "key: "+literalKey+"\n") {
			t.Errorf("%s does not read %s, so it never sees the overlay chain that name promises",
				w.manifest, literalKey)
		}
		for _, overlay := range overlays {
			if !mounts(manifest, overlay) {
				t.Errorf("%s is told to read %s and does not mount it. A named overlay that is "+
					"not on disk stops the process — deliberately, because the alternative is a "+
					"node that starts on posix_fs with an empty snapshot store.",
					w.manifest, overlay)
			}
		}
	}

	// 🔴 And the divergence itself must stay additive. api's chain has to be a
	// strict superset of the shared one — the two segments both workloads need
	// for [backend.oss] — never a replacement for it: losing that would mean
	// api quietly stopped resolving snapshots from the same repository the
	// DaemonSet does, the moment somebody edited AENV_API_CONFIG_OVERLAY_PATH
	// and dropped a segment instead of adding to it.
	shared := overlayPathsFromManifestsFor(t, sharedConfigOverlayLiteral)
	apiChain := overlayPathsFromManifestsFor(t, apiConfigOverlayLiteral)
	apiSet := map[string]bool{}
	for _, seg := range apiChain {
		apiSet[seg] = true
	}
	for _, seg := range shared {
		if !apiSet[seg] {
			t.Errorf("agentenv-api-deployment.yaml's own overlay chain (%v) is missing %q, which "+
				"the shared AENV_CONFIG_OVERLAY_PATH chain (%v) carries — api would stop seeing "+
				"[backend.oss] the way the DaemonSet does", apiChain, seg, shared)
		}
	}
}

// 🔴 No single loss lands this cluster on `posix_fs`.
//
// `posix_fs` is the dangerous direction because it *works*: the node starts,
// serves an empty local snapshot store, logs nothing above `info`, and the
// catalog goes on naming artifacts the process can no longer fetch. The backend
// is therefore said twice — as an environment variable and inside the overlay
// file — and this resolves the manifests the way the loader does to check that
// losing either one alone does not reach it.
//
// Precedence, as `src/cfg.rs` implements it: `config/default.toml`, then each
// overlay in order, then the environment.
func TestNoSingleLostSwitchLandsTheClusterOnPosixFs(t *testing.T) {
	raw, err := os.ReadFile(runtimeConfigPath)
	if err != nil {
		t.Fatalf("reading the runtime config failed: %v", err)
	}
	fileBackend := tomlAssignments(t, string(raw))["snapshot"]["repository_backend"]

	overlayBackend := ""
	overlaySection := map[string]string{}
	for _, overlay := range overlayPathsFromManifests(t) {
		local := filepath.Join(manifestDir, "config", filepath.Base(overlay))
		if !strings.HasPrefix(overlay, "/workspace/config/") {
			continue
		}
		overlayRaw, readErr := os.ReadFile(local)
		if readErr != nil {
			t.Fatalf("reading %s failed: %v", local, readErr)
		}
		assignments := tomlAssignments(t, string(overlayRaw))
		if value := assignments["snapshot"]["repository_backend"]; value != "" {
			overlayBackend = value
		}
		for key, value := range assignments["backend.oss"] {
			overlaySection[key] = value
		}
	}

	envBackend := ""
	if manifestsSetTheOssBackend(t) {
		envBackend = "oss"
	}

	resolve := func(withOverlay, withEnv bool) string {
		effective := fileBackend
		if withOverlay && overlayBackend != "" {
			effective = overlayBackend
		}
		if withEnv && envBackend != "" {
			effective = envBackend
		}
		return effective
	}

	// 🔴 The control first: the resolver has to be able to *say* posix_fs, or
	// the three assertions below are three ways of reading a constant. With
	// both switches gone this is exactly what an apply leaves behind, and it is
	// the regression this whole file exists to stop.
	if got := resolve(false, false); got != "posix_fs" {
		t.Fatalf("with neither switch the resolver says %q, want \"posix_fs\": it is not reading "+
			"config/default.toml and its other answers mean nothing", got)
	}

	if got := resolve(true, true); got != "oss" {
		t.Errorf("the manifests as they stand resolve to %q, want \"oss\". An apply would move "+
			"this cluster's ~2.7k snapshots out of reach without failing.", got)
	}
	if got := resolve(true, false); got != "oss" {
		t.Errorf("losing %s alone resolves to %q, want \"oss\". The overlay file carries the "+
			"backend as well for exactly this case; put it back.", ossBackendEnv, got)
	}
	if got := resolve(false, true); got != "oss" {
		t.Errorf("losing the overlay alone resolves to %q, want \"oss\"", got)
	}

	// 🔴 And that last case is loud rather than merely correct: with the
	// overlay gone the section goes with it, and `oss` without a section
	// refuses to start instead of serving an empty store. Which is why the two
	// halves may not be merged into one switch.
	if overlaySection["endpoint"] == "" || overlaySection["bucket"] == "" {
		t.Error("the overlay file no longer carries [backend.oss].endpoint/bucket, so losing it " +
			"would leave the environment saying oss with no section anywhere — still loud, but " +
			"for a different reason than this test describes")
	}
	for _, credential := range []string{"access_key_id", "access_key_secret", "security_token"} {
		if overlaySection[credential] != "" {
			t.Errorf("the tracked overlay carries %s. The credentials come from the "+
				"rustfs-credentials Secret and from nowhere else; the deep merge exists so that "+
				"this file does not have to hold them.", credential)
		}
	}
}

// 🔴 Order, not membership.
//
// TestEveryWorkloadReadingTheBackendMountsTheOverlaysItNames's superset check
// (its final block, comparing apiChain against shared) uses a
// `map[string]bool`, which cannot see order at all. Writing
// AENV_API_CONFIG_OVERLAY_PATH's four segments as
// "...backend-oss.toml:...oss-overlay.toml:...pg-dsn.toml:...pg-overlay.toml"
// — both tracked-then-secret pairs reversed — passes that superset check and
// every other assertion in this file (each segment still exists, is still
// mounted, is still a superset of the shared chain), while silently
// reversing precedence: kustomization.yaml's own comment on
// `config/oss-overlay.toml` and on `config/pg-overlay.toml` both say the
// Secret-backed file is "listed last so it wins where they overlap" — with
// the pair reversed the credential-free tracked file would win instead, and
// the Secret's endpoint/dsn would be silently discarded by the merge.
func TestApiOverlayChainListsEachSecretFileAfterItsTrackedFile(t *testing.T) {
	chain := overlayPathsFromManifestsFor(t, apiConfigOverlayLiteral)
	if len(chain) == 0 {
		t.Fatal("no manifest sets " + apiConfigOverlayLiteral + ", so its ordering cannot be checked")
	}

	indexOfSuffix := func(suffix string) int {
		for i, seg := range chain {
			if strings.HasSuffix(seg, suffix) {
				return i
			}
		}
		t.Fatalf("the api overlay chain %v names no segment ending in %q", chain, suffix)
		return -1
	}

	ossTracked := indexOfSuffix("oss-overlay.toml")
	ossSecret := indexOfSuffix("backend-oss.toml")
	pgTracked := indexOfSuffix("pg-overlay.toml")
	pgSecret := indexOfSuffix("pg-dsn.toml")

	if ossSecret < ossTracked {
		t.Errorf("backend-oss.toml (position %d) is listed before oss-overlay.toml (position %d) "+
			"in %v: the credential-free tracked file would win over the Secret in the deep merge, "+
			"silently discarding whatever endpoint/bucket/credentials the Secret carries",
			ossSecret, ossTracked, chain)
	}
	if pgSecret < pgTracked {
		t.Errorf("pg-dsn.toml (position %d) is listed before pg-overlay.toml (position %d) in %v: "+
			"the tracked file would win over the Secret's dsn in the deep merge, and [pg] would "+
			"never see a connection string", pgSecret, pgTracked, chain)
	}
}

// envNameForConfigMapKey returns the `name:` of the env entry whose
// `configMapKeyRef.key` is exactly `targetKey`, or "" if no entry reads that
// key. Scanned rather than parsed, like the rest of this file: a `- name:`
// line (the dash marks a new env-list entry) sets the current entry's own
// name, and everything after it up to the next `- name:` line — including
// the nested `configMapKeyRef.name`, which is a ConfigMap name, not an env
// var name — belongs to that entry.
func envNameForConfigMapKey(manifest, targetKey string) string {
	current := ""
	for _, line := range strings.Split(manifest, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "- name: ") {
			current = strings.TrimSpace(strings.TrimPrefix(trimmed, "- name: "))
			continue
		}
		if trimmed == "key: "+targetKey {
			return current
		}
	}
	return ""
}

// 🔴 The `name:`/`key:` pairing, not just the key's presence.
//
// TestEveryWorkloadReadingTheBackendMountsTheOverlaysItNames only checks that
// `"key: "+literalKey+"\n"` appears somewhere in the manifest — it never
// checks which env var's `name:` that `key:` sits under. `src/cfg.rs` reads
// `AENV_CONFIG_OVERLAY_PATH` and nothing else; both workloads' ConfigMap keys
// (the shared `AENV_CONFIG_OVERLAY_PATH` key and api's own
// `AENV_API_CONFIG_OVERLAY_PATH` key, chosen precisely so a plain ConfigMap
// value could carry api's longer, four-segment list without the DaemonSet
// ever seeing it — see kustomization.yaml's own note on
// `AENV_API_CONFIG_OVERLAY_PATH` under snapshot-storage-config) must both be
// projected into an env var whose own `name:` is the literal
// `AENV_CONFIG_OVERLAY_PATH`. A rename of the *env var's* `name:` alone (the
// ConfigMap key left untouched) passes every existing check in this file —
// the key is still there, the file is still mounted — while the workload
// silently stops reading any overlay at all: no `[backend.oss]`, and for
// agentenv-api-deployment.yaml, no `[pg]` either.
func TestConfigOverlayEnvNameMatchesWhatTheProcessReads(t *testing.T) {
	// 🔴 The non-empty half: prove the helper can tell two entries in the
	// same block apart before trusting its verdict on the real manifests.
	probe := strings.Join([]string{
		"        env:",
		"          - name: SOME_OTHER_VAR",
		"            valueFrom:",
		"              configMapKeyRef:",
		"                name: some-config",
		"                key: SOME_OTHER_KEY",
		"          - name: AENV_CONFIG_OVERLAY_PATH",
		"            valueFrom:",
		"              configMapKeyRef:",
		"                name: snapshot-storage-config",
		"                key: AENV_API_CONFIG_OVERLAY_PATH",
	}, "\n")
	if got := envNameForConfigMapKey(probe, "AENV_API_CONFIG_OVERLAY_PATH"); got != "AENV_CONFIG_OVERLAY_PATH" {
		t.Fatalf("envNameForConfigMapKey misreads a planted two-entry block: got %q, want "+
			"\"AENV_CONFIG_OVERLAY_PATH\" — its verdict on the real manifests means nothing until "+
			"it reads this correctly", got)
	}
	if got := envNameForConfigMapKey(probe, "SOME_OTHER_KEY"); got != "SOME_OTHER_VAR" {
		t.Fatalf("envNameForConfigMapKey misreads the first entry in a planted two-entry block: "+
			"got %q, want \"SOME_OTHER_VAR\"", got)
	}
	if got := envNameForConfigMapKey(probe, "no-such-key"); got != "" {
		t.Fatalf("envNameForConfigMapKey found a name for a key that is not in the block: %q", got)
	}

	for _, workload := range []struct {
		manifest string
		key      string
	}{
		{"agentenv-api-deployment.yaml", "AENV_API_CONFIG_OVERLAY_PATH"},
		{"agentenv-daemonset.yaml", "AENV_CONFIG_OVERLAY_PATH"},
	} {
		raw, err := os.ReadFile(filepath.Join(manifestDir, workload.manifest))
		if err != nil {
			t.Fatalf("reading %s failed: %v", workload.manifest, err)
		}
		got := envNameForConfigMapKey(string(raw), workload.key)
		if got != "AENV_CONFIG_OVERLAY_PATH" {
			t.Errorf("%s: the env entry reading ConfigMap key %s is named %q, want "+
				"\"AENV_CONFIG_OVERLAY_PATH\" — that is the exact variable name src/cfg.rs reads; "+
				"anything else and this workload silently stops seeing any overlay file at all",
				workload.manifest, workload.key, got)
		}
	}
}

// 🔴 `--role node` must never hold `[pg]` — `ServerRole::check_pg_dsn`
// refuses startup outright the moment `[pg].dsn` is configured at all, which
// on a DaemonSet means every node Pod in the fleet CrashLoops at once, not
// just the one workload that made the mistake.
//
// Nothing in this file asserted that before: the generic overlay/mount tests
// above are satisfied as long as whatever AENV_CONFIG_OVERLAY_PATH names is
// actually mounted, and would stay green even if `[pg]`'s two files were
// appended to the *shared* ConfigMap key and agentenv-daemonset.yaml grew the
// matching volume mounts to go with them — every existing assertion in this
// file passes on that tree, and every node in the fleet fails to start.
func TestDaemonSetNeverReadsPg(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(manifestDir, "agentenv-daemonset.yaml"))
	if err != nil {
		t.Fatalf("reading agentenv-daemonset.yaml failed: %v", err)
	}
	manifest := string(raw)

	for _, forbidden := range []string{
		"pg-overlay.toml",
		"pg-dsn.toml",
		"pg-credentials",
		"[pg]",
		"AENV_API_CONFIG_OVERLAY_PATH",
	} {
		if strings.Contains(manifest, forbidden) {
			t.Errorf("agentenv-daemonset.yaml mentions %q. --role node must never hold [pg]: "+
				"ServerRole::check_pg_dsn refuses startup outright the moment [pg].dsn is "+
				"configured, which on a DaemonSet CrashLoops every node Pod in the fleet at once.",
				forbidden)
		}
	}

	// 🔴 Belt and suspenders: even if a future rename stops literally saying
	// "pg" in agentenv-daemonset.yaml's own text, the *resolved* overlay
	// chain it reads (the shared AENV_CONFIG_OVERLAY_PATH key, read from
	// kustomization.yaml's configMapGenerator) must not carry a pg segment
	// either — this is what would actually reach the process at runtime.
	for _, seg := range overlayPathsFromManifests(t) {
		if strings.Contains(strings.ToLower(seg), "pg") {
			t.Errorf("the shared AENV_CONFIG_OVERLAY_PATH chain %v names %q, which --role node "+
				"(agentenv-daemonset.yaml) also reads — that chain must never carry a pg segment",
				overlayPathsFromManifests(t), seg)
		}
	}
}
