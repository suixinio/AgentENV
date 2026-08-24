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

	credentialKeys := []string{"access_key_id", "access_key_secret", "security_token"}

	roots := []string{manifestDir, filepath.Join(manifestDir, "..", "..", "..", "config")}
	scanned := 0
	for _, root := range roots {
		err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() {
				return err
			}
			raw, readErr := os.ReadFile(path)
			if readErr != nil {
				return readErr
			}
			scanned++
			for section, assignments := range tomlAssignments(t, string(raw)) {
				if !strings.HasPrefix(section, "backend.") {
					continue
				}
				for _, key := range credentialKeys {
					if value, found := assignments[key]; found && value != "" {
						t.Errorf("%s carries a committed credential: [%s].%s. "+
							"Credentials belong in the `rustfs-credentials` Secret, never in a file "+
							"this repository tracks and never in the ConfigMap run.sh generates from it.",
							path, section, key)
					}
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walking %s failed: %v", root, err)
		}
	}
	if scanned == 0 {
		t.Fatal("the credential scan read no files at all; its clean result means nothing")
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

// 🔴 The environment override is only half a switch, and the halves must move
// together.
//
// `AENV_SNAPSHOT_REPOSITORY_BACKEND` survives an apply; `[backend.oss]` does
// not. confique reaches a field from the environment only through
// `#[config(nested)]`, and `nested` may not be `Option<_>` — so `backend.oss`,
// which is `Option<OssBackendConfig>`, is deserialized from the file and
// nothing else. No `secretKeyRef` can reach the endpoint, the bucket or the
// credentials. `no_new_env_binding_is_declared_where_confique_cannot_read_it`
// in `src/cfg.rs` pins that from the Rust side.
//
// So a manifest that sets the backend to `oss` while the file it is applied
// alongside carries no `[backend.oss]` section produces a node that refuses to
// start: "backend.oss config is required when repository_backend = oss". Loud,
// and therefore survivable — but it is a fleet-wide outage on the next pod
// restart, and it is the exact half-finished state somebody lands in by wiring
// the ConfigMap and calling it done.
func TestTheOssBackendSwitchIsNeverSetWithoutTheSectionItNeeds(t *testing.T) {
	const backendEnv = "AENV_SNAPSHOT_REPOSITORY_BACKEND"

	setsOss := false
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
			if strings.HasPrefix(trimmed, "#") || !strings.Contains(trimmed, backendEnv) {
				continue
			}
			if strings.Contains(trimmed, "oss") {
				setsOss = true
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking %s failed: %v", manifestDir, err)
	}

	// The non-empty half: prove the scan can see the setting and does not count
	// the comment that explains it.
	for _, probe := range []struct {
		line string
		want bool
	}{
		{line: "      - " + backendEnv + "=oss", want: true},
		{line: "  " + backendEnv + ": oss", want: true},
		{line: "            # " + backendEnv + "=oss would need the section too", want: false},
	} {
		trimmed := strings.TrimSpace(probe.line)
		saw := !strings.HasPrefix(trimmed, "#") &&
			strings.Contains(trimmed, backendEnv) &&
			strings.Contains(trimmed, "oss")
		if saw != probe.want {
			t.Fatalf("the scan reads %q as set=%v, want %v; its result about the real manifests "+
				"cannot be trusted until it reads these correctly", probe.line, saw, probe.want)
		}
	}
	if scanned == 0 {
		t.Fatal("the manifest scan read no files at all; its result means nothing")
	}

	if !setsOss {
		return
	}

	raw, readErr := os.ReadFile(runtimeConfigPath)
	if readErr != nil {
		t.Fatalf("reading the runtime config failed: %v", readErr)
	}
	section := tomlAssignments(t, string(raw))["backend.oss"]
	for _, required := range []string{"endpoint", "bucket"} {
		if section[required] == "" {
			t.Fatalf("a manifest sets %s to oss, but config/default.toml — the file run.sh copies "+
				"over the cluster ConfigMap — carries no [backend.oss].%s. The environment cannot "+
				"supply it: [backend.oss] is Option<OssBackendConfig> and confique never reads a "+
				"non-nested Option from the environment. A node applied in this state fails "+
				"startup with \"backend.oss config is required when repository_backend = oss\".",
				backendEnv, required)
		}
	}
}
