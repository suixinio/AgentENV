package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The vocabulary of the REST upstream switch, in one table.
//
// 🔴 The empty string is a value here rather than an error, and it is the only
// address in this file that is: it is the switch off. Everything else either
// resolves to a base URL the gateway can forward to or stops the process. There
// is no third outcome, because the third outcome is a gateway that reports the
// switch as on and answers 502.
func TestParseRestUpstream(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  string
		want string
	}{
		{name: "off", raw: "", want: ""},
		{name: "off, written as whitespace", raw: "   ", want: ""},
		{name: "bare host and port, read as http", raw: "agentenv-api:8000", want: "http://agentenv-api:8000"},
		{name: "explicit http", raw: "http://agentenv-api:8000", want: "http://agentenv-api:8000"},
		{name: "https", raw: "https://api.example.invalid", want: "https://api.example.invalid"},
		// A trailing slash is what an operator pasting from a browser produces.
		// It means the same thing as no path at all, so it is dropped rather
		// than refused — and dropped rather than kept, since keeping it would
		// forward `/sandboxes` as `//sandboxes`.
		{name: "trailing slash is dropped", raw: "http://agentenv-api:8000/", want: "http://agentenv-api:8000"},
		{name: "an ipv6 literal", raw: "http://[fd00::1]:8000", want: "http://[fd00::1]:8000"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseRestUpstream(tc.raw)
			if err != nil {
				t.Fatalf("ParseRestUpstream(%q) failed: %v", tc.raw, err)
			}
			if got != tc.want {
				t.Fatalf("ParseRestUpstream(%q) = %q, want %q", tc.raw, got, tc.want)
			}
		})
	}

	for _, tc := range []struct {
		name string
		raw  string
		says string
	}{
		{name: "no host", raw: "http://", says: "names no host"},
		{name: "a scheme nothing speaks", raw: "grpc://agentenv-api:8002", says: "must be http or https"},
		// 🔴 A path prefix would be prepended to every forwarded route, so
		// `/sandboxes` would arrive at the api half as `/v2/sandboxes` — a real
		// route, answering something else entirely.
		{name: "a path prefix", raw: "http://agentenv-api:8000/v2", says: "must not carry a path"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := ParseRestUpstream(tc.raw); err == nil {
				t.Fatalf("ParseRestUpstream(%q) was accepted", tc.raw)
			} else if !strings.Contains(err.Error(), tc.says) {
				t.Fatalf("ParseRestUpstream(%q) said %q, want it to mention %q", tc.raw, err, tc.says)
			}
		})
	}
}

// 🔴 The refusal reaches the process, and not only the parser.
//
// A value that cannot be used has to stop the gateway at load, because the
// alternative is a 502 on every REST call while every switch reports itself as
// correctly on — and an operator would spend that incident looking at the api
// half, which is fine.
func TestALoadedGatewayConfigRefusesARestUpstreamItCannotUse(t *testing.T) {
	t.Setenv("GATEWAY_REST_UPSTREAM_ADDR", "")

	dir := t.TempDir()
	bad := filepath.Join(dir, "bad.json")
	if err := os.WriteFile(bad, []byte(`{"gateway":{"rest_upstream_addr":"grpc://agentenv-api:8002"}}`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	if _, err := Load(bad); err == nil {
		t.Fatal("a gateway config naming an unusable REST upstream loaded successfully")
	}

	// The control: the same file with a usable address loads, so the refusal
	// above is about the value rather than about the key existing at all.
	good := filepath.Join(dir, "good.json")
	if err := os.WriteFile(good, []byte(`{"gateway":{"rest_upstream_addr":"agentenv-api:8000"}}`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	cfg, err := Load(good)
	if err != nil {
		t.Fatalf("a gateway config naming a usable REST upstream failed to load: %v", err)
	}
	if cfg.Gateway.RestUpstreamAddr != "agentenv-api:8000" {
		t.Fatalf("rest_upstream_addr came out as %q", cfg.Gateway.RestUpstreamAddr)
	}

	// ...and the environment is refused on the same terms, which matters
	// because the environment is how this switch is actually flipped.
	t.Setenv("GATEWAY_REST_UPSTREAM_ADDR", "http://agentenv-api:8000/v2")
	if _, err := Load(good); err == nil {
		t.Fatal("an unusable REST upstream from the environment loaded successfully")
	}
}
