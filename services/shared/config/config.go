package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// GatewayExecutionFencing is the two-state switch over the gateway's routing
// layer refusal: whether it stamps the incarnation it routed against onto the
// request, and whether a mismatch is refused or only counted.
//
// 🔴 It governs the gateway alone. Named for its scope rather than merged with
// anything else, on purpose: the Go scheduler used to carry two siblings with
// names that read the same way and turned off different halves —
// scheduler.registry.write_fencing and scheduler.routing.execution_arbitration
// — and merging any of the three would have let a single panic-flip switch off
// a half nobody meant to, silently. Both siblings were deleted along with
// services/scheduler; the aenv-api/Rust side's equivalent is
// binding_store.arbitration (see src/cfg.rs).
//
// 🔴 A third state, Observe, lived here through the rollout that proved
// Enforce was safe to turn on everywhere: it compared and counted, and stamped
// nothing, so a 412 the node might otherwise have produced was never armed.
// That rollout is over — the deploy manifest's execution-fencing-config
// comment records the cluster reaching enforce — and the state was deleted
// from the type, from ParseGatewayExecutionFencing, and from decideFencing in
// execution_fencing.go. The literal string "observe" is not silently remapped
// to either remaining value: it now falls into this parser's own default case
// below and is refused, the same as any other unrecognised value.
type GatewayExecutionFencing string

const (
	// Off is the complete rollback: the gateway behaves byte for byte as it did
	// before execution fencing existed.
	GatewayExecutionFencingOff GatewayExecutionFencing = "off"
	// Enforce is the default. Both gates are live.
	GatewayExecutionFencingEnforce GatewayExecutionFencing = "enforce"
)

// ParseGatewayExecutionFencing is the one place a mode string becomes a mode.
//
// 🔴 An unrecognised value is an error, never a fallback. A fallback would make
// one mistyped letter switch fencing off — or on — without saying so, and the
// resulting behaviour is indistinguishable from the value having been meant.
// The empty string is not a mistyped value: it is the absence of a setting, and
// it resolves to the documented default. This is also, deliberately, what now
// happens to the literal "observe": it used to be a recognised third state and
// is not any more, so it is refused here rather than silently landing on
// enforce or off — a manifest that still names it fails loudly instead of
// starting in a mode the operator did not choose.
func ParseGatewayExecutionFencing(raw string) (GatewayExecutionFencing, error) {
	switch GatewayExecutionFencing(strings.ToLower(strings.TrimSpace(raw))) {
	case "":
		return GatewayExecutionFencingEnforce, nil
	case GatewayExecutionFencingOff:
		return GatewayExecutionFencingOff, nil
	case GatewayExecutionFencingEnforce:
		return GatewayExecutionFencingEnforce, nil
	default:
		return "", fmt.Errorf("gateway.routing.execution_fencing must be one of %s, %s, got %q",
			GatewayExecutionFencingOff, GatewayExecutionFencingEnforce, raw)
	}
}

// ParseRoutingProjectionSwitch is the one place an on/off setting becomes a
// bool.
//
// 🔴 An unrecognised value is an error rather than a false. These switches
// change what a running cluster does to its own routing table, and the failure
// mode of guessing is a rollout that reports success while the switch it was
// for never moved. "true"/"false"/"1"/"0" are accepted alongside "on"/"off"
// because operators reach for all of them and a typo should be the only thing
// that stops the process.
func ParseRoutingProjectionSwitch(raw string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "on", "true", "1":
		return true, nil
	case "off", "false", "0":
		return false, nil
	default:
		return false, fmt.Errorf("must be one of on, off (got %q)", raw)
	}
}

// GatewayRoutingConfig groups the switches over what the gateway does with a
// routing answer, as opposed to where it listens or who it talks to.
type GatewayRoutingConfig struct {
	ExecutionFencing GatewayExecutionFencing `json:"execution_fencing"`
	// ProjectionRead lets the gateway answer a sandbox route from the routing
	// projection directly, asking the api half's resume RPC only on a miss or
	// an error. Off sends every request naming a sandbox to that RPC.
	//
	// 🔴 Turning this on must be paired with the api half's
	// `[binding_store].arbitration` (src/cfg.rs) staying in its enforcing
	// mode: its rollback works by blanking the two incarnation fields on the
	// way out of the api half's sandbox lookup, and a gateway reading the
	// projection itself never sees that blanking, so it would go on fencing
	// against incarnations arbitration has stopped judging. There is no
	// mechanism for it — the pairing is an operational rule, written here
	// because this is where somebody reads it.
	ProjectionRead bool `json:"projection_read"`
}

type GatewayConfig struct {
	HTTPListenAddr    string `json:"http_listen_addr"`
	MetricsListenAddr string `json:"metrics_listen_addr"`
	// SchedulerAddr is whichever process answers
	// `services/api/proto/scheduler.proto` — `agentenv-api` on every current
	// deployment.
	//
	// 🔴 It also carries the wake-up RPC. There used to be a second address,
	// `resume_addr`, naming the api half's `SandboxResumeService`; it held the
	// same value as this one on every shipped deployment, because it is the
	// same gRPC listener, so `cmd/main.go` built two ClientConns to one
	// process. The field is deleted rather than defaulted to this one —
	// a knob with exactly one correct value is a knob somebody eventually
	// sets to a second one — and the gateway reuses this connection for both.
	SchedulerAddr string `json:"scheduler_addr"`
	// RedisAddr is where the routing projection lives. Read-only from here:
	// the gateway never writes a record, and the scheduler is the only process
	// that arbitrates one.
	RedisAddr           string        `json:"redis_addr"`
	RequestTimeout      time.Duration `json:"request_timeout"`
	SandboxProxyDomains []string      `json:"sandbox_proxy_domains"`
	// DebugMode enables debug-only behaviors in the gateway such as exposing
	// the backend node id on proxied responses. It is off by default.
	DebugMode bool                 `json:"debug_mode"`
	Routing   GatewayRoutingConfig `json:"routing"`
	// ControlPlaneToken is the shared secret the gateway stamps on every request
	// it forwards to a node, so the node can tell control-plane traffic from
	// anything that reached it another way.
	//
	// It is a credential, so it arrives through the environment or a Secret and
	// never through the config file, which is a ConfigMap — the same rule the
	// registry DSN follows, and the reason it is absent from the JSON shape
	// below rather than merely undocumented. Empty is an ordinary value: the
	// gateway stamps nothing and the node's gate stays open, which is what makes
	// the rollout config-driven instead of deploy-driven.
	ControlPlaneToken string `json:"-"`
}

func (g *GatewayConfig) UnmarshalJSON(data []byte) error {
	type wire struct {
		HTTPListenAddr      *string         `json:"http_listen_addr"`
		MetricsListenAddr   *string         `json:"metrics_listen_addr"`
		SchedulerAddr       *string         `json:"scheduler_addr"`
		RedisAddr           *string         `json:"redis_addr"`
		RequestTimeout      json.RawMessage `json:"request_timeout"`
		SandboxProxyDomains *[]string       `json:"sandbox_proxy_domains"`
		DebugMode           *bool           `json:"debug_mode"`
		// Nested one pointer deep on each side, so a config file that names the
		// block without naming the key inside it leaves the default alone rather
		// than blanking it.
		Routing *struct {
			ExecutionFencing *string `json:"execution_fencing"`
			ProjectionRead   *bool   `json:"projection_read"`
		} `json:"routing"`
	}

	parsed := wire{}
	if err := json.Unmarshal(data, &parsed); err != nil {
		return err
	}

	if parsed.HTTPListenAddr != nil {
		g.HTTPListenAddr = *parsed.HTTPListenAddr
	}
	if parsed.MetricsListenAddr != nil {
		g.MetricsListenAddr = *parsed.MetricsListenAddr
	}
	if parsed.SchedulerAddr != nil {
		g.SchedulerAddr = *parsed.SchedulerAddr
	}
	if parsed.RedisAddr != nil {
		g.RedisAddr = *parsed.RedisAddr
	}
	if parsed.SandboxProxyDomains != nil {
		g.SandboxProxyDomains = *parsed.SandboxProxyDomains
	}
	if parsed.DebugMode != nil {
		g.DebugMode = *parsed.DebugMode
	}
	if parsed.Routing != nil && parsed.Routing.ExecutionFencing != nil {
		// Carried through verbatim rather than parsed here: an unrecognised
		// value has to reach validate() and stop the process, and parsing at
		// this depth would turn it into an unmarshal error whose message names
		// the JSON rather than the setting.
		g.Routing.ExecutionFencing = GatewayExecutionFencing(*parsed.Routing.ExecutionFencing)
	}
	if parsed.Routing != nil && parsed.Routing.ProjectionRead != nil {
		g.Routing.ProjectionRead = *parsed.Routing.ProjectionRead
	}

	if len(bytes.TrimSpace(parsed.RequestTimeout)) > 0 {
		d, err := parseGatewayRequestTimeout(parsed.RequestTimeout)
		if err != nil {
			return err
		}
		g.RequestTimeout = d
	}
	return nil
}

func parseGatewayRequestTimeout(raw json.RawMessage) (time.Duration, error) {
	return parseGatewayDuration("gateway.request_timeout", raw)
}

// parseGatewayDuration parses a gateway duration field carried through
// UnmarshalJSON as json.RawMessage, so a config file may write a bare
// duration string ("30s") and a numeric value is rejected with a message
// naming the field rather than becoming a silently-wrong nanosecond count.
func parseGatewayDuration(field string, raw json.RawMessage) (time.Duration, error) {
	var asString string
	if err := json.Unmarshal(raw, &asString); err == nil {
		d, parseErr := time.ParseDuration(strings.TrimSpace(asString))
		if parseErr != nil {
			return 0, fmt.Errorf("%s must be a duration string like \"30s\": %w", field, parseErr)
		}
		return d, nil
	}

	var asNumber json.Number
	if err := json.Unmarshal(raw, &asNumber); err == nil {
		return 0, fmt.Errorf("%s must be a duration string like \"30s\", got numeric value %s", field, asNumber.String())
	}

	return 0, fmt.Errorf("%s must be a duration string like \"30s\"", field)
}

// Config is one process's configuration, and that process is the gateway.
//
// 🔴 There used to be a `Service` field, set from a string every caller passed
// as "gateway", and the whole gateway half of validate() was indented under
// `if c.Service == "gateway"`. It existed while this module shipped two
// binaries: `services/scheduler` loaded the same struct and skipped that block.
// The scheduler is deleted, `services/gateway/cmd/main.go` is the only non-test
// caller left, and a discriminator with one value is a branch that cannot be
// exercised — so the field is gone and the gateway block runs unconditionally.
//
// Dropping the `service` JSON tag is not a breaking change for a deployed
// manifest: `json.Unmarshal` ignores a key no field claims, so a ConfigMap that
// still carries `"service": "gateway"` loads exactly as it did.
// `TestAConfigStillNamingItsServiceLoads` pins that rather than trusting it.
type Config struct {
	LogLevel  string        `json:"log_level"`
	LogFormat string        `json:"log_format"`
	Gateway   GatewayConfig `json:"gateway"`
}

func Load(path string) (Config, error) {
	return load(path)
}

func load(path string) (Config, error) {
	cfg := defaultConfig()
	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return Config{}, fmt.Errorf("read config file: %w", err)
		}
		if err := json.Unmarshal(data, &cfg); err != nil {
			return Config{}, fmt.Errorf("unmarshal config json: %w", err)
		}
	}
	if err := overrideWithEnv(&cfg); err != nil {
		return Config{}, err
	}
	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func defaultConfig() Config {
	return Config{
		LogLevel:  "info",
		LogFormat: "auto",
		Gateway: GatewayConfig{
			HTTPListenAddr:      ":8080",
			MetricsListenAddr:   ":9102",
			SchedulerAddr:       "127.0.0.1:9090",
			RequestTimeout:      30 * time.Second,
			SandboxProxyDomains: []string{},
			// The default points at the end state rather than at the cautious
			// first step. Starting a release on observe is release discipline,
			// which belongs in the runbook; putting it in the default leaves
			// clusters parked there with nobody aware they were never flipped.
			Routing: GatewayRoutingConfig{ExecutionFencing: GatewayExecutionFencingEnforce},
		},
	}
}

func overrideWithEnv(cfg *Config) error {
	set := func(key string, target *string) {
		if v := strings.TrimSpace(os.Getenv(key)); v != "" {
			*target = v
		}
	}
	set("LOG_LEVEL", &cfg.LogLevel)
	set("LOG_FORMAT", &cfg.LogFormat)
	set("GATEWAY_HTTP_LISTEN_ADDR", &cfg.Gateway.HTTPListenAddr)
	set("GATEWAY_METRICS_LISTEN_ADDR", &cfg.Gateway.MetricsListenAddr)
	set("GATEWAY_SCHEDULER_ADDR", &cfg.Gateway.SchedulerAddr)
	set("GATEWAY_REDIS_ADDR", &cfg.Gateway.RedisAddr)
	// A shared secret, so it arrives the same way the DSN does and never through
	// the ConfigMap.
	set("GATEWAY_CONTROL_PLANE_TOKEN", &cfg.Gateway.ControlPlaneToken)
	if v := strings.TrimSpace(os.Getenv("GATEWAY_SANDBOX_PROXY_DOMAINS")); v != "" {
		cfg.Gateway.SandboxProxyDomains = splitCommaSeparated(v)
	}

	if v := strings.TrimSpace(os.Getenv("GATEWAY_REQUEST_TIMEOUT")); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("invalid GATEWAY_REQUEST_TIMEOUT %q: %w", v, err)
		}
		cfg.Gateway.RequestTimeout = d
	}

	if v := strings.TrimSpace(os.Getenv("GATEWAY_DEBUG_MODE")); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return fmt.Errorf("invalid GATEWAY_DEBUG_MODE %q: %w", v, err)
		}
		cfg.Gateway.DebugMode = b
	}

	if v := strings.TrimSpace(os.Getenv("GATEWAY_ROUTING_EXECUTION_FENCING")); v != "" {
		mode, err := ParseGatewayExecutionFencing(v)
		if err != nil {
			return fmt.Errorf("invalid GATEWAY_ROUTING_EXECUTION_FENCING: %w", err)
		}
		cfg.Gateway.Routing.ExecutionFencing = mode
	}

	// 🔴 The projection switch arrives as an environment variable and never
	// as a mounted file. The two are not interchangeable here: a mounted
	// value that cannot be read is deliberately held at its last good value by
	// the consumer that reads one, and kubelet refreshes volumes minutes apart
	// and unevenly across nodes — so deleting a key and writing an empty string
	// have opposite effects and the failing side is silent. A configMapKeyRef
	// is read once at start-up: deleting the key falls back to the code
	// default, writing an empty string leaves the value alone, and neither
	// takes effect until the pod restarts. Flipping one is therefore `kubectl
	// set env`, which rolls the deployment and is loud.
	if v := strings.TrimSpace(os.Getenv("GATEWAY_ROUTING_PROJECTION_READ")); v != "" {
		on, err := ParseRoutingProjectionSwitch(v)
		if err != nil {
			return fmt.Errorf("invalid GATEWAY_ROUTING_PROJECTION_READ: %w", err)
		}
		cfg.Gateway.Routing.ProjectionRead = on
	}

	return nil
}

func splitCommaSeparated(raw string) []string {
	parts := strings.Split(raw, ",")
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			values = append(values, part)
		}
	}
	return values
}

func (c *Config) applyDefaults() {
	if strings.TrimSpace(c.Gateway.MetricsListenAddr) == "" {
		c.Gateway.MetricsListenAddr = ":9102"
	}
	// An explicitly empty value in the config file means the same thing as the
	// key being absent. Anything else is left alone for validate() to refuse:
	// defaulting a value it could not read would be the silent fallback this
	// setting must not have.
	if strings.TrimSpace(string(c.Gateway.Routing.ExecutionFencing)) == "" {
		c.Gateway.Routing.ExecutionFencing = GatewayExecutionFencingEnforce
	}
}

func (c Config) Validate() error {
	return c.validate()
}

// validate refuses a configuration this process cannot serve.
//
// 🔴 Everything below the log settings used to sit inside `if c.Service ==
// "gateway"`, and there was a `c.Service == ""` refusal above it. Both are
// gone with the field: this module ships one binary, so the gateway checks are
// this loader's checks. Re-introducing a discriminator would silently switch
// the whole block off for any value that is not the one string it compared
// against — which is exactly the failure an un-migrated manifest would have
// produced, had `service` ever been read back off the file rather than passed
// in by the caller.
func (c Config) validate() error {
	if c.LogLevel == "" {
		return errors.New("log_level is required")
	}
	if c.LogFormat == "" {
		return errors.New("log_format is required")
	}
	switch strings.ToLower(c.LogFormat) {
	case "auto", "console", "json":
	default:
		return errors.New("log_format must be one of auto, console, json")
	}
	if c.Gateway.HTTPListenAddr == "" {
		return errors.New("gateway.http_listen_addr is required")
	}
	if c.Gateway.MetricsListenAddr == "" {
		return errors.New("gateway.metrics_listen_addr is required")
	}
	if c.Gateway.SchedulerAddr == "" {
		return errors.New("gateway.scheduler_addr is required")
	}
	if _, err := ParseGatewayExecutionFencing(string(c.Gateway.Routing.ExecutionFencing)); err != nil {
		return err
	}
	// 🔴 Refused rather than quietly ignored. A read switch with nowhere to
	// read from is a switch that reports as on and does nothing, and the
	// symptom — every request still going to the scheduler — is exactly
	// what the switch being off looks like.
	if c.Gateway.Routing.ProjectionRead && strings.TrimSpace(c.Gateway.RedisAddr) == "" {
		return errors.New("gateway.routing.projection_read requires gateway.redis_addr")
	}
	return nil
}
