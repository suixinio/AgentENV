package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// defaultGatewaySchedulerFallbackTimeout bounds the query-only-scheduler
// LookupNode call a projection miss and an undecided wake-up both fall
// through to (services/gateway/internal/server.go's lookupNodeFallback).
//
// 🔴 Mirrors gateway.defaultSchedulerFallbackTimeout, the value NewServer
// falls back to when a caller constructs ServerOptions directly (every test,
// and any embedder that does not go through config.Load). This package
// cannot import the gateway package to share one constant, so the two are
// declared independently and must be kept equal by hand — if they drift,
// config.Load's callers see one value and a caller building ServerOptions
// directly sees the other.
const defaultGatewaySchedulerFallbackTimeout = 3 * time.Second

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

// ParseRestUpstream turns the configured REST upstream into the base URL the
// gateway forwards to, or reports that there is none.
//
// The empty string — the switch off — is not an error: it answers "" with a nil
// error, and the caller reads that as "fan out to the nodes, as before".
//
// 🔴 Anything else must be a usable absolute address or the process stops. A
// REST upstream that cannot be parsed is not a degraded upstream, it is a
// gateway that answers every sandbox create with a 502 while reporting the
// switch as on — and "the switch is on and nothing works" is the state an
// operator will spend the incident staring at the api half for. A bare
// `host:port` is accepted and read as http, because that is the shape every
// other address in this config file has and an operator copying the style of
// `scheduler_addr` should not get a refusal for it.
func ParseRestUpstream(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", nil
	}

	candidate := trimmed
	if !strings.Contains(candidate, "://") {
		candidate = "http://" + candidate
	}
	parsed, err := url.Parse(candidate)
	if err != nil {
		return "", fmt.Errorf("gateway.rest_upstream_addr %q is not an address: %w", raw, err)
	}
	switch parsed.Scheme {
	case "http", "https":
	default:
		return "", fmt.Errorf("gateway.rest_upstream_addr %q must be http or https, got scheme %q", raw, parsed.Scheme)
	}
	if parsed.Host == "" {
		return "", fmt.Errorf("gateway.rest_upstream_addr %q names no host", raw)
	}
	// 🔴 A path would be silently prepended to every forwarded route, so a
	// value like `http://agentenv-api:8000/` — which an operator pasting from a
	// browser produces without thinking — must not turn `/sandboxes` into
	// `//sandboxes`. A bare "/" is the one path that means nothing, so it is
	// dropped rather than refused; anything longer is a real prefix and this
	// switch does not carry one.
	if path := strings.Trim(parsed.Path, "/"); path != "" {
		return "", fmt.Errorf("gateway.rest_upstream_addr %q must not carry a path (%q)", raw, parsed.Path)
	}
	parsed.Path = ""
	parsed.RawPath = ""
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String(), nil
}

// GatewayRoutingConfig groups the switches over what the gateway does with a
// routing answer, as opposed to where it listens or who it talks to.
type GatewayRoutingConfig struct {
	ExecutionFencing GatewayExecutionFencing `json:"execution_fencing"`
	// ProjectionRead lets the gateway answer a sandbox route from the routing
	// projection directly, falling back to the scheduler on a miss or an error.
	// Off leaves every request going through LookupNode, which is what shipped
	// before this existed.
	//
	// 🔴 Turning this on must be paired with whatever now answers LookupNode
	// staying in its enforcing mode. That used to be the Go scheduler's
	// routing.execution_arbitration; the equivalent on aenv-api's own
	// in-process registry (`[cluster].node_placement_source = "native"`) is
	// `[binding_store].arbitration` (src/cfg.rs) — its rollback works by
	// blanking the two incarnation fields on the way out of LookupNode, and a
	// gateway reading the projection itself never sees that blanking, so it
	// would go on fencing against incarnations arbitration has stopped
	// judging. There is no mechanism for it — the pairing is an operational
	// rule, written here because this is where somebody reads it.
	ProjectionRead bool `json:"projection_read"`
	// ProjectionAuthoritative is the gateway's half of the write-side switch:
	// it makes resume and connect record an assignment, and makes the gateway
	// forward the incarnation and TTL a node reports. Off means it forwards
	// neither, which is a projection write identical to today's.
	//
	// 🔴 The write side is one logical switch across two processes and each
	// holds half of it. Neither ordering is unsafe — see the note in the stage
	// plan — so they do not have to be flipped together.
	ProjectionAuthoritative bool `json:"projection_authoritative"`
}

type GatewayConfig struct {
	HTTPListenAddr         string `json:"http_listen_addr"`
	MetricsListenAddr      string `json:"metrics_listen_addr"`
	SchedulerAddr          string `json:"scheduler_addr"`
	QueryOnlySchedulerAddr string `json:"query_only_scheduler_addr"`
	// ResumeAddr is the api half's wake-up surface, asked when the routing
	// projection has no answer for a sandbox.
	//
	// 🔴 No longer optional. Empty used to be the switch off — every
	// projection miss fell through to the scheduler, and the node the request
	// landed on woke the sandbox itself — while 阶段 3a's rollback was a
	// ConfigMap change and a gateway restart rather than a DaemonSet roll
	// (`_sd-impl-phase3-role.md` §11.1, §11.2). That premise is retired: nodes
	// run `aenv-node` now and have no wake-up surface of their own under any
	// configuration, so `Validate` refuses a config with this empty rather
	// than letting a cluster discover it as every resume attempt silently
	// falling back to the scheduler. See rest_upstream.go and
	// `TestTheRestUpstreamIsAlwaysSetBecauseNodesNeverServeRest`.
	ResumeAddr string `json:"resume_addr"`
	// RestUpstreamAddr is where user-facing REST goes: sandbox, snapshot and
	// template calls, the routes `aenv-node` answers 404 on.
	//
	// 🔴 No longer optional, for the same reason as ResumeAddr above. Empty
	// used to be the switch off — the gateway asked the scheduler which node
	// should serve the call and forwarded it there, because the nodes were
	// still the pre-split single process and never stopped being able to
	// serve it (`_sd-impl-phase3-role.md` §11.1, §11.2). That premise is
	// retired: nodes run `aenv-node` now and answer 404 on every user-facing
	// REST route under any configuration, so `Validate` refuses a config with
	// this empty rather than letting a cluster discover it as REST 404s. Set —
	// `http://agentenv-api:8000`, or a bare `agentenv-api:8000` which is read
	// as http — the calls go to the api half, which owns sandboxes and drives
	// the machines itself. See rest_upstream.go and
	// `TestTheRestUpstreamIsAlwaysSetBecauseNodesNeverServeRest`.
	//
	// 🔴 Never carries data-plane traffic. A request routed by proxy headers or
	// by a sandbox proxy domain goes to the node holding the sandbox whatever
	// this says: the api half runs no sandboxes and has nothing to proxy to.
	RestUpstreamAddr string `json:"rest_upstream_addr"`
	// RedisAddr is where the routing projection lives. Read-only from here:
	// the gateway never writes a record, and the scheduler is the only process
	// that arbitrates one.
	RedisAddr           string        `json:"redis_addr"`
	RequestTimeout      time.Duration `json:"request_timeout"`
	ForwardResponseSize int64         `json:"forward_response_size"`
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
	// SchedulerFallbackDisabled turns off the query-only-scheduler LookupNode
	// call that a projection miss and an undecided wake-up both fall through
	// to. False — the default, and today's behaviour exactly — still asks the
	// scheduler on that cold path.
	//
	// 🔴 Phase 4's decommissioning lever for that one call, in the same shape
	// as ResumeAddr and RestUpstreamAddr above: a ConfigMap edit and a
	// restart, not a code change. See gateway/internal/server.go's
	// ServerOptions.SchedulerFallbackDisabled.
	SchedulerFallbackDisabled bool `json:"scheduler_fallback_disabled"`
	// SchedulerFallbackTimeout bounds that same call on its own, separately
	// from RequestTimeout, so a scheduler that is merely unreachable cannot
	// hold it open for whatever of the request's overall budget is left. Zero
	// (the default when unset) is resolved to a fixed fallback inside
	// gateway.NewServer, never left as "no timeout".
	SchedulerFallbackTimeout time.Duration `json:"scheduler_fallback_timeout"`
}

func (g *GatewayConfig) UnmarshalJSON(data []byte) error {
	type wire struct {
		HTTPListenAddr         *string         `json:"http_listen_addr"`
		MetricsListenAddr      *string         `json:"metrics_listen_addr"`
		SchedulerAddr          *string         `json:"scheduler_addr"`
		QueryOnlySchedulerAddr *string         `json:"query_only_scheduler_addr"`
		ResumeAddr             *string         `json:"resume_addr"`
		RestUpstreamAddr       *string         `json:"rest_upstream_addr"`
		RedisAddr              *string         `json:"redis_addr"`
		RequestTimeout         json.RawMessage `json:"request_timeout"`
		ForwardResponseSize    *int64          `json:"forward_response_size"`
		SandboxProxyDomains    *[]string       `json:"sandbox_proxy_domains"`
		DebugMode              *bool           `json:"debug_mode"`
		// Nested one pointer deep on each side, so a config file that names the
		// block without naming the key inside it leaves the default alone rather
		// than blanking it.
		Routing *struct {
			ExecutionFencing        *string `json:"execution_fencing"`
			ProjectionRead          *bool   `json:"projection_read"`
			ProjectionAuthoritative *bool   `json:"projection_authoritative"`
		} `json:"routing"`
		SchedulerFallbackDisabled *bool           `json:"scheduler_fallback_disabled"`
		SchedulerFallbackTimeout  json.RawMessage `json:"scheduler_fallback_timeout"`
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
	if parsed.QueryOnlySchedulerAddr != nil {
		g.QueryOnlySchedulerAddr = *parsed.QueryOnlySchedulerAddr
	}
	if parsed.ResumeAddr != nil {
		g.ResumeAddr = *parsed.ResumeAddr
	}
	if parsed.RestUpstreamAddr != nil {
		g.RestUpstreamAddr = *parsed.RestUpstreamAddr
	}
	if parsed.RedisAddr != nil {
		g.RedisAddr = *parsed.RedisAddr
	}
	if parsed.ForwardResponseSize != nil {
		g.ForwardResponseSize = *parsed.ForwardResponseSize
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
	if parsed.Routing != nil && parsed.Routing.ProjectionAuthoritative != nil {
		g.Routing.ProjectionAuthoritative = *parsed.Routing.ProjectionAuthoritative
	}

	if len(bytes.TrimSpace(parsed.RequestTimeout)) > 0 {
		d, err := parseGatewayRequestTimeout(parsed.RequestTimeout)
		if err != nil {
			return err
		}
		g.RequestTimeout = d
	}
	if parsed.SchedulerFallbackDisabled != nil {
		g.SchedulerFallbackDisabled = *parsed.SchedulerFallbackDisabled
	}
	if len(bytes.TrimSpace(parsed.SchedulerFallbackTimeout)) > 0 {
		d, err := parseGatewayDuration("gateway.scheduler_fallback_timeout", parsed.SchedulerFallbackTimeout)
		if err != nil {
			return err
		}
		g.SchedulerFallbackTimeout = d
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

type Config struct {
	Service   string        `json:"service"`
	LogLevel  string        `json:"log_level"`
	LogFormat string        `json:"log_format"`
	Gateway   GatewayConfig `json:"gateway"`
}

func Load(path string, service string) (Config, error) {
	return load(path, service)
}

func load(path string, service string) (Config, error) {
	cfg := defaultConfig(service)
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
	cfg.Service = service
	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func defaultConfig(service string) Config {
	return Config{
		Service:   service,
		LogLevel:  "info",
		LogFormat: "auto",
		Gateway: GatewayConfig{
			HTTPListenAddr:           ":8080",
			MetricsListenAddr:        ":9102",
			SchedulerAddr:            "127.0.0.1:9090",
			RequestTimeout:           30 * time.Second,
			ForwardResponseSize:      4 << 20,
			SandboxProxyDomains:      []string{},
			SchedulerFallbackTimeout: defaultGatewaySchedulerFallbackTimeout,
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
	set("GATEWAY_QUERY_ONLY_SCHEDULER_ADDR", &cfg.Gateway.QueryOnlySchedulerAddr)
	set("GATEWAY_RESUME_ADDR", &cfg.Gateway.ResumeAddr)
	set("GATEWAY_REST_UPSTREAM_ADDR", &cfg.Gateway.RestUpstreamAddr)
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

	// 🔴 Phase 4's decommissioning lever — see GatewayConfig.
	// SchedulerFallbackDisabled — so it follows GATEWAY_DEBUG_MODE's shape
	// rather than the mounted-file pattern the projection switches use below:
	// this one is meant to be flipped by `kubectl set env` and a restart, the
	// same way ResumeAddr and RestUpstreamAddr are.
	if v := strings.TrimSpace(os.Getenv("GATEWAY_SCHEDULER_FALLBACK_DISABLED")); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return fmt.Errorf("invalid GATEWAY_SCHEDULER_FALLBACK_DISABLED %q: %w", v, err)
		}
		cfg.Gateway.SchedulerFallbackDisabled = b
	}

	if v := strings.TrimSpace(os.Getenv("GATEWAY_SCHEDULER_FALLBACK_TIMEOUT")); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("invalid GATEWAY_SCHEDULER_FALLBACK_TIMEOUT %q: %w", v, err)
		}
		cfg.Gateway.SchedulerFallbackTimeout = d
	}

	if v := strings.TrimSpace(os.Getenv("GATEWAY_ROUTING_EXECUTION_FENCING")); v != "" {
		mode, err := ParseGatewayExecutionFencing(v)
		if err != nil {
			return fmt.Errorf("invalid GATEWAY_ROUTING_EXECUTION_FENCING: %w", err)
		}
		cfg.Gateway.Routing.ExecutionFencing = mode
	}

	// 🔴 The three projection switches arrive as environment variables and
	// never as a mounted file. The two are not interchangeable here: a mounted
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

	if v := strings.TrimSpace(os.Getenv("GATEWAY_ROUTING_PROJECTION_AUTHORITATIVE")); v != "" {
		on, err := ParseRoutingProjectionSwitch(v)
		if err != nil {
			return fmt.Errorf("invalid GATEWAY_ROUTING_PROJECTION_AUTHORITATIVE: %w", err)
		}
		cfg.Gateway.Routing.ProjectionAuthoritative = on
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

func (c Config) validate() error {
	if c.Service == "" {
		return errors.New("service is required")
	}
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
	if c.Service == "gateway" {
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
		// Same reasoning, one step earlier: a REST upstream nobody can parse
		// stops the process here rather than turning into a 502 per request.
		if _, err := ParseRestUpstream(c.Gateway.RestUpstreamAddr); err != nil {
			return err
		}
		// 🔴 阶段 3a no longer has an "off" position for either of its two
		// addresses. Nodes run aenv-node now and answer 404 on every
		// user-facing REST route and have no wake-up surface of their own, so
		// an empty rest_upstream_addr or resume_addr is not a rollback — it is
		// an outage with no matching half, and refusing it here turns that into
		// a startup failure instead of a 404/502 discovered per request. See
		// rest_upstream.go and TestTheRestUpstreamIsAlwaysSetBecauseNodesNeverServeRest.
		if strings.TrimSpace(c.Gateway.RestUpstreamAddr) == "" {
			return errors.New("gateway.rest_upstream_addr is required: aenv-node answers 404 on " +
				"user-facing REST, so the gateway has nowhere else to send it")
		}
		if strings.TrimSpace(c.Gateway.ResumeAddr) == "" {
			return errors.New("gateway.resume_addr is required: aenv-node has no wake-up surface " +
				"of its own, so the gateway has nowhere else to ask a paused sandbox to be woken")
		}
	}
	return nil
}
