package gateway

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"agentenv/services/gateway/internal/cors"
	"agentenv/services/gateway/internal/resume"
	"agentenv/services/shared/config"
	"agentenv/services/shared/routing"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	headerSandboxID    = "x-agentenv-sandbox-id"
	headerE2BSandboxID = "e2b-sandbox-id"
	headerTargetPort   = "x-agentenv-target-port"
	// The caller's envd access token, and the token a locked sandbox's clients
	// present. The gateway checks neither — it has no record to check against —
	// it forwards them to the half that does, which is the node's own proxy.
	// Whatever this file grows into, that stays true: a second place deciding
	// whether a request may reach a sandbox is a second place to get it wrong.
	headerEnvdAccessToken = "x-access-token"
	headerE2BTargetPort   = "e2b-sandbox-port"
	headerNodeID          = "x-agentenv-node-id"
)

type routeSource string

const (
	routeSourceHeader routeSource = "header"
	routeSourceHost   routeSource = "host"
	routeSourcePath   routeSource = "path"
	// The gateway answered out of itself rather than resolving anything.
	routeSourceGateway routeSource = "gateway"
)

type ServerOptions struct {
	RequestTimeout      time.Duration
	DebugMode           bool
	SandboxProxyDomains []string
	// ExecutionFencing is the raw configured mode. It is parsed in NewServer so
	// an unrecognised value stops the process rather than becoming a silently
	// chosen behaviour; the empty string is the absence of a setting and takes
	// the default.
	ExecutionFencing string
	// ControlPlaneToken is stamped on every request the gateway forwards to a
	// node. Empty means stamp nothing, which is the state the fleet runs in
	// until the node-side gate is turned on.
	ControlPlaneToken string
	// ProjectionReader answers a sandbox route out of the routing projection
	// before the api half is asked. Nil is the read switch in its off
	// position: every request naming a sandbox goes to ResumeClient.
	ProjectionReader projectionReader

	// ResumeClient asks the API half where a sandbox is running, waking it
	// when it is paused, whenever the routing projection has no answer. Nil
	// makes every projection miss fail with a 502.
	//
	// 🔴 阶段 3a used to be able to leave this nil in production: nodes were
	// still the pre-split single process and could wake a sandbox themselves,
	// so nil was a supported rollback lever (`_sd-impl-phase3-role.md` §11.2).
	// That premise is retired — `aenv-node` has no wake-up surface of its own —
	// and `cmd/main.go` builds this over the scheduler connection
	// unconditionally, so it can no longer construct a `*Server` with this
	// nil. It stays nil-able here purely
	// because this package's own tests use an unconfigured `*Server` as their
	// baseline fixture for exercising the data-plane routing paths, the
	// projection and this wake-up client.
	ResumeClient *resume.Client
}

type Server struct {
	logger         *zap.Logger
	httpClient     *http.Client
	requestTimeout time.Duration
	// debugMode, when true, enables debug-only behaviors such as exposing
	// the backend node id on proxied responses via the x-agentenv-node-id
	// header. Off by default; toggled via GatewayConfig.DebugMode.
	debugMode           bool
	sandboxProxyDomains []string
	// executionFencing is resolved once, at construction. Reading a string and
	// branching on it at each call site is how one of these switches ends up
	// meaning different things in different places.
	executionFencing  fencingMode
	controlPlaneToken string
	// projectionReader is nil when the read switch is off. Checked once per
	// request rather than being wrapped in a no-op implementation, so "the
	// switch is off" is a state a reader of this code can see.
	projectionReader projectionReader
	// Nil when no wake-up endpoint is configured — no longer reachable from a
	// validated deployment, see ServerOptions.ResumeClient.
	resumeClient *resume.Client
}

func NewServer(logger *zap.Logger, options ServerOptions) (*Server, error) {
	sandboxProxyDomains, err := normalizeProxyDomains(options.SandboxProxyDomains)
	if err != nil {
		return nil, err
	}

	executionFencing, err := config.ParseGatewayExecutionFencing(options.ExecutionFencing)
	if err != nil {
		return nil, err
	}

	return &Server{
		logger:              logger,
		httpClient:          &http.Client{},
		requestTimeout:      options.RequestTimeout,
		debugMode:           options.DebugMode,
		sandboxProxyDomains: sandboxProxyDomains,
		executionFencing:    executionFencing,
		controlPlaneToken:   strings.TrimSpace(options.ControlPlaneToken),
		projectionReader:    options.ProjectionReader,
		resumeClient:        options.ResumeClient,
	}, nil
}

func (s *Server) SandboxProxyDomains() []string {
	return s.sandboxProxyDomains
}

func (s *Server) Handler() http.Handler {
	// We avoid http.ServeMux because it normalizes request paths (e.g.
	// decoding %2F → / and issuing 301 redirects), which breaks proxy
	// forwarding of percent-encoded path segments such as /files/%2F.
	core := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" || r.URL.Path == "/metrics" {
			hostRoute, hostRouteErr := parseHostRoute(r.Host, s.sandboxProxyDomains)
			if hostRoute != nil || hostRouteErr != nil {
				s.handleProxy(w, r)
				return
			}
			if hasProxyRoutingHeaders(r.Header) {
				if _, hasSandbox := sandboxIDFromHeaders(r.Header); !hasSandbox {
					setGatewayRouteSource(w, routeSourceHeader)
					cors.Fail(w, r, "sandbox id header required", http.StatusBadRequest)
					return
				}
				s.handleProxy(w, r)
				return
			}
			if r.URL.Path == "/health" {
				// Keep load balancer health checks local when they are not sandbox-routed.
				w.WriteHeader(http.StatusNoContent)
			} else {
				// Gateway Prometheus metrics use the separate metrics listener. Keep
				// this path unavailable on the public HTTP listener unless it is
				// explicitly routed to a sandbox.
				cors.NotFound(w, r)
			}
			return
		}
		s.handleProxy(w, r)
	})
	return s.instrumentGatewayHTTP(core)
}

func (s *Server) handleProxy(w http.ResponseWriter, r *http.Request) {
	websocket := isWebSocketRequest(r)
	streaming := isStreamingRequest(r)
	longLived := streaming || websocket
	routingCtx, cancelRouting := context.WithTimeout(r.Context(), s.requestTimeout)
	defer cancelRouting()

	hostRoute, hostRouteErr := parseHostRoute(r.Host, s.sandboxProxyDomains)
	if hostRouteErr != nil {
		setGatewayRouteSource(w, routeSourceHost)
		s.logger.Debug("host routing rejected",
			zap.String("host", r.Host),
			zap.Error(hostRouteErr),
			zap.Int("status", http.StatusBadRequest),
		)
		cors.Fail(w, r, hostRouteErr.Error(), http.StatusBadRequest)
		return
	}

	// Nothing addresses a sandbox: no proxy host name and no routing headers.
	// The gateway carries the sandbox data plane and nothing else — user-facing
	// REST has its own address, on the api half — so there is no upstream here
	// to hand this to.
	if hostRoute == nil && !hasProxyRoutingHeaders(r.Header) {
		setGatewayRouteSource(w, routeSourceGateway)
		// Nobody upstream can answer the preflight, and a browser drops the
		// real request unless the preflight gets a 2xx.
		if cors.HandlePreflight(w, r) {
			return
		}
		cors.NotFound(w, r)
		return
	}

	sandboxID, hasSandbox := "", false
	routeSource := routeSourceHeader
	if hostRoute != nil {
		s.logHostRoutingHeaderConflicts(r, hostRoute)
		sandboxID = hostRoute.sandboxID
		hasSandbox = true
		routeSource = routeSourceHost
	} else if isSandboxControlPlaneRequest(r) {
		sandboxID, hasSandbox = sandboxIDFromPath(r.URL.Path)
		routeSource = routeSourcePath
	} else {
		sandboxID, hasSandbox = sandboxIDFromHeaders(r.Header)
	}
	setGatewayRouteSource(w, routeSource)
	if !hasSandbox {
		// Only the header route lands here: a proxy host always carries an id
		// and isSandboxControlPlaneRequest refuses a blank path segment. Routing
		// headers naming no sandbox is the shape the /health branch refuses
		// too, and there is no upstream to hand it to.
		cors.Fail(w, r, "sandbox id header required", http.StatusBadRequest)
		return
	}

	// The two ways a sandbox route gets answered, in order: the routing
	// projection, then the api half's resume RPC. A hit and a woken sandbox
	// are rendered in one shape — routing.Synthesize and resume.Result.Answer
	// both answer Bound — so nothing below this block needs to know which of
	// the two answered.
	source := routeResolutionRedisHit
	answer, resolved := s.resolveFromProjection(routingCtx, sandboxID)
	if !resolved {
		// 🔴 Everything the projection could not answer lands here, and that
		// includes a read error. A miss is not an absence: the api half walks
		// the binding, then the heartbeat roster, then the snapshot catalog,
		// answers a running sandbox as it stands and wakes a paused one. Only
		// the half that owns sandboxes can tell those apart, and it is the
		// only thing asked: a verdict that is not "woken" ends the request
		// here, with no second opinion from anywhere else. An api half that
		// cannot be asked is a 502, never a 404.
		woke := s.resumeClient.Wake(routingCtx, resume.Request{
			SandboxID:       sandboxID,
			TargetPort:      resumeTargetPort(r, hostRoute),
			EnvdAccessToken: r.Header.Get(headerEnvdAccessToken),
		})
		recordResumeAttempt(woke)
		if woke.Verdict != resume.VerdictWoken {
			s.writeResumeError(w, r, sandboxID, woke)
			return
		}
		s.logger.Info("the api half located the sandbox",
			zap.String("sandbox_id", sandboxID),
			zap.String("node_id", woke.NodeID),
			zap.String("execution_id", woke.ExecutionID),
		)
		answer = woke.Answer()
		source = routeResolutionResumeWoken
	}
	recordRouteResolution(source)
	node := answer.Node
	location := answer.Location
	recordGatewaySandboxLocation(location)
	// Decided once, from the one routing answer, and carried to both ends of
	// the proxied exchange.
	plane := fencingPlaneFor(routeSource)
	fencing := decideFencing(s.executionFencing, plane, answer)
	recordExecutionFencing(plane, fencing.decision)

	s.logger.Debug("gateway routed request",
		zap.String("method", r.Method),
		zap.String("path", r.URL.Path),
		zap.String("route_source", string(routeSource)),
		zap.String("location", gatewaySandboxLocationLabel(location)),
		zap.String("sandbox_id", sandboxID),
		zap.String("node_id", node.ID),
		zap.String("upstream_endpoint", node.Endpoint),
		// The control plane resolves an incarnation and never acts on it, so
		// this line is the only place it is visible for those requests.
		zap.String("expected_execution_id", fencing.expect),
		zap.String("execution_authority", fencing.authority.String()),
		zap.String("fencing_stage", fencingStageGatewayRoute),
		zap.String("route_resolution", source),
	)

	decodedPath := upstreamTargetPath(routeSource, r.URL.Path)
	escapedPath := upstreamTargetEscapedPath(routeSource, requestEscapedPath(r))
	upstreamURL, err := joinUpstream(node.Endpoint, decodedPath, escapedPath, r.URL.RawQuery)
	if err != nil {
		cors.Fail(w, r, "invalid upstream endpoint", http.StatusBadGateway)
		return
	}

	upstreamCtx, cancelUpstream := requestContextForProxy(r, routingCtx, longLived)
	defer cancelUpstream()

	s.proxyRequest(
		w,
		r.Clone(upstreamCtx),
		upstreamURL,
		node,
		proxyRequestOptions{
			hostRoute:        hostRoute,
			flushImmediately: longLived,
			sandboxID:        sandboxID,
			fencing:          fencing,
		},
	)
}

// schedulerUnreachable is what a caller is told when the RPC never reached a
// scheduler at all.
const schedulerUnreachable = "the scheduler could not be reached"

// transportNoise is the vocabulary of a gRPC client that never got an answer.
//
// 🔴 A status with one of these in it was written by the *client*, not by the
// scheduler: nothing on the scheduler side has any reason to say "dial tcp" or
// "transport:". Measured on the dev cluster with the scheduler scaled to zero,
// POST /v3/templates answered 503 — correctly — with
// `dial tcp 10.43.165.31:9090: connect: connection refused` as the body.
var transportNoise = []string{
	"connection error",
	"transport:",
	"dial tcp",
	"dial udp",
	"dial unix",
	"connection refused",
	"connection reset",
	"broken pipe",
	"no such host",
	"name resolver error",
	"produced zero addresses",
	"i/o timeout",
	"context deadline exceeded",
	"the client connection is closing",
	"grpc: the connection is unavailable",
}

// hostPort matches anything shaped like a network endpoint: `10.43.165.31:9090`,
// `scheduler.default.svc:9090`, `[::1]:9090`.
var hostPort = regexp.MustCompile(`(?:\[[0-9A-Fa-f:.]+\]|[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?):[0-9]{1,5}\b`)

// schedulerReason is what of the scheduler's answer a caller may see.
//
// 🔴 The scheduler's message is deliberately forwarded — `sandbox is being
// resumed by node "node-a"` is a reason a client acts on, and the tests above
// pin the forwarding. What must not be forwarded is the text a
// failed *dial* produces, because it is not the scheduler speaking and it names
// the cluster's internal addressing to whoever asked.
//
// Two layers, and the second is the one that matters. The first recognises the
// shapes grpc-go produces today. The second redacts anything that looks like a
// network endpoint whatever wrapped it, so a phrasing this list has not seen —
// a future grpc-go, a proxy, a service mesh — still cannot put an address in a
// response body.
//
// The raw text is logged, so nothing is lost to the operator.
func (s *Server) schedulerReason(st *status.Status) string {
	message := strings.TrimSpace(st.Message())
	if message == "" {
		return schedulerUnreachable
	}
	lowered := strings.ToLower(message)
	for _, noise := range transportNoise {
		if strings.Contains(lowered, noise) {
			s.logger.Warn("the scheduler RPC did not reach a scheduler",
				zap.String("code", st.Code().String()),
				zap.String("scheduler_error", message),
			)
			return schedulerUnreachable
		}
	}
	redacted := hostPort.ReplaceAllString(message, "[redacted]")
	if redacted != message {
		s.logger.Warn("redacted an endpoint from a scheduler error before answering",
			zap.String("code", st.Code().String()),
			zap.String("scheduler_error", message),
		)
	}
	return redacted
}

type proxyRequestOptions struct {
	hostRoute        *hostRoute
	flushImmediately bool
	// sandboxID is the sandbox this exchange was routed for, empty when the
	// request was scheduled rather than resolved. It appears in refusals.
	sandboxID string
	// fencing is the plan decided from the lookup answer. The zero value stamps
	// nothing and reads nothing back, which is what the paths with no sandbox
	// and the node admin surface both get.
	fencing fencingPlan
}

// proxyRequest forwards the request to one node.
func (s *Server) proxyRequest(
	w http.ResponseWriter,
	proxyReq *http.Request,
	target string,
	node routing.Node,
	options proxyRequestOptions,
) {
	upstreamURL, err := url.Parse(target)
	if err != nil {
		cors.Fail(w, proxyReq, "invalid upstream endpoint", http.StatusBadGateway)
		return
	}

	proxy := &httputil.ReverseProxy{
		Rewrite: func(req *httputil.ProxyRequest) {
			req.Out.URL.Scheme = upstreamURL.Scheme
			req.Out.URL.Host = upstreamURL.Host
			req.Out.URL.Path = upstreamURL.Path
			req.Out.URL.RawPath = upstreamURL.RawPath
			req.Out.URL.RawQuery = upstreamURL.RawQuery
			req.Out.Host = req.In.Host
			injectForwardedHeaders(req.Out.Header, req.In)
			// 🔴 After the client's own headers have been copied, and
			// unconditionally: these two header names are the gateway's to
			// assert, and leaving an inbound one in place would make the gateway
			// a passthrough for a forged control-plane identity or a forged
			// fencing token.
			s.stampOutboundGatewayHeaders(req.Out.Header, options.fencing.stampedExecutionID())
			if options.hostRoute != nil {
				req.Out.Header.Set(headerSandboxID, options.hostRoute.sandboxID)
				req.Out.Header.Set(headerTargetPort, strconv.Itoa(options.hostRoute.targetPort))
			}
		},
		FlushInterval: flushInterval(options.flushImmediately),
		ModifyResponse: func(resp *http.Response) error {
			// In debug mode, expose the upstream node id on the response so
			// operators can tell which backend node served a given request.
			// This is purely for debugging/observability and is not consumed
			// by the client.
			if s.debugMode {
				if nodeID := node.ID; nodeID != "" {
					resp.Header.Set(headerNodeID, nodeID)
				}
			}
			return s.fenceProxyResponse(options.fencing, options.sandboxID, node, resp)
		},
		ErrorHandler: func(rw http.ResponseWriter, _ *http.Request, err error) {
			if errors.Is(err, context.Canceled) {
				logLevel := zap.WarnLevel
				if isStreamInputProxyRequest(proxyReq) {
					logLevel = zap.DebugLevel
				}
				s.logger.Log(logLevel, "proxy request closed by client",
					zap.Error(err),
					zap.String("node", node.ID),
					zap.String("path", proxyReq.URL.Path),
					zap.String("target", upstreamURL.String()),
				)
				return
			}

			if errors.Is(err, context.DeadlineExceeded) || errors.Is(proxyReq.Context().Err(), context.DeadlineExceeded) {
				s.logger.Warn("proxy request timed out",
					zap.Error(err),
					zap.String("node", node.ID),
					zap.String("path", proxyReq.URL.Path),
					zap.String("target", upstreamURL.String()),
				)
				cors.Fail(rw, proxyReq, "upstream timeout", http.StatusGatewayTimeout)
				return
			}

			var proxyErr *proxyResponseError
			if errors.As(err, &proxyErr) {
				proxyErr.write(rw, proxyReq)
				return
			}

			s.logger.Warn("proxy request failed",
				zap.Error(err),
				zap.String("node", node.ID),
				zap.String("path", proxyReq.URL.Path),
				zap.String("target", upstreamURL.String()),
			)
			cors.Fail(rw, proxyReq, "upstream unavailable", http.StatusBadGateway)
		},
	}

	proxyStart := time.Now()
	route := gatewayRouteLabel(proxyReq.URL.Path)
	proxy.ServeHTTP(w, proxyReq)
	recordGatewayUpstreamProxy(route, proxyStart, w, proxyReq.Context())
}

func isStreamInputProxyRequest(r *http.Request) bool {
	return r.Method == http.MethodPost && r.URL.Path == "/process.Process/StreamInput"
}

// proxyResponseError is how ModifyResponse ends a proxied exchange on its own
// terms. Returning it makes the reverse proxy close the upstream body and hand
// the error to ErrorHandler, so the upstream's own response never reaches the
// client — including for a 101, which the upgrade path would otherwise have
// taken over before anything written in place could matter.
type proxyResponseError struct {
	statusCode int
	message    string
	cause      error
	// contentType, body and headers carry a structured refusal. When body is
	// empty the error falls back to http.Error's plain text, which is what every
	// pre-existing caller wants.
	contentType string
	body        []byte
	headers     http.Header
}

// write emits the error as a response. The upstream's answer was discarded,
// so a preflight is the gateway's to answer.
func (e *proxyResponseError) write(rw http.ResponseWriter, r *http.Request) {
	if cors.HandlePreflight(rw, r) {
		return
	}
	if len(e.body) == 0 {
		cors.Error(rw, e.message, e.statusCode)
		return
	}
	cors.SetHeaders(rw)
	for name, values := range e.headers {
		for _, value := range values {
			rw.Header().Add(name, value)
		}
	}
	if e.contentType != "" {
		rw.Header().Set("Content-Type", e.contentType)
	}
	rw.Header().Set("Content-Length", strconv.Itoa(len(e.body)))
	rw.WriteHeader(e.statusCode)
	if _, err := rw.Write(e.body); err != nil {
		return
	}
}

func (e *proxyResponseError) Error() string {
	if e.cause != nil {
		return e.cause.Error()
	}
	return e.message
}

func flushInterval(flushImmediately bool) time.Duration {
	if flushImmediately {
		return -1
	}
	return 0
}

func sandboxIDFromHeaders(h http.Header) (string, bool) {
	for _, name := range []string{headerSandboxID, headerE2BSandboxID} {
		v := strings.TrimSpace(h.Get(name))
		if v != "" {
			return v, true
		}
	}
	return "", false
}

func hasProxyRoutingHeaders(h http.Header) bool {
	for _, name := range []string{
		headerSandboxID,
		headerE2BSandboxID,
		headerTargetPort,
		headerE2BTargetPort,
	} {
		if strings.TrimSpace(h.Get(name)) != "" {
			return true
		}
	}
	return false
}

func targetPortFromHeaders(h http.Header) (string, bool) {
	for _, name := range []string{headerTargetPort, headerE2BTargetPort} {
		v := strings.TrimSpace(h.Get(name))
		if v != "" {
			return v, true
		}
	}
	return "", false
}

func sandboxIDFromPath(path string) (string, bool) {
	const marker = "/sandboxes/"
	rest, found := strings.CutPrefix(path, marker)
	if !found {
		_, rest, found = strings.Cut(path, marker)
	}
	if !found {
		return "", false
	}
	rest = strings.TrimSpace(rest)
	if rest == "" {
		return "", false
	}
	if id, _, hasSlash := strings.Cut(rest, "/"); hasSlash {
		rest = id
	}
	rest = strings.TrimSpace(rest)
	if rest == "" {
		return "", false
	}
	return rest, true
}

func isSandboxControlPlaneRequest(r *http.Request) bool {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 2 || parts[0] != "sandboxes" || strings.TrimSpace(parts[1]) == "" {
		return false
	}

	if len(parts) == 2 {
		return r.Method == http.MethodGet || r.Method == http.MethodDelete
	}
	if len(parts) != 3 {
		return false
	}

	switch parts[2] {
	case "pause", "resume", "fork", "connect", "timeout", "refreshes", "snapshots":
		return r.Method == http.MethodPost
	case "network":
		return r.Method == http.MethodPut
	case "custom-extension-params":
		return r.Method == http.MethodGet || r.Method == http.MethodPatch
	default:
		return false
	}
}

func (s *Server) logHostRoutingHeaderConflicts(r *http.Request, route *hostRoute) {
	headerSandboxIDValue, hasHeaderSandboxID := sandboxIDFromHeaders(r.Header)
	headerTargetPortValue, hasHeaderTargetPort := targetPortFromHeaders(r.Header)

	hostTargetPortValue := strconv.Itoa(route.targetPort)
	sandboxIDConflict := hasHeaderSandboxID && headerSandboxIDValue != route.sandboxID
	targetPortConflict := hasHeaderTargetPort && headerTargetPortValue != hostTargetPortValue
	if !sandboxIDConflict && !targetPortConflict {
		return
	}

	s.logger.Debug("host routing overrides conflicting routing headers",
		zap.String("host", r.Host),
		zap.String("host_sandbox_id", route.sandboxID),
		zap.String("host_target_port", hostTargetPortValue),
		zap.String("header_sandbox_id", headerSandboxIDValue),
		zap.String("header_target_port", headerTargetPortValue),
		zap.Bool("sandbox_id_conflict", sandboxIDConflict),
		zap.Bool("target_port_conflict", targetPortConflict),
	)
}

func isDataPlaneRouteSource(routeSource routeSource) bool {
	return routeSource == routeSourceHeader || routeSource == routeSourceHost
}

// upstreamTargetPath returns the path to use when forwarding to the upstream
// node. Requests routed via sandbox proxy host or routing headers are forwarded
// to the /proxy sub-tree on the upstream, while control-plane and scheduled
// requests are forwarded as-is.
func upstreamTargetPath(routeSource routeSource, originalPath string) string {
	if isDataPlaneRouteSource(routeSource) {
		return "/proxy" + originalPath
	}
	return originalPath
}

func upstreamTargetEscapedPath(routeSource routeSource, originalEscapedPath string) string {
	if isDataPlaneRouteSource(routeSource) {
		return "/proxy" + originalEscapedPath
	}
	return originalEscapedPath
}

func joinUpstream(endpoint string, path string, escapedPath string, rawQuery string) (string, error) {
	base, err := url.Parse(endpoint)
	if err != nil {
		return "", err
	}
	if base.Scheme == "" || base.Host == "" {
		return "", errors.New("endpoint must include scheme and host")
	}
	baseEscapedPath := base.EscapedPath()
	base.Path = joinURLPath(base.Path, path)
	if escapedPath != "" {
		base.RawPath = joinURLPath(baseEscapedPath, escapedPath)
	}
	base.RawQuery = rawQuery
	return base.String(), nil
}

func requestEscapedPath(r *http.Request) string {
	if raw := r.URL.RawPath; raw != "" {
		return raw
	}
	if uri := strings.TrimSpace(r.RequestURI); uri != "" {
		if parsed, err := url.ParseRequestURI(uri); err == nil {
			if escaped := parsed.EscapedPath(); escaped != "" {
				return escaped
			}
		}
	}
	if escaped := r.URL.EscapedPath(); escaped != "" {
		return escaped
	}
	return "/"
}

func joinURLPath(basePath, path string) string {
	switch {
	case strings.HasSuffix(basePath, "/") && strings.HasPrefix(path, "/"):
		return basePath + strings.TrimPrefix(path, "/")
	case !strings.HasSuffix(basePath, "/") && !strings.HasPrefix(path, "/"):
		if basePath == "" {
			return "/" + path
		}
		return basePath + "/" + path
	default:
		if basePath == "" {
			return "/" + strings.TrimPrefix(path, "/")
		}
		return basePath + path
	}
}

func injectForwardedHeaders(h http.Header, r *http.Request) {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	setXForwardedFor(h, r.RemoteAddr)
	h.Set("X-Forwarded-Host", r.Host)
	h.Set("X-Forwarded-Proto", scheme)
	h.Set("X-Forwarded-Method", r.Method)
	h.Set("X-Forwarded-URI", r.URL.RequestURI())
}

func setXForwardedFor(h http.Header, remoteAddr string) {
	host := strings.TrimSpace(remoteAddr)
	if parsedHost, _, err := net.SplitHostPort(remoteAddr); err == nil {
		host = parsedHost
	}
	if host == "" {
		h.Del("X-Forwarded-For")
		return
	}
	h.Set("X-Forwarded-For", host)
}

func requestContextForProxy(r *http.Request, routingCtx context.Context, streaming bool) (context.Context, context.CancelFunc) {
	if streaming {
		return r.Context(), func() {}
	}
	return routingCtx, func() {}
}

func isStreamingRequest(r *http.Request) bool {
	contentType := strings.ToLower(strings.TrimSpace(r.Header.Get("Content-Type")))
	if strings.HasPrefix(contentType, "application/grpc") {
		return true
	}
	if strings.HasPrefix(contentType, "application/connect+") {
		return true
	}
	if strings.TrimSpace(r.Header.Get("Connect-Protocol-Version")) != "" {
		return true
	}
	if strings.EqualFold(strings.TrimSpace(r.Header.Get("Accept")), "text/event-stream") {
		return true
	}
	if headerContainsToken(r.Header, "Te", "trailers") {
		return true
	}
	return false
}

func isWebSocketRequest(r *http.Request) bool {
	return strings.EqualFold(strings.TrimSpace(r.Header.Get("Upgrade")), "websocket") &&
		headerContainsToken(r.Header, "Connection", "upgrade")
}

func headerContainsToken(h http.Header, name string, want string) bool {
	for _, v := range h.Values(name) {
		for _, token := range strings.Split(v, ",") {
			if strings.EqualFold(strings.TrimSpace(token), want) {
				return true
			}
		}
	}
	return false
}

// sandboxAssignment is one sandbox named by a response, with whatever that
// response said about it alongside.
// resumeTargetPort is the port the data-plane request was addressed to, as it
// appeared on the wire.
//
// 🔴 Forwarded as text, unparsed. The API half treats an unparseable port the
// same as an absent one — as *possibly* envd traffic, which is the strict
// direction for the credential check — and re-deciding that here would put one
// decision in two places that could disagree. A gateway that "helpfully"
// dropped a malformed port would hand a caller the skip it was reaching for.
func resumeTargetPort(r *http.Request, route *hostRoute) string {
	if route != nil {
		return strconv.Itoa(route.targetPort)
	}
	if value, ok := targetPortFromHeaders(r.Header); ok {
		return value
	}
	return ""
}

// writeResumeError turns the API half's refusal into a status code.
//
// 🔴 A 404 on a resume is the end of that sandbox as far as any client is
// concerned — the platform's contract for it is "rebuild from the template",
// which resets the user's workspace — so it is spent only on a verdict that
// positively says the sandbox is gone: NotFound, the one code the resume
// client maps to VerdictGone.
//
// 🔴 A FailedPrecondition is a 503 and never a retry against another node. For
// a sandbox whose snapshot never reached shared storage there is no second copy
// to try: waking it elsewhere would not fail, it would succeed, by rebuilding
// from an older snapshot and losing the last pause. Retry-After tells the
// client to wait for the machine that has the bytes, which is the only correct
// thing to wait for.
func (s *Server) writeResumeError(w http.ResponseWriter, r *http.Request, sandboxID string, result resume.Result) {
	reason := s.resumeReason(result)
	code := codes.Unknown
	if result.Status != nil {
		code = result.Status.Code()
	}

	s.logger.Warn("the api half refused to wake a sandbox",
		zap.String("sandbox_id", sandboxID),
		zap.String("verdict", result.Verdict.String()),
		zap.String("code", code.String()),
		zap.String("refusal", result.Reason),
		zap.String("origin_node_id", result.OriginNodeID),
	)

	// The refusal ends the request here, so nobody upstream can answer a
	// preflight; the real request gets the status below.
	if cors.HandlePreflight(w, r) {
		return
	}

	switch code {
	case codes.NotFound:
		cors.Error(w, reason, http.StatusNotFound)
	case codes.PermissionDenied:
		cors.Error(w, reason, http.StatusForbidden)
	case codes.FailedPrecondition, codes.ResourceExhausted:
		// 🔴 410 and not 503, and this is the one refusal in this switch that
		// is not a failure. `autoResume: {enabled: false}` is the sandbox's
		// owner saying traffic must not bring it back; a 503 would advertise
		// that as temporary and have every client retry forever against a
		// sandbox that is never going to answer. 410 is also byte-for-byte
		// what a node answers for a paused sandbox it will not wake
		// (`src/api/proxy.rs`'s `SandboxUnavailable`), so the flag reads the
		// same to a client whichever half fields the request.
		if result.Reason == resumeReasonAutoResumeDisabled {
			cors.Error(w, reason, http.StatusGone)
			return
		}
		// Retry-After on the transient one only. `transition_in_progress`
		// clears in a moment; a pin refusal clears when a machine comes back,
		// which may be never, and a small Retry-After on that would have the
		// client hammering a node that is not coming.
		if result.Reason == resumeReasonTransitionInProgress {
			w.Header().Set("Retry-After", "1")
		}
		cors.Error(w, reason, http.StatusServiceUnavailable)
	case codes.InvalidArgument:
		cors.Error(w, reason, http.StatusBadRequest)
	default:
		// 🔴 Unimplemented lands here, and that is load-bearing: §12 P3's
		// control C stubs this RPC out with Unimplemented and requires the data
		// plane to *fail*. Anything but a failure here would mean a second
		// wake-up path was quietly doing the work, which is the exact thing the
		// control is designed to detect.
		cors.Error(w, "sandbox wake-up failed", http.StatusBadGateway)
	}
}

// resumeReason is what of the API half's answer a caller may see.
//
// Runs through the same redaction as the scheduler's, because the same hazard
// applies: a status produced by a failed dial names the cluster's internal
// addressing, and it is not the API half speaking.
func (s *Server) resumeReason(result resume.Result) string {
	if result.Status == nil {
		return "the api half could not be reached"
	}
	return s.schedulerReason(result.Status)
}
