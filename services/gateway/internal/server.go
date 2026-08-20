package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"time"

	schedulerv1 "agentenv/services/api/proto"
	"agentenv/services/shared/config"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	headerSandboxID     = "x-agentenv-sandbox-id"
	headerE2BSandboxID  = "e2b-sandbox-id"
	headerTargetPort    = "x-agentenv-target-port"
	headerE2BTargetPort = "e2b-sandbox-port"
	headerNodeID        = "x-agentenv-node-id"
	// headerProjectionTTLSecs is the node's own budget for how long the routing
	// projection of the sandbox it just started should live, in whole seconds.
	//
	// 🔴 Absent, unparseable and non-positive all mean the same thing here:
	// nothing to forward, and the scheduler falls back to its binding_ttl. None
	// of them may ever become "no expiry".
	headerProjectionTTLSecs    = "x-agentenv-projection-ttl-secs"
	maxRecordAssignmentTimeout = 5 * time.Second

	// headerReroute is how an isolated node asks for a request to be handed to
	// somebody else instead. The gateway does not act on it: since the
	// scheduler resolves a sandbox against the paused registry before anything
	// is forwarded, an isolated node is either excluded from the decision or is
	// the only node that could have served the request at all. The marker is
	// forwarded verbatim, so an operator still sees why a node refused.
	headerReroute         = "x-agentenv-reroute"
	rerouteReasonSchedule = "schedule"
)

type routeSource string

const (
	routeSourceHeader   routeSource = "header"
	routeSourceHost     routeSource = "host"
	routeSourcePath     routeSource = "path"
	routeSourceSchedule routeSource = "schedule"
	routeSourceGateway  routeSource = "gateway"
)

type ServerOptions struct {
	RequestTimeout           time.Duration
	MaxResponseSize          int64
	DebugMode                bool
	SandboxProxyDomains      []string
	QueryOnlySchedulerClient schedulerv1.SchedulerClient
	// ExecutionFencing is the raw configured mode. It is parsed in NewServer so
	// an unrecognised value stops the process rather than becoming a silently
	// chosen behaviour; the empty string is the absence of a setting and takes
	// the default.
	ExecutionFencing string
	// ControlPlaneToken is stamped on every request the gateway forwards to a
	// node. Empty means stamp nothing, which is the state the fleet runs in
	// until the node-side gate is turned on.
	ControlPlaneToken string
	// ProjectionReader lets a sandbox route be answered out of the routing
	// projection instead of from a LookupNode call. Nil is the read switch in
	// its off position, and is the behaviour that shipped before it existed.
	ProjectionReader projectionReader
	// ProjectionAuthoritative is the gateway's half of the write-side switch:
	// resume and connect record an assignment, and the incarnation and TTL a
	// node reports are forwarded to the scheduler. Off forwards neither, which
	// is a projection write identical to today's.
	ProjectionAuthoritative bool
}

type Server struct {
	logger             *zap.Logger
	scheduler          schedulerv1.SchedulerClient
	queryOnlyScheduler schedulerv1.SchedulerClient
	httpClient         *http.Client
	requestTimeout     time.Duration
	maxRespSize        int64
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
	projectionReader        projectionReader
	projectionAuthoritative bool
}

func NewServer(logger *zap.Logger, schedulerClient schedulerv1.SchedulerClient, options ServerOptions) (*Server, error) {
	sandboxProxyDomains, err := normalizeProxyDomains(options.SandboxProxyDomains)
	if err != nil {
		return nil, err
	}

	executionFencing, err := config.ParseGatewayExecutionFencing(options.ExecutionFencing)
	if err != nil {
		return nil, err
	}

	queryOnlyScheduler := options.QueryOnlySchedulerClient
	if queryOnlyScheduler == nil {
		queryOnlyScheduler = schedulerClient
	}

	return &Server{
		logger:                  logger,
		scheduler:               schedulerClient,
		queryOnlyScheduler:      queryOnlyScheduler,
		httpClient:              &http.Client{},
		requestTimeout:          options.RequestTimeout,
		maxRespSize:             options.MaxResponseSize,
		debugMode:               options.DebugMode,
		sandboxProxyDomains:     sandboxProxyDomains,
		executionFencing:        executionFencing,
		controlPlaneToken:       strings.TrimSpace(options.ControlPlaneToken),
		projectionReader:        options.ProjectionReader,
		projectionAuthoritative: options.ProjectionAuthoritative,
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
					http.Error(w, "sandbox id header required", http.StatusBadRequest)
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
				http.NotFound(w, r)
			}
			return
		}
		s.handleProxy(w, r)
	})
	return s.instrumentGatewayHTTP(core)
}

func (s *Server) writeJSON(w http.ResponseWriter, status int, value any) {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(value); err != nil {
		s.logger.Warn("encode json response failed",
			zap.Error(err),
			zap.Int("status", status),
		)
		http.Error(w, "failed to encode response", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if _, err := buf.WriteTo(w); err != nil {
		s.logger.Warn("write json response failed",
			zap.Error(err),
			zap.Int("status", status),
		)
	}
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
		http.Error(w, hostRouteErr.Error(), http.StatusBadRequest)
		return
	}

	if hostRoute == nil && !hasProxyRoutingHeaders(r.Header) {
		if isClusterListRequest(r) {
			setGatewayRouteSource(w, routeSourceGateway)
			s.handleClusterList(w, r, routingCtx)
			return
		} else if isNodeListRequest(r) {
			setGatewayRouteSource(w, routeSourceGateway)
			s.handleNodeList(w, r, routingCtx)
			return
		} else if isRegistryListRequest(r) {
			setGatewayRouteSource(w, routeSourceGateway)
			s.handleRegistryList(w, r, routingCtx)
			return
		} else if nodeID, ok := isNodeAdminRequest(r); ok {
			setGatewayRouteSource(w, routeSourcePath)
			s.handleNodeDetail(w, r, routingCtx, nodeID, longLived)
			return
		}
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
	if !hasSandbox {
		routeSource = routeSourceSchedule
	}
	setGatewayRouteSource(w, routeSource)
	var node *schedulerv1.Node
	// How the scheduler arrived at that node. Anything other than BOUND means
	// the node does not hold the sandbox yet, which is what decides whether the
	// binding has to be written on the way back.
	location := schedulerv1.SandboxLocation_SANDBOX_LOCATION_UNSPECIFIED
	// Decided once, from the one lookup answer, and carried to both ends of the
	// proxied exchange. A request the scheduler never resolved has no incarnation
	// to reason about, so it keeps the zero plan and stamps nothing.
	fencing := fencingPlan{}
	// Empty for a scheduled request, which resolves no sandbox and so has no
	// route to have resolved one way or the other.
	routeResolution := ""

	if hasSandbox {
		// The routing projection first, when the read switch is on. A hit is
		// the same answer the scheduler's binding hit would have produced —
		// routing.Synthesize and lookup.go's binding exit are held field-for-
		// field identical by a golden test — so nothing below this block needs
		// to know which of the two answered.
		//
		// source starts at "scheduler" and only a projection hit moves it. A
		// miss or a read error counts itself where it happens and still leaves
		// source alone, so the two series reconcile:
		//
		//	Δ{redis_miss} + Δ{redis_error} ≈ Δ{scheduler}
		source := routeResolutionScheduler
		resp := s.resolveFromProjection(routingCtx, sandboxID)
		if resp != nil {
			source = routeResolutionRedisHit
		}

		if resp == nil {
			// 🔴 Everything the projection could not answer lands here, and
			// that includes a read error. A miss is not an absence: the
			// scheduler walks the binding, then the heartbeat roster, then the
			// paused registry, and the last two are exactly what covers a
			// projection that has expired or been deleted. Answering 404 from
			// a miss would cut all of that out.
			//
			// One call, one answer. The scheduler owns the whole decision —
			// which node holds the sandbox, which node should rebuild it, and
			// whether it exists at all — so there is nothing here to
			// second-guess or retry against a different node.
			rpcStart := time.Now()
			var err error
			resp, err = s.queryOnlyScheduler.LookupNode(routingCtx, &schedulerv1.LookupNodeRequest{SandboxId: sandboxID})
			recordGatewaySchedulerRPC("LookupNode", rpcStart, err)
			if err != nil {
				// 🔴 Still the only source of a 404 or a 503 in this package.
				s.writeSchedulerError(w, err)
				return
			}
		}
		recordRouteResolution(source)
		routeResolution = source
		node = resp.GetNode()
		location = resp.GetLocation()
		recordGatewaySandboxLocation(location)
		plane := fencingPlaneFor(routeSource)
		fencing = decideFencing(s.executionFencing, plane, resp)
		recordExecutionFencing(plane, fencing.decision)
		if locationNeedsAssignment(location) {
			s.logger.Info("routing a sandbox the scheduler resolved from the paused registry",
				zap.String("sandbox_id", sandboxID),
				zap.String("location", gatewaySandboxLocationLabel(location)),
				zap.String("node_id", node.GetNodeId()),
				zap.String("origin_node_id", resp.GetOriginNodeId()),
			)
		}
	} else {
		hint, err := buildScheduleHint(r)
		if err != nil {
			// this only happens it cannot read request body, so the request cannot continue
			s.logger.Warn("Fatal error when building schedule hint",
				zap.String("method", r.Method),
				zap.String("path", r.URL.Path),
				zap.Error(err),
			)
			http.Error(w, "failed to read request body", http.StatusBadRequest)
			return
		}
		rpcStart := time.Now()
		resp, err := s.scheduler.Schedule(routingCtx, &schedulerv1.ScheduleRequest{
			Hint: hint,
		})
		recordGatewaySchedulerRPC("Schedule", rpcStart, err)
		if err != nil {
			s.writeSchedulerError(w, err)
			return
		}
		node = resp.GetNode()
	}

	s.logger.Debug("gateway routed request",
		zap.String("method", r.Method),
		zap.String("path", r.URL.Path),
		zap.String("route_source", string(routeSource)),
		zap.String("location", gatewaySandboxLocationLabel(location)),
		zap.String("sandbox_id", sandboxID),
		zap.String("node_id", node.GetNodeId()),
		zap.String("upstream_endpoint", node.GetEndpoint()),
		// The control plane resolves an incarnation and never acts on it, so
		// this line is the only place it is visible for those requests.
		zap.String("expected_execution_id", fencing.expect),
		zap.String("execution_authority", fencing.authority.String()),
		zap.String("fencing_stage", fencingStageGatewayRoute),
		zap.String("route_resolution", routeResolution),
	)

	decodedPath := upstreamTargetPath(routeSource, r.URL.Path)
	escapedPath := upstreamTargetEscapedPath(routeSource, requestEscapedPath(r))
	upstreamURL, err := joinUpstream(node.GetEndpoint(), decodedPath, escapedPath, r.URL.RawQuery)
	if err != nil {
		http.Error(w, "invalid upstream endpoint", http.StatusBadGateway)
		return
	}

	upstreamCtx, cancelUpstream := requestContextForProxy(r, routingCtx, longLived)
	defer cancelUpstream()

	s.proxyRequest(
		w,
		r.Clone(upstreamCtx),
		r.Context(),
		upstreamURL,
		node,
		proxyRequestOptions{
			assignment:       s.assignmentRouteFor(r, routeSource, hasSandbox, location),
			hostRoute:        hostRoute,
			flushImmediately: longLived,
			sandboxID:        sandboxID,
			fencing:          fencing,
		},
	)
}

// locationNeedsAssignment reports whether the binding has to be written against
// the node the scheduler named, as soon as that node answers.
//
// PLACED and PINNED both come from a registry row rather than from a binding,
// so the node is about to hold a sandbox nothing has recorded against it.
// Waiting for its next heartbeat to notice would leave every request for that
// sandbox unroutable until then.
func locationNeedsAssignment(location schedulerv1.SandboxLocation) bool {
	switch location {
	case schedulerv1.SandboxLocation_SANDBOX_LOCATION_PLACED,
		schedulerv1.SandboxLocation_SANDBOX_LOCATION_PINNED:
		return true
	default:
		return false
	}
}

// writeSchedulerError turns the scheduler's answer into a status code.
//
// 🔴 The three failure codes must stay distinct. NotFound is the scheduler
// saying the sandbox exists nowhere, and a 404 on a resume is the end of that
// sandbox as far as any client is concerned. Unavailable is the scheduler
// saying it could not look, and FailedPrecondition is it saying the one node
// that could serve this sandbox will not — both are 503s, because both are
// states the caller may find changed a moment later. Collapsing any of them
// into another is the bug this whole path exists to avoid.
func (s *Server) writeSchedulerError(w http.ResponseWriter, err error) {
	st, ok := status.FromError(err)
	if !ok {
		http.Error(w, "scheduler unavailable", http.StatusBadGateway)
		return
	}
	switch st.Code() {
	case codes.InvalidArgument:
		http.Error(w, st.Message(), http.StatusBadRequest)
	case codes.NotFound:
		http.Error(w, st.Message(), http.StatusNotFound)
	case codes.Unavailable, codes.FailedPrecondition:
		http.Error(w, st.Message(), http.StatusServiceUnavailable)
	default:
		http.Error(w, "scheduler error", http.StatusBadGateway)
	}
}

type proxyRequestOptions struct {
	assignment       assignmentRoute
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
	originalCtx context.Context,
	target string,
	node *schedulerv1.Node,
	options proxyRequestOptions,
) {
	upstreamURL, err := url.Parse(target)
	if err != nil {
		http.Error(w, "invalid upstream endpoint", http.StatusBadGateway)
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
				if nodeID := node.GetNodeId(); nodeID != "" {
					resp.Header.Set(headerNodeID, nodeID)
				}
			}
			// Before the assignment record, on purpose: a refused exchange must
			// not leave a binding behind naming the node that was refused.
			if err := s.fenceProxyResponse(options.fencing, options.sandboxID, node, resp); err != nil {
				return err
			}
			if options.assignment == assignmentRouteNone || resp.StatusCode < 200 || resp.StatusCode >= 300 {
				return nil
			}
			return s.recordAssignmentFromResponse(originalCtx, resp, node, options)
		},
		ErrorHandler: func(rw http.ResponseWriter, _ *http.Request, err error) {
			if errors.Is(err, context.Canceled) {
				logLevel := zap.WarnLevel
				if isStreamInputProxyRequest(proxyReq) {
					logLevel = zap.DebugLevel
				}
				s.logger.Log(logLevel, "proxy request closed by client",
					zap.Error(err),
					zap.String("node", node.GetNodeId()),
					zap.String("path", proxyReq.URL.Path),
					zap.String("target", upstreamURL.String()),
				)
				return
			}

			if errors.Is(err, context.DeadlineExceeded) || errors.Is(proxyReq.Context().Err(), context.DeadlineExceeded) {
				s.logger.Warn("proxy request timed out",
					zap.Error(err),
					zap.String("node", node.GetNodeId()),
					zap.String("path", proxyReq.URL.Path),
					zap.String("target", upstreamURL.String()),
				)
				http.Error(rw, "upstream timeout", http.StatusGatewayTimeout)
				return
			}

			var proxyErr *proxyResponseError
			if errors.As(err, &proxyErr) {
				proxyErr.write(rw)
				return
			}

			s.logger.Warn("proxy request failed",
				zap.Error(err),
				zap.String("node", node.GetNodeId()),
				zap.String("path", proxyReq.URL.Path),
				zap.String("target", upstreamURL.String()),
			)
			http.Error(rw, "upstream unavailable", http.StatusBadGateway)
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

// write emits the error as a response.
func (e *proxyResponseError) write(rw http.ResponseWriter) {
	if len(e.body) == 0 {
		http.Error(rw, e.message, e.statusCode)
		return
	}
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

func (s *Server) recordAssignmentFromResponse(ctx context.Context, resp *http.Response, node *schedulerv1.Node, options proxyRequestOptions) error {
	recordCtx, cancelRecord := context.WithTimeout(ctx, recordAssignmentTimeout(s.requestTimeout))
	defer cancelRecord()

	// The node names the incarnation it just started on the same response. It is
	// optional on the way in: absent only costs authority for the window between
	// the create and the node's first heartbeat, and inside that window the
	// sandbox is new and has exactly one incarnation.
	executionID := executionIDFromResponse(resp.Header)
	projectionTTLSecs := s.projectionTTLToRecord(resp.Header)

	// 🔴 The routed sandbox is the answer for resume and connect, and it costs
	// nothing to find: it came off the request path. Falling through to the
	// body would buffer the whole response to rediscover an id already in hand,
	// and — worse — resume's 201 carries no sandbox-id header at all, so the
	// buffering would not be optional.
	if options.assignment == assignmentRoutePath {
		if sandboxID := strings.TrimSpace(options.sandboxID); sandboxID != "" {
			s.recordAssignment(recordCtx, sandboxID, node, executionID, projectionTTLSecs, "routed_path")
			return nil
		}
	}

	if sandboxID, ok := sandboxIDFromHeaders(resp.Header); ok {
		s.recordAssignment(recordCtx, sandboxID, node, executionID, projectionTTLSecs, "response_header")
		return nil
	}

	body, truncated, err := readBodyWithLimit(resp.Body, s.maxRespSize)
	if err != nil {
		return &proxyResponseError{
			statusCode: http.StatusBadGateway,
			message:    "failed to read upstream response",
			cause:      err,
		}
	}
	if truncated {
		s.logger.Warn("upstream response exceeded configured forwarding limit",
			zap.Int64("max_response_size_bytes", s.maxRespSize),
			zap.Int64("upstream_content_length", resp.ContentLength),
			zap.String("content_type", resp.Header.Get("Content-Type")),
		)
		return &proxyResponseError{
			statusCode: http.StatusBadGateway,
			message:    "upstream response too large",
		}
	}
	_ = resp.Body.Close()

	resp.Body = io.NopCloser(bytes.NewReader(body))
	resp.ContentLength = int64(len(body))
	if resp.Header == nil {
		resp.Header = make(http.Header)
	}
	resp.Header.Set("Content-Length", strconv.Itoa(len(body)))

	for _, assignment := range extractSandboxAssignmentsFromResponse(body) {
		// 🔴 Each element's own incarnation, never the response's echoed one
		// and never a neighbour's. A fork answers with several sandboxes and
		// one echo; stamping that echo onto a child would name an incarnation
		// that never ran there, which is why this path used to record none at
		// all. Now that each element carries its own, an element that has one
		// is recorded with it and an element that does not is recorded without
		// — taking the same unauthoritative window a fresh create takes, until
		// the node's first heartbeat.
		s.recordAssignment(recordCtx, assignment.sandboxID, node,
			assignment.executionID,
			s.projectionTTLToRecordValue(assignment.projectionTTLSecs),
			"response_body")
	}
	return nil
}

// projectionTTLToRecord is the gateway's half of the write-side switch, and it
// is deliberately narrow.
//
// 🔴 It gates the TTL and nothing else. The incarnation is forwarded either
// way: reading it off a response and passing it on is behaviour that already
// shipped, and gating it here would be a rollback of something the switch was
// never about. The TTL is the new fact, and with the switch off the gateway
// sends zero — which the scheduler reads as "use binding_ttl", making the
// projection write byte-identical to the one that shipped before this existed.
// That is what lets the two halves of the switch be flipped in either order
// without an intermediate state anybody has to reason about.
func (s *Server) projectionTTLToRecord(h http.Header) uint32 {
	return s.projectionTTLToRecordValue(projectionTTLSecsFromHeaders(h))
}

func (s *Server) projectionTTLToRecordValue(secs uint32) uint32 {
	if !s.projectionAuthoritative {
		return 0
	}
	return secs
}

func (s *Server) recordAssignment(ctx context.Context, sandboxID string, node *schedulerv1.Node, executionID string, projectionTTLSecs uint32, source string) {
	rpcStart := time.Now()
	_, err := s.scheduler.RecordAssignment(ctx, &schedulerv1.RecordAssignmentRequest{
		SandboxId:         sandboxID,
		Node:              node,
		ExecutionId:       executionID,
		ProjectionTtlSecs: projectionTTLSecs,
	})
	recordGatewaySchedulerRPC("RecordAssignment", rpcStart, err)
	if err != nil {
		s.logger.Warn("record assignment failed", zap.Error(err), zap.String("sandbox_id", sandboxID), zap.String("node_id", node.GetNodeId()))
		return
	}

	s.logger.Debug("gateway recorded sandbox assignment",
		zap.String("sandbox_id", sandboxID),
		zap.String("node_id", node.GetNodeId()),
		zap.String("observed_execution_id", executionID),
		zap.Uint32("projection_ttl_secs", projectionTTLSecs),
		zap.String("source", source),
	)
}

func readBodyWithLimit(src io.Reader, limit int64) ([]byte, bool, error) {
	if limit <= 0 {
		body, err := io.ReadAll(src)
		return body, false, err
	}
	body, err := io.ReadAll(io.LimitReader(src, limit+1))
	if err != nil {
		return nil, false, err
	}
	if int64(len(body)) > limit {
		return nil, true, nil
	}
	return body, false, nil
}

func recordAssignmentTimeout(requestTimeout time.Duration) time.Duration {
	if requestTimeout <= 0 {
		return maxRecordAssignmentTimeout
	}
	if requestTimeout < maxRecordAssignmentTimeout {
		return requestTimeout
	}
	return maxRecordAssignmentTimeout
}

func flushInterval(flushImmediately bool) time.Duration {
	if flushImmediately {
		return -1
	}
	return 0
}

// assignmentRoute says how the sandbox an assignment is for should be found.
//
// 🔴 Not a bool, because the two ways are genuinely different work. One reads
// the response — a header if the node put one there, otherwise the whole body,
// buffered. The other already knows: the request was routed for that sandbox
// and the id came out of the path. Collapsing them would mean either buffering
// a body to rediscover an id we are holding, or using the routed id on the one
// route where it is the wrong answer.
type assignmentRoute int

const (
	// assignmentRouteNone: this exchange writes no assignment.
	assignmentRouteNone assignmentRoute = iota
	// assignmentRouteResponse: the sandboxes are named in the response.
	// Creates, cold creates, and forks — a fork answers with several sandboxes
	// and none of them is the one the request was routed for.
	assignmentRouteResponse
	// assignmentRoutePath: the assignment is for the sandbox this request was
	// already routed for. Resume and connect, and any control-plane request the
	// scheduler resolved off the paused registry.
	assignmentRoutePath
)

// assignmentRouteFor decides whether this exchange writes a routing projection,
// and how the sandbox is named.
func (s *Server) assignmentRouteFor(r *http.Request, routeSource routeSource, hasSandbox bool, location schedulerv1.SandboxLocation) assignmentRoute {
	if isForkRequest(r, routeSource, hasSandbox) {
		return assignmentRouteResponse
	}
	if shouldRecordCreateAssignment(r, hasSandbox) {
		return assignmentRouteResponse
	}
	// 🔴 resume and connect both, never resume alone. Connect is a resume
	// entry point — the node routes both into the same resume path — so
	// recording one and not the other leaves the identical hole under a
	// different name.
	if s.projectionAuthoritative && isResumeEntryPoint(r, routeSource, hasSandbox) {
		return assignmentRoutePath
	}
	// A sandbox the scheduler resolved off the paused registry is about to be
	// held by a node nothing has recorded against. That was already true before
	// any of this and is unrelated to the switch.
	if hasSandbox && locationNeedsAssignment(location) {
		if routeSource == routeSourcePath {
			return assignmentRoutePath
		}
		return assignmentRouteResponse
	}
	return assignmentRouteNone
}

func shouldRecordCreateAssignment(r *http.Request, hasSandbox bool) bool {
	if r.Method != http.MethodPost || hasSandbox {
		return false
	}
	path := strings.TrimRight(r.URL.Path, "/")
	return path == "/sandboxes" || path == "/sandboxes-cold"
}

// isForkRequest: routed by the source sandbox, but it creates child sandbox
// assignments, so the routed id is not the one to record.
func isForkRequest(r *http.Request, routeSource routeSource, hasSandbox bool) bool {
	parts, ok := sandboxSubResourcePath(r, routeSource, hasSandbox)
	return ok && parts[2] == "fork"
}

func isResumeEntryPoint(r *http.Request, routeSource routeSource, hasSandbox bool) bool {
	parts, ok := sandboxSubResourcePath(r, routeSource, hasSandbox)
	if !ok {
		return false
	}
	switch parts[2] {
	case "resume", "connect":
		return true
	default:
		return false
	}
}

func sandboxSubResourcePath(r *http.Request, routeSource routeSource, hasSandbox bool) ([]string, bool) {
	if r.Method != http.MethodPost || !hasSandbox || routeSource != routeSourcePath {
		return nil, false
	}
	path := strings.Trim(strings.TrimRight(r.URL.Path, "/"), "/")
	parts := strings.Split(path, "/")
	if len(parts) != 3 || parts[0] != "sandboxes" || strings.TrimSpace(parts[1]) == "" {
		return nil, false
	}
	return parts, true
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
type sandboxAssignment struct {
	sandboxID   string
	executionID string
	// projectionTTLSecs is the node's budget for this sandbox's routing
	// projection. 🔴 Zero means "not offered", which the scheduler reads as
	// "use binding_ttl". It is never "no expiry".
	projectionTTLSecs uint32
}

func extractSandboxIDFromResponse(body []byte) (string, bool) {
	assignments := extractSandboxAssignmentsFromResponse(body)
	if len(assignments) == 0 {
		return "", false
	}
	return assignments[0].sandboxID, true
}

// extractSandboxAssignmentsFromResponse reads every sandbox a response names.
//
// 🔴 The top-level array comes first, and it is the whole reason this function
// changed. Fork's 201 answers with a bare JSON array of per-fork results — no
// envelope, no object — and this used to begin by unmarshalling into a
// map[string]any, which fails outright on an array and returned nil. Fork's
// projection write therefore never happened, in any build, and the only test
// covering it fed a {"sandboxes":[…]} envelope that no route in this repo
// produces.
//
// The object shapes below are kept because create and cold-create answer with
// one, and because a caller may reach here with a body this function has always
// been able to read.
func extractSandboxAssignmentsFromResponse(body []byte) []sandboxAssignment {
	var assignments []sandboxAssignment

	var array []any
	if err := json.Unmarshal(body, &array); err == nil {
		for _, item := range array {
			object, ok := item.(map[string]any)
			if !ok {
				continue
			}
			assignments = appendSandboxAssignment(assignments, object)
		}
		return dedupeSandboxAssignments(assignments)
	}

	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil
	}
	assignments = appendSandboxAssignment(assignments, payload)
	if data, ok := payload["data"].(map[string]any); ok {
		assignments = appendSandboxAssignment(assignments, data)
	}
	appendFromArray := func(value any) {
		items, ok := value.([]any)
		if !ok {
			return
		}
		for _, item := range items {
			object, ok := item.(map[string]any)
			if !ok {
				continue
			}
			assignments = appendSandboxAssignment(assignments, object)
		}
	}
	appendFromArray(payload["sandboxes"])
	if data, ok := payload["data"].(map[string]any); ok {
		appendFromArray(data["sandboxes"])
	}
	return dedupeSandboxAssignments(assignments)
}

// appendSandboxAssignment reads one object.
//
// A fork result wraps the sandbox one level down and carries the projection
// budget beside it rather than inside it, because the budget is infrastructure
// and the sandbox is the user-visible model. Every other shape carries both on
// the object itself, so both levels are consulted, outer first for the budget.
func appendSandboxAssignment(dst []sandboxAssignment, object map[string]any) []sandboxAssignment {
	if object == nil {
		return dst
	}
	inner := object
	if nested, ok := object["sandbox"].(map[string]any); ok {
		inner = nested
	}
	sandboxID := firstStringField(inner, "sandboxID", "sandboxId", "sandbox_id")
	if sandboxID == "" {
		return dst
	}
	ttl := firstTTLField(object)
	if ttl == 0 && inner != nil {
		ttl = firstTTLField(inner)
	}
	return append(dst, sandboxAssignment{
		sandboxID:   sandboxID,
		executionID: firstStringField(inner, "executionID", "executionId", "execution_id"),

		projectionTTLSecs: ttl,
	})
}

func firstStringField(object map[string]any, keys ...string) string {
	for _, key := range keys {
		if value, ok := object[key].(string); ok {
			if trimmed := strings.TrimSpace(value); trimmed != "" {
				return trimmed
			}
		}
	}
	return ""
}

// firstTTLField reads a projection budget out of a decoded JSON object.
//
// 🔴 Anything that is not a positive whole number of seconds becomes zero,
// which the scheduler reads as "use binding_ttl". A negative value in
// particular must never survive into something a store could read as "keep this
// forever" — that is the exact shape of the bug this rule exists to avoid.
func firstTTLField(object map[string]any) uint32 {
	for _, key := range []string{"projectionTtlSecs", "projectionTTLSecs", "projection_ttl_secs"} {
		value, ok := object[key].(float64)
		if !ok {
			continue
		}
		if value <= 0 {
			return 0
		}
		if value > math.MaxUint32 {
			return math.MaxUint32
		}
		return uint32(value)
	}
	return 0
}

// dedupeSandboxAssignments keeps the first spelling of each sandbox id, as this
// has always done. A later element naming the same sandbox is a duplicate, not
// a correction.
func dedupeSandboxAssignments(assignments []sandboxAssignment) []sandboxAssignment {
	if len(assignments) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(assignments))
	unique := assignments[:0]
	for _, assignment := range assignments {
		if _, ok := seen[assignment.sandboxID]; ok {
			continue
		}
		seen[assignment.sandboxID] = struct{}{}
		unique = append(unique, assignment)
	}
	return unique
}

// projectionTTLSecsFromHeaders reads the node's budget off a response header.
//
// 🔴 Absent, unparseable, and non-positive all come back as zero — "not
// offered" — and the scheduler falls back to its own binding_ttl. None of them
// may become "no expiry", which is what a Redis SET with no TTL argument is.
func projectionTTLSecsFromHeaders(h http.Header) uint32 {
	raw := strings.TrimSpace(h.Get(headerProjectionTTLSecs))
	if raw == "" {
		return 0
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value <= 0 {
		return 0
	}
	if value > math.MaxUint32 {
		return math.MaxUint32
	}
	return uint32(value)
}
