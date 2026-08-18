package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	headerSandboxID            = "x-agentenv-sandbox-id"
	headerE2BSandboxID         = "e2b-sandbox-id"
	headerTargetPort           = "x-agentenv-target-port"
	headerE2BTargetPort        = "e2b-sandbox-port"
	headerNodeID               = "x-agentenv-node-id"
	maxRecordAssignmentTimeout = 5 * time.Second

	// headerReroute is how an isolated node asks the gateway to hand a request
	// to somebody else instead. Only a node that has established the work can
	// be picked up elsewhere sets it — see the resume path in the node's API.
	headerReroute         = "x-agentenv-reroute"
	rerouteReasonSchedule = "schedule"
)

// errRerouteRequested aborts a proxied response before any of it reaches the
// client, so the request can be sent to a different node. It never surfaces to
// a caller: the error handler swallows it and the caller retries.
var errRerouteRequested = errors.New("upstream asked for the request to be rerouted")

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
}

func NewServer(logger *zap.Logger, schedulerClient schedulerv1.SchedulerClient, options ServerOptions) (*Server, error) {
	sandboxProxyDomains, err := normalizeProxyDomains(options.SandboxProxyDomains)
	if err != nil {
		return nil, err
	}

	queryOnlyScheduler := options.QueryOnlySchedulerClient
	if queryOnlyScheduler == nil {
		queryOnlyScheduler = schedulerClient
	}

	return &Server{
		logger:              logger,
		scheduler:           schedulerClient,
		queryOnlyScheduler:  queryOnlyScheduler,
		httpClient:          &http.Client{},
		requestTimeout:      options.RequestTimeout,
		maxRespSize:         options.MaxResponseSize,
		debugMode:           options.DebugMode,
		sandboxProxyDomains: sandboxProxyDomains,
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
		if r.URL.Path == "/metrics" {
			http.NotFound(w, r)
			return
		}
		if r.URL.Path == "/health" {
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
			// Keep load balancer health checks local when they are not sandbox-routed.
			w.WriteHeader(http.StatusNoContent)
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
	// Set when a resume was routed to a node that does not hold the sandbox, so
	// the assignment gets recorded as soon as that node brings it back up.
	recoveredSandbox := false

	if hasSandbox {
		rpcStart := time.Now()
		resp, err := s.queryOnlyScheduler.LookupNode(routingCtx, &schedulerv1.LookupNodeRequest{SandboxId: sandboxID})
		recordGatewaySchedulerRPC("LookupNode", rpcStart, err)
		switch {
		case err == nil:
			node = resp.GetNode()
		case status.Code(err) == codes.NotFound && isPausedSandboxRecoveryRequest(r):
			// No node holds this sandbox. It may still be paused elsewhere in
			// the cluster with its snapshot in shared storage, and the node that
			// paused it may be gone for good — so instead of answering "not
			// found" from the gateway, hand the resume to a scheduled node and
			// let it claim the sandbox from the paused registry. A sandbox that
			// genuinely does not exist still comes back 404, from that node.
			recovery, recoveryErr := s.scheduleRecoveryNode(routingCtx, sandboxID)
			if recoveryErr != nil {
				// Report the original lookup failure: the sandbox not being
				// assigned anywhere is the more useful error here.
				s.writeSchedulerError(w, err)
				return
			}
			node = recovery
			recoveredSandbox = true
		default:
			s.writeSchedulerError(w, err)
			return
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
		zap.String("sandbox_id", sandboxID),
		zap.String("node_id", node.GetNodeId()),
		zap.String("upstream_endpoint", node.GetEndpoint()),
	)

	decodedPath := upstreamTargetPath(routeSource, r.URL.Path)
	escapedPath := upstreamTargetEscapedPath(routeSource, requestEscapedPath(r))
	upstreamURL, err := joinUpstream(node.GetEndpoint(), decodedPath, escapedPath, r.URL.RawQuery)
	if err != nil {
		http.Error(w, "invalid upstream endpoint", http.StatusBadGateway)
		return
	}

	// A resume sent to a node by an existing binding can come back declined:
	// that node is isolated and has established the sandbox can be rebuilt
	// elsewhere. Buffer the body up front so the request can be replayed; a
	// body too large to buffer simply forfeits the reroute rather than being
	// truncated.
	replayBody, canReplay := []byte(nil), false
	if !recoveredSandbox && isPausedSandboxRecoveryRequest(r) {
		var err error
		replayBody, canReplay, err = captureReplayBody(r)
		if err != nil {
			http.Error(w, "failed to read request body", http.StatusBadRequest)
			return
		}
	}

	upstreamCtx, cancelUpstream := requestContextForProxy(r, routingCtx, longLived)
	defer cancelUpstream()

	rerouted := s.proxyRequest(
		w,
		r.Clone(upstreamCtx),
		r.Context(),
		upstreamURL,
		node,
		proxyRequestOptions{
			recordAssignment: recoveredSandbox || shouldRecordAssignment(r, routeSource, hasSandbox),
			hostRoute:        hostRoute,
			flushImmediately: longLived,
			allowReroute:     canReplay,
		},
	)
	if !rerouted {
		return
	}

	s.rerouteToScheduledNode(w, r, routingCtx, sandboxID, replayBody, longLived)
}

// rerouteToScheduledNode re-sends a request that its bound node declined, to a
// node the scheduler picks instead. Nothing has been written to w yet.
//
// The assignment is recorded on the way out: the sandbox is moving to a node
// that never held it, and the binding has to follow it there rather than wait
// for the next heartbeat to notice.
func (s *Server) rerouteToScheduledNode(
	w http.ResponseWriter,
	r *http.Request,
	routingCtx context.Context,
	sandboxID string,
	body []byte,
	longLived bool,
) {
	recovery, err := s.scheduleRecoveryNode(routingCtx, sandboxID)
	if err != nil {
		s.writeSchedulerError(w, err)
		return
	}

	upstreamURL, err := joinUpstream(
		recovery.GetEndpoint(),
		upstreamTargetPath(routeSourcePath, r.URL.Path),
		upstreamTargetEscapedPath(routeSourcePath, requestEscapedPath(r)),
		r.URL.RawQuery,
	)
	if err != nil {
		http.Error(w, "invalid upstream endpoint", http.StatusBadGateway)
		return
	}

	setGatewayRouteSource(w, routeSourceSchedule)
	s.logger.Info("rerouting declined resume to a scheduled node",
		zap.String("sandbox_id", sandboxID),
		zap.String("node_id", recovery.GetNodeId()),
		zap.String("upstream_endpoint", recovery.GetEndpoint()),
	)

	restoreReplayBody(r, body)
	upstreamCtx, cancelUpstream := requestContextForProxy(r, routingCtx, longLived)
	defer cancelUpstream()

	s.proxyRequest(
		w,
		r.Clone(upstreamCtx),
		r.Context(),
		upstreamURL,
		recovery,
		proxyRequestOptions{
			recordAssignment: true,
			flushImmediately: longLived,
		},
	)
}

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
	case codes.Unavailable:
		http.Error(w, st.Message(), http.StatusServiceUnavailable)
	default:
		http.Error(w, "scheduler error", http.StatusBadGateway)
	}
}

type proxyRequestOptions struct {
	recordAssignment bool
	hostRoute        *hostRoute
	flushImmediately bool
	// allowReroute lets the upstream node decline the request in a way the
	// gateway acts on rather than forwards. Only set it where the caller is
	// prepared to send the request somewhere else.
	allowReroute bool
}

// proxyRequest forwards the request to one node. It reports whether that node
// declined it and asked for a different node to be tried; nothing has been
// written to w in that case, so the caller may forward it again.
func (s *Server) proxyRequest(
	w http.ResponseWriter,
	proxyReq *http.Request,
	originalCtx context.Context,
	target string,
	node *schedulerv1.Node,
	options proxyRequestOptions,
) bool {
	upstreamURL, err := url.Parse(target)
	if err != nil {
		http.Error(w, "invalid upstream endpoint", http.StatusBadGateway)
		return false
	}

	rerouteRequested := false

	proxy := &httputil.ReverseProxy{
		Rewrite: func(req *httputil.ProxyRequest) {
			req.Out.URL.Scheme = upstreamURL.Scheme
			req.Out.URL.Host = upstreamURL.Host
			req.Out.URL.Path = upstreamURL.Path
			req.Out.URL.RawPath = upstreamURL.RawPath
			req.Out.URL.RawQuery = upstreamURL.RawQuery
			req.Out.Host = req.In.Host
			injectForwardedHeaders(req.Out.Header, req.In)
			if options.hostRoute != nil {
				req.Out.Header.Set(headerSandboxID, options.hostRoute.sandboxID)
				req.Out.Header.Set(headerTargetPort, strconv.Itoa(options.hostRoute.targetPort))
			}
		},
		FlushInterval: flushInterval(options.flushImmediately),
		ModifyResponse: func(resp *http.Response) error {
			// An isolated node answers a resume it does not want to serve with
			// this marker instead of starting the sandbox. Drop the response
			// before any of it is copied to the client so the request can go to
			// another node; the client never learns this happened.
			if options.allowReroute &&
				resp.StatusCode == http.StatusServiceUnavailable &&
				strings.EqualFold(resp.Header.Get(headerReroute), rerouteReasonSchedule) {
				_ = resp.Body.Close()
				rerouteRequested = true
				return errRerouteRequested
			}
			// In debug mode, expose the upstream node id on the response so
			// operators can tell which backend node served a given request.
			// This is purely for debugging/observability and is not consumed
			// by the client.
			if s.debugMode {
				if nodeID := node.GetNodeId(); nodeID != "" {
					resp.Header.Set(headerNodeID, nodeID)
				}
			}
			if !options.recordAssignment || resp.StatusCode < 200 || resp.StatusCode >= 300 {
				return nil
			}
			return s.recordAssignmentFromResponse(originalCtx, resp, node)
		},
		ErrorHandler: func(rw http.ResponseWriter, _ *http.Request, err error) {
			if errors.Is(err, errRerouteRequested) {
				// Deliberately writes nothing: the caller retries on another
				// node and owns the response.
				s.logger.Debug("upstream asked for reroute",
					zap.String("node", node.GetNodeId()),
					zap.String("path", proxyReq.URL.Path),
				)
				return
			}

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
				http.Error(rw, proxyErr.message, proxyErr.statusCode)
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
	if rerouteRequested {
		return true
	}
	recordGatewayUpstreamProxy(route, proxyStart, w, proxyReq.Context())
	return false
}

func isStreamInputProxyRequest(r *http.Request) bool {
	return r.Method == http.MethodPost && r.URL.Path == "/process.Process/StreamInput"
}

type proxyResponseError struct {
	statusCode int
	message    string
	cause      error
}

func (e *proxyResponseError) Error() string {
	if e.cause != nil {
		return e.cause.Error()
	}
	return e.message
}

func (s *Server) recordAssignmentFromResponse(ctx context.Context, resp *http.Response, node *schedulerv1.Node) error {
	recordCtx, cancelRecord := context.WithTimeout(ctx, recordAssignmentTimeout(s.requestTimeout))
	defer cancelRecord()

	if sandboxID, ok := sandboxIDFromHeaders(resp.Header); ok {
		s.recordAssignment(recordCtx, sandboxID, node, "response_header")
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

	for _, sandboxID := range extractSandboxIDsFromResponse(body) {
		s.recordAssignment(recordCtx, sandboxID, node, "response_body")
	}
	return nil
}

func (s *Server) recordAssignment(ctx context.Context, sandboxID string, node *schedulerv1.Node, source string) {
	rpcStart := time.Now()
	_, err := s.scheduler.RecordAssignment(ctx, &schedulerv1.RecordAssignmentRequest{SandboxId: sandboxID, Node: node})
	recordGatewaySchedulerRPC("RecordAssignment", rpcStart, err)
	if err != nil {
		s.logger.Warn("record assignment failed", zap.Error(err), zap.String("sandbox_id", sandboxID), zap.String("node_id", node.GetNodeId()))
		return
	}

	s.logger.Debug("gateway recorded sandbox assignment",
		zap.String("sandbox_id", sandboxID),
		zap.String("node_id", node.GetNodeId()),
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

func shouldRecordAssignment(r *http.Request, routeSource routeSource, hasSandbox bool) bool {
	if r.Method != http.MethodPost {
		return false
	}
	path := strings.TrimRight(r.URL.Path, "/")
	if !hasSandbox {
		return path == "/sandboxes" || path == "/sandboxes-cold"
	}
	if routeSource != routeSourcePath {
		return false
	}

	// Fork is routed by the source sandbox but creates child sandbox assignments.
	parts := strings.Split(strings.Trim(path, "/"), "/")
	return len(parts) == 3 && parts[0] == "sandboxes" && strings.TrimSpace(parts[1]) != "" && parts[2] == "fork"
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

// isPausedSandboxRecoveryRequest reports whether a request may be served by a
// node that has never run the sandbox.
//
// Only resume qualifies. Every other sandbox endpoint addresses a sandbox the
// caller believes is live somewhere, and sending those to an arbitrary node
// would turn a stale routing entry into a confusing 404 against the wrong node.
// Resume is different: it is the one operation whose whole purpose is to bring
// a sandbox back from a snapshot, and the node-side handler already falls back
// to the paused registry when it does not hold the sandbox locally.
func isPausedSandboxRecoveryRequest(r *http.Request) bool {
	if r.Method != http.MethodPost {
		return false
	}
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")

	return len(parts) == 3 && parts[0] == "sandboxes" && strings.TrimSpace(parts[1]) != "" &&
		parts[2] == "resume"
}

// scheduleRecoveryNode picks the node that will attempt to rebuild a paused
// sandbox nobody currently holds.
func (s *Server) scheduleRecoveryNode(ctx context.Context, sandboxID string) (*schedulerv1.Node, error) {
	rpcStart := time.Now()
	resp, err := s.scheduler.Schedule(ctx, &schedulerv1.ScheduleRequest{})
	recordGatewaySchedulerRPC("Schedule", rpcStart, err)
	if err != nil {
		s.logger.Warn("no node available to recover an unassigned sandbox",
			zap.String("sandbox_id", sandboxID),
			zap.Error(err),
		)

		return nil, err
	}

	node := resp.GetNode()
	s.logger.Info("routing resume of an unassigned sandbox to a scheduled node",
		zap.String("sandbox_id", sandboxID),
		zap.String("node_id", node.GetNodeId()),
	)

	return node, nil
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

func extractSandboxIDFromResponse(body []byte) (string, bool) {
	ids := extractSandboxIDsFromResponse(body)
	if len(ids) == 0 {
		return "", false
	}
	return ids[0], true
}

func extractSandboxIDsFromResponse(body []byte) []string {
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil
	}
	var ids []string
	appendSandboxID := func(value any) {
		if id, ok := value.(string); ok && strings.TrimSpace(id) != "" {
			ids = append(ids, id)
		}
	}
	for _, key := range []string{"sandboxID", "sandboxId", "sandbox_id"} {
		appendSandboxID(payload[key])
	}
	if data, ok := payload["data"].(map[string]any); ok {
		for _, key := range []string{"sandboxID", "sandboxId", "sandbox_id"} {
			appendSandboxID(data[key])
		}
	}
	appendSandboxIDsFromArray := func(value any) {
		items, ok := value.([]any)
		if !ok {
			return
		}
		for _, item := range items {
			object, ok := item.(map[string]any)
			if !ok {
				continue
			}
			for _, key := range []string{"sandboxID", "sandboxId", "sandbox_id"} {
				appendSandboxID(object[key])
			}
		}
	}
	appendSandboxIDsFromArray(payload["sandboxes"])
	if data, ok := payload["data"].(map[string]any); ok {
		appendSandboxIDsFromArray(data["sandboxes"])
	}
	if len(ids) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(ids))
	unique := ids[:0]
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		unique = append(unique, id)
	}
	return unique
}
