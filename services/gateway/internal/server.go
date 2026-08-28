package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	schedulerv1 "agentenv/services/api/proto"
	"agentenv/services/gateway/internal/resume"
	"agentenv/services/shared/config"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	headerSandboxID    = "x-agentenv-sandbox-id"
	headerE2BSandboxID = "e2b-sandbox-id"
	headerTargetPort   = "x-agentenv-target-port"
	// The caller's envd access token. The gateway does not check it — it has no
	// record to check against — it forwards it to the half that does.
	headerEnvdAccessToken = "x-access-token"
	headerE2BTargetPort   = "e2b-sandbox-port"
	headerNodeID          = "x-agentenv-node-id"
	// headerProjectionTTLSecs is the node's own budget for how long the routing
	// projection of the sandbox it just started should live, in whole seconds.
	//
	// 🔴 Absent, unparseable and non-positive all mean the same thing here:
	// nothing to forward, and the scheduler falls back to its binding_ttl. None
	// of them may ever become "no expiry".
	headerProjectionTTLSecs    = "x-agentenv-projection-ttl-secs"
	maxRecordAssignmentTimeout = 5 * time.Second
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
	RequestTimeout      time.Duration
	MaxResponseSize     int64
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
	// ProjectionReader lets a sandbox route be answered out of the routing
	// projection instead of from a LookupNode call. Nil is the read switch in
	// its off position, and is the behaviour that shipped before it existed.
	ProjectionReader projectionReader
	// ProjectionAuthoritative is the gateway's half of the write-side switch:
	// resume and connect record an assignment, and the incarnation and TTL a
	// node reports are forwarded to the scheduler. Off forwards neither, which
	// is a projection write identical to today's.
	ProjectionAuthoritative bool

	// ResumeClient asks the API half to wake a paused sandbox when the routing
	// projection has no answer. Nil makes the gateway fall through to the
	// scheduler on every projection miss, exactly as it did before the
	// wake-up decision moved.
	//
	// 🔴 阶段 3a used to be able to leave this nil in production: nodes were
	// still the pre-split single process and could wake a sandbox themselves,
	// so nil was a supported rollback lever (`_sd-impl-phase3-role.md` §11.2).
	// That premise is retired — `aenv-node` has no wake-up surface of its own —
	// and `services/shared/config`'s `Config.Validate` now refuses to load a
	// gateway config with `resume_addr` empty, so `cmd/main.go` can no longer
	// construct a `*Server` with this nil. It stays nil-able here purely
	// because this package's own tests use an unconfigured `*Server` as their
	// baseline fixture for exercising the data-plane routing paths — the
	// projection, this wake-up client, and the cold-path LookupNode call
	// below — none of which has ever depended on RestUpstreamAddr.
	ResumeClient *resume.Client

	// RestUpstreamAddr sends every user-facing REST call to the api half.
	// Parsed in NewServer, so an address that cannot be used stops the
	// process rather than becoming a 502 per request.
	//
	// 🔴 The empty string is still accepted here — see rest_upstream.go for
	// why — but it is no longer a supported deployment position, and unlike
	// 阶段 3a it is not a rollback lever either: there is no longer a
	// node-routing fallback in handleProxy for an empty value to fall through
	// to. `services/shared/config`'s `Config.Validate` refuses to load a
	// gateway config with `rest_upstream_addr` empty, so no code reachable
	// from a validated deployment can leave this empty; only this package's
	// tests still construct a `*Server` that way, purely as a data-plane
	// fixture — a REST call against one now answers 502 rather than
	// exercising anything.
	RestUpstreamAddr string

	// ColdLookupTimeout bounds the LookupNode call a projection miss and an
	// undecided wake-up both fall through to (see the "if resp == nil" block
	// below the wake-up attempt in handleProxy), separately from
	// RequestTimeout. Zero (the default when unset by the caller) is resolved
	// to defaultColdLookupTimeout in NewServer, never left as "no timeout" —
	// a target that is merely unreachable (a Service with no ready endpoints,
	// or a black-holed route) must not be allowed to hold this call open for
	// whatever of RequestTimeout happens to be left, let alone for gRPC's own
	// connection backoff.
	//
	// 🔴 Deliberately its own setting rather than a fraction of
	// RequestTimeout: the two bound different things. RequestTimeout is a
	// budget for a legitimate, possibly slow end-to-end exchange (including a
	// proxied body); this is a budget for one read against a service that, in
	// the scenario this exists for, is not answering at all. Tying it to
	// RequestTimeout would mean nobody could tighten one without retuning the
	// other.
	//
	// 🔴 This used to be SchedulerFallbackTimeout, with a sibling
	// (SchedulerFallbackDisabled) that could skip the call entirely and a
	// client-selection field (QueryOnlySchedulerClient) that could point it at
	// a different scheduler than every other RPC in this package. Both are
	// deleted along with the Go scheduler they existed to decommission — the
	// call now always goes to the client NewServer was given, unconditionally
	// — but this timeout is not decommissioning-only scaffolding: it is the
	// ordinary protection every outbound RPC with a budget shorter than its
	// caller's needs, and removing it would silently widen this call's
	// failure window from a few seconds to RequestTimeout's 30-90s.
	ColdLookupTimeout time.Duration
}

type Server struct {
	logger         *zap.Logger
	scheduler      schedulerv1.SchedulerClient
	httpClient     *http.Client
	requestTimeout time.Duration
	maxRespSize    int64
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
	// Nil when no wake-up endpoint is configured — no longer reachable from a
	// validated deployment, see ServerOptions.ResumeClient.
	resumeClient *resume.Client
	// Empty is a data-plane-only test fixture, a position
	// `services/shared/config` no longer lets a deployed gateway reach; see
	// ServerOptions.RestUpstreamAddr and rest_upstream.go. Normalised to a
	// base URL once, at construction, for the reason executionFencing is: a
	// string re-read and re-interpreted at each call site is how one switch
	// ends up meaning two things.
	restUpstream string
	// See ServerOptions.ColdLookupTimeout. Never zero past NewServer — see
	// defaultColdLookupTimeout.
	coldLookupTimeout time.Duration
}

// defaultColdLookupTimeout is used when ServerOptions.ColdLookupTimeout is
// zero, which includes every test and config that predates this setting.
//
// 🔴 Picked to be well clear of a healthy LookupNode's latency (a single
// registry read, normally milliseconds) while being nowhere near
// RequestTimeout's default 30s or a TCP handshake's own retry ceiling
// (`tcp_syn_retries`'s default of 6 costs on the order of two minutes) — the
// two failure shapes this setting exists to stop this call from being
// exposed to.
const defaultColdLookupTimeout = 3 * time.Second

func NewServer(logger *zap.Logger, schedulerClient schedulerv1.SchedulerClient, options ServerOptions) (*Server, error) {
	sandboxProxyDomains, err := normalizeProxyDomains(options.SandboxProxyDomains)
	if err != nil {
		return nil, err
	}

	executionFencing, err := config.ParseGatewayExecutionFencing(options.ExecutionFencing)
	if err != nil {
		return nil, err
	}

	restUpstream, err := config.ParseRestUpstream(options.RestUpstreamAddr)
	if err != nil {
		return nil, err
	}

	// 🔴 Never left at zero: a zero timeout would make every cold-path call
	// fail before it started, which for every test and config written before
	// this setting existed silently changes today's behaviour instead of
	// preserving it.
	coldLookupTimeout := options.ColdLookupTimeout
	if coldLookupTimeout <= 0 {
		coldLookupTimeout = defaultColdLookupTimeout
	}

	return &Server{
		logger:                  logger,
		scheduler:               schedulerClient,
		httpClient:              &http.Client{},
		requestTimeout:          options.RequestTimeout,
		maxRespSize:             options.MaxResponseSize,
		debugMode:               options.DebugMode,
		sandboxProxyDomains:     sandboxProxyDomains,
		executionFencing:        executionFencing,
		controlPlaneToken:       strings.TrimSpace(options.ControlPlaneToken),
		projectionReader:        options.ProjectionReader,
		projectionAuthoritative: options.ProjectionAuthoritative,
		resumeClient:            options.ResumeClient,
		restUpstream:            restUpstream,
		coldLookupTimeout:       coldLookupTimeout,
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
		if isNodeListRequest(r) {
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

	// 阶段 3a, and since 阶段 4/R9 the only position left: every user-facing
	// REST exchange — including `GET /sandboxes` and `GET /v2/sandboxes`,
	// which used to fan out to every node when no api half was configured, see
	// cluster_list.go's history — goes to the api half. There is no longer a
	// node-routing fallback to fall through to: `services/shared/config`'s
	// `Config.Validate` has refused to load a gateway config with
	// `rest_upstream_addr` empty since 阶段 3b, so the branch that used to run
	// when it was empty was dead in every validated deployment and has been
	// deleted outright rather than kept as an unreachable option.
	if isUserFacingRestRequest(r, hostRoute, routeSource) {
		recordRestUpstream(restUpstreamAPI)
		s.forwardToRestUpstream(w, r, routingCtx, sandboxID, longLived)
		return
	}

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
			// 🔴 The cold path. The projection had no answer, so the sandbox
			// is paused, gone, or somewhere the projection has not caught up
			// with — and only the half that owns sandboxes can tell which.
			// Before this, the gateway asked the scheduler for a node and the
			// node woke the sandbox itself; that is the arrangement `--role
			// node` exists to end (§6.1).
			//
			// 🔴 Three states, and the third is why this is not a two-way
			// branch. "The API half says there is no such sandbox" ends the
			// request at 404. "The API half could not be asked" says nothing
			// about the sandbox at all, and falls through to exactly what this
			// gateway did before the wake-up client existed — so an api that
			// is down costs latency and not availability.
			if s.resumeClient != nil {
				woke := s.resumeClient.Wake(routingCtx, resume.Request{
					SandboxID:       sandboxID,
					TargetPort:      resumeTargetPort(r, hostRoute),
					EnvdAccessToken: r.Header.Get(headerEnvdAccessToken),
				})
				recordResumeAttempt(woke)
				switch woke.Verdict {
				case resume.VerdictWoken:
					s.logger.Info("woke a paused sandbox through the api half",
						zap.String("sandbox_id", sandboxID),
						zap.String("node_id", woke.NodeID),
						zap.String("execution_id", woke.ExecutionID),
					)
					resp = woke.LookupResponse()
					source = routeResolutionResumeWoken
				case resume.VerdictGone, resume.VerdictRefused:
					s.writeResumeError(w, sandboxID, woke)
					return
				case resume.VerdictUndecided:
					// Deliberately nothing. The scheduler call below is the
					// fallback, and it is the same call this gateway made for
					// every projection miss before this branch existed.
					s.logger.Warn("could not ask the api half to wake a sandbox; falling back to the scheduler",
						zap.String("sandbox_id", sandboxID),
						zap.String("resume_error", s.resumeReason(woke)),
					)
					recordRouteResolution(routeResolutionResumeUndecided)
				}
			}
		}

		if resp == nil {
			var err error
			resp, err = s.lookupNodeColdPath(routingCtx, sandboxID)
			if err != nil {
				// 🔴 Still the only source of a 404 or a 503 on the scheduler
				// path in this package.
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
	}
	// 🔴 No else. Every request that resolves no sandbox — a create, a cold
	// create, a template build — is a user-facing REST call, and the
	// isUserFacingRestRequest branch above has already forwarded it (and
	// returned) before this point is reached. The gateway used to build a
	// scheduling hint and call Schedule itself here, then proxy straight to
	// whichever node it named; placement is the api half's decision now
	// (`NodePlacement::record_placement`, called from the stub that has just
	// had a create or a resume acknowledged), and calling Schedule only to
	// discard the answer would consume a placement and move the strategy's
	// cursor for a request that never went there.
	//
	// What is left of this branch at runtime is a defensive no-op: `node`
	// stays nil for the one shape of request that can still reach here
	// without a sandbox — proxy routing headers present, but none of them
	// naming a sandbox id — and the upstream-URL build below fails closed
	// with a 502 rather than proxying anywhere.

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
			assignment:       assignmentRouteFor(hasSandbox, location),
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

// lookupNodeColdPath is the cold path a projection miss and an undecided
// wake-up both fall through to: the last resort that asks the scheduler
// client directly instead of answering from a projection or from the api
// half's own wake-up decision.
//
// 🔴 This used to also carry a disable switch (schedulerFallbackDisabled) and
// a separate client (queryOnlyScheduler) that could point this one call at a
// different address than every other Scheduler RPC in this package. Both are
// deleted along with the Go scheduler they existed to let this call stop
// depending on independently of `gateway.scheduler_addr` itself — the call is
// unconditional again and always goes to s.scheduler, exactly as every other
// RPC in this file does.
//
// What is left, and stays, is the timeout: an unreachable-but-not-yet-failed
// target must not be allowed to hold this call open for whatever of the
// request's overall budget happens to be left, or for gRPC's own connection
// backoff, so it runs under its own deadline (coldLookupTimeout) rather than
// only routingCtx's. Firing this cap is distinguished from routingCtx's own
// (pre-existing) deadline firing by checking ctx's own error first — if the
// caller's context is already done, this cap did not decide anything, and the
// error it returns is left exactly as it would have been before this method
// existed.
func (s *Server) lookupNodeColdPath(ctx context.Context, sandboxID string) (*schedulerv1.LookupNodeResponse, error) {
	coldCtx, cancel := context.WithTimeout(ctx, s.coldLookupTimeout)
	defer cancel()

	rpcStart := time.Now()
	resp, err := s.scheduler.LookupNode(coldCtx, &schedulerv1.LookupNodeRequest{SandboxId: sandboxID})
	recordGatewaySchedulerRPC("LookupNode", rpcStart, err)
	if err == nil {
		return resp, nil
	}

	// Only our own cap firing is reclassified. If ctx (routingCtx) is also
	// done, this timeout did not decide anything — some larger, pre-existing
	// deadline did, and that keeps behaving exactly as it always has.
	if ctx.Err() == nil && coldCtx.Err() != nil {
		recordGatewayColdLookupTimeout()
		s.logger.Warn("cold-path LookupNode timed out",
			zap.String("sandbox_id", sandboxID),
			zap.Duration("timeout", s.coldLookupTimeout),
			zap.Error(err),
		)

		return nil, status.Error(codes.Unavailable, fmt.Sprintf(
			"cold-path LookupNode did not answer within %s; the api half may be scaled down or "+
				"unreachable (this is not the sandbox's fault)",
			s.coldLookupTimeout,
		))
	}

	return nil, err
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
	reason := s.schedulerReason(st)
	switch st.Code() {
	case codes.InvalidArgument:
		http.Error(w, reason, http.StatusBadRequest)
	case codes.NotFound:
		http.Error(w, reason, http.StatusNotFound)
	case codes.Unavailable, codes.FailedPrecondition:
		http.Error(w, reason, http.StatusServiceUnavailable)
	default:
		http.Error(w, "scheduler error", http.StatusBadGateway)
	}
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
// 🔴 The scheduler's message is deliberately forwarded — `local_only on node
// "node-a"` and `paused registry is not ready` are reasons a client acts on,
// and the tests above pin them. What must not be forwarded is the text a
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

// assignmentRoute says whether this exchange writes a routing projection.
//
// 🔴 Only two states remain. A third, assignmentRoutePath — the assignment is
// for the sandbox this request was already routed for, used by resume,
// connect and any other control-plane call the scheduler resolved off the
// paused registry — existed for as long as those calls could be routed
// straight to a node by this gateway. They cannot be any more:
// isUserFacingRestRequest forwards every one of them to the api half before
// assignmentRouteFor is ever reached, and the api half records its own
// placements (`NodePlacement::record_placement`). Reintroducing a
// path-routed control-plane call here would mean this predicate has stopped
// being true.
type assignmentRoute int

const (
	// assignmentRouteNone: this exchange writes no assignment.
	assignmentRouteNone assignmentRoute = iota
	// assignmentRouteResponse: the sandbox is named in the response, read from
	// a header if the node put one there, otherwise from the body.
	assignmentRouteResponse
)

// assignmentRouteFor decides whether this exchange writes a routing
// projection.
//
// 🔴 Only ever called for data-plane traffic — routed by a proxy header or by
// a sandbox proxy host name. Every user-facing REST call (create, fork,
// resume, connect, and the rest of the sandbox control surface) is forwarded
// to the api half by handleProxy's isUserFacingRestRequest branch before this
// is reached, so there is no create or control-plane path left to
// distinguish here; see the assignmentRoute doc comment. What remains is the
// one case that predates the REST switch entirely and is unrelated to it: a
// sandbox the scheduler resolved off the paused registry (PLACED or PINNED)
// is about to be held by a node nothing has recorded against, and that
// binding still has to be written from the data-plane response that reaches
// it first.
func assignmentRouteFor(hasSandbox bool, location schedulerv1.SandboxLocation) assignmentRoute {
	if hasSandbox && locationNeedsAssignment(location) {
		return assignmentRouteResponse
	}
	return assignmentRouteNone
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
// 🔴 The distinctions here are the same ones writeSchedulerError guards, and
// they matter for the same reason. A 404 on a resume is the end of that sandbox
// as far as any client is concerned — the platform's contract for it is
// "rebuild from the template", which resets the user's workspace — so it is
// spent only on a verdict that positively says the sandbox is gone.
//
// 🔴 A FailedPrecondition is a 503 and never a retry against another node. For
// a sandbox whose snapshot never reached shared storage there is no second copy
// to try: waking it elsewhere would not fail, it would succeed, by rebuilding
// from an older snapshot and losing the last pause. Retry-After tells the
// client to wait for the machine that has the bytes, which is the only correct
// thing to wait for.
func (s *Server) writeResumeError(w http.ResponseWriter, sandboxID string, result resume.Result) {
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

	switch code {
	case codes.NotFound:
		http.Error(w, reason, http.StatusNotFound)
	case codes.PermissionDenied:
		http.Error(w, reason, http.StatusForbidden)
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
			http.Error(w, reason, http.StatusGone)
			return
		}
		// Retry-After on the transient one only. `transition_in_progress`
		// clears in a moment; a pin refusal clears when a machine comes back,
		// which may be never, and a small Retry-After on that would have the
		// client hammering a node that is not coming.
		if result.Reason == resumeReasonTransitionInProgress {
			w.Header().Set("Retry-After", "1")
		}
		http.Error(w, reason, http.StatusServiceUnavailable)
	case codes.InvalidArgument:
		http.Error(w, reason, http.StatusBadRequest)
	default:
		// 🔴 Unimplemented lands here, and that is load-bearing: §12 P3's
		// control C stubs this RPC out with Unimplemented and requires the data
		// plane to *fail*. If it fell through to the scheduler the probe would
		// pass while a second wake-up path was quietly doing the work, which is
		// the exact thing the control is designed to detect.
		http.Error(w, "sandbox wake-up failed", http.StatusBadGateway)
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
