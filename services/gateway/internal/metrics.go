package gateway

import (
	"bufio"
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"time"

	schedulerv1 "agentenv/services/api/proto"
	"agentenv/services/gateway/internal/resume"
	"agentenv/services/shared/observability"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc/codes"
)

var (
	gatewayHTTPDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "agentenv_gateway_http_request_duration_seconds",
			Help:    "Gateway HTTP request duration by method, route, route source, and status.",
			Buckets: observability.DurationBuckets,
		},
		[]string{"method", "route", "route_source", "status"},
	)
	gatewayUpstreamProxyDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "agentenv_gateway_upstream_proxy_duration_seconds",
			Help:    "Gateway upstream reverse proxy duration by route and status.",
			Buckets: observability.DurationBuckets,
		},
		[]string{"route", "status"},
	)
	gatewaySchedulerRPCDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "agentenv_gateway_scheduler_rpc_duration_seconds",
			Help:    "Gateway scheduler RPC duration by RPC and status.",
			Buckets: observability.DurationBuckets,
		},
		[]string{"rpc", "status"},
	)
	// How each resolved sandbox request was located. This is deliberately its
	// own series rather than another route_source value: route_source answers
	// "where did the sandbox id come from", which is orthogonal and still
	// needed. Before this existed, a resume served from a binding and one
	// rebuilt on a node that had never held the sandbox were indistinguishable
	// in every metric the gateway published.
	gatewaySandboxLocations = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "agentenv_gateway_sandbox_location_total",
			Help: "Resolved sandbox requests by how the scheduler located the sandbox.",
		},
		[]string{"location"},
	)
	// What the routing layer did about the incarnation, per resolved request.
	//
	// The refused_* decisions should read zero on a healthy cluster, which makes
	// them alertable as they are. The unfenced_* ones are the more useful half:
	// their absolute value is the size of the coverage gap, and each names a
	// different reason for it — no authority from the centre, or a node that does
	// not answer with one. Before this existed there was no way to tell a gateway
	// that was fencing everything from one that was fencing nothing.
	gatewayExecutionFencing = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "agentenv_gateway_execution_fencing_total",
			Help: "Resolved sandbox requests by plane and by what the routing layer decided about the sandbox incarnation.",
		},
		[]string{"plane", "decision"},
	)
	// How each resolved sandbox route was answered: out of the routing
	// projection, or by asking the scheduler.
	//
	// 🔴 This exists because turning the direct read on drives the scheduler's
	// own lookup counters towards zero, and those counters were half of a
	// two-sided reconciliation. This is the other half restored on this side.
	// In a window where nothing has changed:
	//
	//	Δ{redis_miss} + Δ{redis_error} ≈ Δ agentenv_api_lookup_node_total
	//
	// A persistent disagreement means one of the two sides is counting
	// something it is not doing.
	//
	// 🔴 redis_error is not a failure mode of the request. It is the count of
	// times the fallback earned its place: the request still went to the
	// scheduler and the client saw nothing. Alert on its rate, never on its
	// existence.
	gatewayRouteResolution = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "agentenv_gateway_route_resolution_total",
			Help: "Resolved sandbox routes by where the answer came from: a routing projection hit, a miss, a projection read error, or the scheduler.",
		},
		[]string{"source"},
	)
	// Wake-up attempts against the API half, by outcome.
	//
	// 🔴 The label vocabulary is deliberately the API half's own
	// (`agentenv_api_resume_grpc_total{result}`), so the two series can be
	// compared label-for-label. §12 P3's control B asserts them 逐条相等, and
	// that check only means something if both sides spell the same outcome the
	// same way — otherwise a disagreement reads as a naming difference and gets
	// waved through.
	//
	// 🔴 The set is closed by resumeResultLabel. The label's value arrives over
	// the wire from another process, and a label value an attacker or a newer
	// build can choose is an unbounded time series, which is a memory leak in
	// this process and in Prometheus.
	gatewayResumeAttempts = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "agentenv_gateway_resume_total",
			Help: "Wake-up attempts against the api half, by outcome, using the api half's own result vocabulary.",
		},
		[]string{"result"},
	)
	// Cold-path LookupNode calls (a projection miss and an undecided wake-up
	// both fall through to lookupNodeColdPath) that hit their own timeout
	// before the RPC returned — recorded in addition to, not instead of,
	// gatewaySchedulerRPCDuration, which already counts the RPC by status
	// including this one's eventual DeadlineExceeded. This series exists only
	// to answer "did the cold-path cap fire", which the RPC-status series
	// cannot answer on its own since a caller-side deadline firing looks
	// identical to it there.
	gatewayColdLookupTimeout = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "agentenv_gateway_cold_lookup_timeout_total",
			Help: "Cold-path LookupNode calls (a projection miss or an undecided wake-up) that hit their own timeout before the RPC returned.",
		},
	)
	// User-facing REST exchanges. The api half is the only upstream that can
	// serve them, so this counter carries no label naming one.
	gatewayRestUpstream = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "agentenv_gateway_rest_upstream_total",
			Help: "User-facing REST exchanges served by the api half.",
		},
	)
)

type statusRecorder struct {
	http.ResponseWriter
	status      int
	routeSource routeSource
}

func (r *statusRecorder) WriteHeader(status int) {
	if r.status != 0 {
		return
	}
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func (r *statusRecorder) Write(body []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	return r.ResponseWriter.Write(body)
}

func (r *statusRecorder) Flush() {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	if flusher, ok := r.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (r *statusRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hijacker, ok := r.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("response writer does not support hijack")
	}
	conn, brw, err := hijacker.Hijack()
	if err == nil && r.status == 0 {
		r.status = http.StatusSwitchingProtocols
	}
	return conn, brw, err
}

func (r *statusRecorder) statusCode() int {
	if r.status == 0 {
		return http.StatusOK
	}
	return r.status
}

func (r *statusRecorder) setRouteSource(source routeSource) {
	r.routeSource = source
}

func (r *statusRecorder) routeSourceLabel() string {
	if r.routeSource == "" {
		return "unknown"
	}
	return string(r.routeSource)
}

type routeSourceRecorder interface {
	setRouteSource(routeSource)
}

func setGatewayRouteSource(w http.ResponseWriter, source routeSource) {
	if recorder, ok := w.(routeSourceRecorder); ok {
		recorder.setRouteSource(source)
	}
}

func (s *Server) instrumentGatewayHTTP(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.isLocalGatewayEndpointRequest(r) {
			next.ServeHTTP(w, r)
			return
		}
		// Sandbox-routed /health and /metrics requests are user traffic, so
		// keep them instrumented like any other proxy call.

		method := gatewayMethodLabel(r.Method)
		route := gatewayRouteLabel(r.URL.Path)
		recorder := &statusRecorder{ResponseWriter: w}
		start := time.Now()
		next.ServeHTTP(recorder, r)
		status := httpStatusLabel(recorder, r.Context())

		gatewayHTTPDuration.WithLabelValues(method, route, recorder.routeSourceLabel(), status).Observe(time.Since(start).Seconds())
	})
}

func (s *Server) isLocalGatewayEndpointRequest(r *http.Request) bool {
	if (r.URL.Path != "/health" && r.URL.Path != "/metrics") || hasProxyRoutingHeaders(r.Header) {
		return false
	}
	hostRoute, hostRouteErr := parseHostRoute(r.Host, s.sandboxProxyDomains)
	return hostRoute == nil && hostRouteErr == nil
}

func recordGatewaySchedulerRPC(rpc string, start time.Time, err error) {
	status := observability.GRPCStatusLabel(err)
	gatewaySchedulerRPCDuration.WithLabelValues(rpc, status).Observe(time.Since(start).Seconds())
}

func recordGatewaySandboxLocation(location schedulerv1.SandboxLocation) {
	gatewaySandboxLocations.WithLabelValues(gatewaySandboxLocationLabel(location)).Inc()
}

// recordExecutionFencing takes both labels from constants in
// execution_fencing.go, which is what keeps the label set closed without a
// mapping function: nothing a scheduler or a node sends reaches this call.
func recordExecutionFencing(plane fencingPlane, decision string) {
	if decision == "" {
		return
	}
	gatewayExecutionFencing.WithLabelValues(string(plane), decision).Inc()
}

// The four ways a sandbox route gets answered. Closed set, and every path
// through the read block lands on exactly one.
const (
	routeResolutionRedisHit   = "redis_hit"
	routeResolutionRedisMiss  = "redis_miss"
	routeResolutionRedisError = "redis_error"
	routeResolutionScheduler  = "scheduler"
	// A route the api half produced by waking the sandbox.
	routeResolutionResumeWoken = "resume_woken"
	// A wake-up nobody could be asked about, which fell through to the
	// scheduler. Like redis_error, this is not a failure mode of the request:
	// it is the count of times the fallback earned its place. Alert on its
	// rate, never on its existence.
	routeResolutionResumeUndecided = "resume_undecided"
)

// The refusal reasons the api half can send, as a closed set.
//
// Kept in step with `PinRefusalReason::as_str` and `REASON_TRANSITION_IN_PROGRESS`
// in `src/api/grpc/resume.rs`. A reason this build does not recognise is
// counted as "other" rather than as itself — see resumeResultLabel.
const (
	resumeReasonTransitionInProgress = "transition_in_progress"
	resumeReasonOriginNotReporting   = "origin_not_reporting"
	resumeReasonOriginNotAccepting   = "origin_not_accepting_work"
	resumeReasonOriginNotReachable   = "origin_not_reachable_from_here"
	resumeReasonOriginUnclassified   = "origin_unclassified"
	// The sandbox was created with autoResume off. Unlike every other reason
	// here this one is not a failure: it is the sandbox behaving as asked, and
	// writeResumeError turns it into a 410 rather than the 503 its
	// FailedPrecondition code would otherwise earn.
	resumeReasonAutoResumeDisabled = "auto_resume_disabled"
)

var knownResumeReasons = map[string]struct{}{
	resumeReasonTransitionInProgress: {},
	resumeReasonOriginNotReporting:   {},
	resumeReasonOriginNotAccepting:   {},
	resumeReasonOriginNotReachable:   {},
	resumeReasonOriginUnclassified:   {},
	resumeReasonAutoResumeDisabled:   {},
}

// The gRPC codes this build maps to a label, spelled as the api half spells
// them. Anything else is "other".
var resumeCodeLabels = map[codes.Code]string{
	codes.OK:                "ok",
	codes.NotFound:          "not_found",
	codes.PermissionDenied:  "permission_denied",
	codes.ResourceExhausted: "resource_exhausted",
	codes.Unavailable:       "unavailable",
	codes.DeadlineExceeded:  "unavailable",
	codes.Canceled:          "unavailable",
	codes.InvalidArgument:   "invalid_argument",
	codes.Internal:          "internal",
	codes.Unimplemented:     "unimplemented",
}

// resumeResultLabel names one wake-up outcome, from a closed set.
//
// 🔴 The refusal reason wins over the code when there is one, because that is
// the distinction that matters: three of the reasons share
// FAILED_PRECONDITION and mean completely different waits. Falling back to the
// code would merge "somebody else is mid-resume, try in a second" with "the
// only machine holding these bytes is gone, and may never come back".
func resumeResultLabel(result resume.Result) string {
	if result.Verdict == resume.VerdictWoken {
		return "ok"
	}
	if result.Reason != "" {
		if _, ok := knownResumeReasons[result.Reason]; ok {
			return result.Reason
		}
		// A reason this build does not know. Counted, but not as itself: see
		// the note on gatewayResumeAttempts.
		return "other"
	}
	if result.Status == nil {
		return "unavailable"
	}
	if label, ok := resumeCodeLabels[result.Status.Code()]; ok {
		return label
	}
	return "other"
}

func recordResumeAttempt(result resume.Result) {
	gatewayResumeAttempts.WithLabelValues(resumeResultLabel(result)).Inc()
}

func recordGatewayColdLookupTimeout() {
	gatewayColdLookupTimeout.Inc()
}

func recordRouteResolution(source string) {
	gatewayRouteResolution.WithLabelValues(source).Inc()
}

// recordRestUpstream counts one user-facing REST exchange about to be handed
// to the api half.
func recordRestUpstream() {
	gatewayRestUpstream.Inc()
}

// gatewaySandboxLocationLabel keeps the label set closed. An enum value this
// build does not know is reported as "other" rather than as its number, so a
// newer scheduler cannot grow the cardinality of this series.
func gatewaySandboxLocationLabel(location schedulerv1.SandboxLocation) string {
	switch location {
	case schedulerv1.SandboxLocation_SANDBOX_LOCATION_BOUND:
		return "bound"
	case schedulerv1.SandboxLocation_SANDBOX_LOCATION_PLACED:
		return "placed"
	case schedulerv1.SandboxLocation_SANDBOX_LOCATION_PINNED:
		return "pinned"
	case schedulerv1.SandboxLocation_SANDBOX_LOCATION_UNSPECIFIED:
		// An older scheduler, which could only ever answer from a binding.
		return "unspecified"
	default:
		return "other"
	}
}

func recordGatewayUpstreamProxy(route string, start time.Time, w http.ResponseWriter, ctx context.Context) {
	gatewayUpstreamProxyDuration.WithLabelValues(route, httpStatusLabel(w, ctx)).Observe(time.Since(start).Seconds())
}

func gatewayMethodLabel(method string) string {
	switch method {
	case http.MethodGet:
		return "GET"
	case http.MethodPost:
		return "POST"
	case http.MethodPut:
		return "PUT"
	case http.MethodPatch:
		return "PATCH"
	case http.MethodDelete:
		return "DELETE"
	case http.MethodHead:
		return "HEAD"
	case http.MethodOptions:
		return "OPTIONS"
	default:
		return "OTHER"
	}
}

// httpStatusLabel derives the status bucket for a recorded response. ctx must
// be the inbound request context (r.Context()): its cancellation means the
// client disconnected, not that the gateway timed out (an upstream timeout is
// context.DeadlineExceeded and is turned into a 504 by the proxy ErrorHandler).
// So when no status was written and ctx was cancelled, it reports
// "client_closed" (nginx-style 499) instead of counting the request as 2xx.
func httpStatusLabel(w http.ResponseWriter, ctx context.Context) string {
	recorder, ok := w.(*statusRecorder)
	if !ok {
		return "other"
	}
	if recorder.status == 0 && errors.Is(ctx.Err(), context.Canceled) {
		return "client_closed"
	}
	status := recorder.statusCode()
	switch {
	case status >= 100 && status < 200:
		return "1xx"
	case status >= 200 && status < 300:
		return "2xx"
	case status >= 300 && status < 400:
		return "3xx"
	case status >= 400 && status < 500:
		return "4xx"
	case status >= 500 && status < 600:
		return "5xx"
	default:
		return "other"
	}
}

func gatewayRouteLabel(path string) string {
	trimmed := strings.TrimRight(strings.TrimSpace(path), "/")
	if trimmed == "" {
		trimmed = "/"
	}

	switch trimmed {
	case "/sandboxes", "/sandboxes-cold", "/v2/sandboxes", "/nodes":
		return trimmed
	}

	parts := strings.Split(strings.Trim(trimmed, "/"), "/")
	if len(parts) == 0 {
		return "unmatched"
	}
	switch parts[0] {
	case "sandboxes":
		if len(parts) == 2 {
			return "/sandboxes/{sandbox_id}"
		}
		if len(parts) == 3 {
			switch parts[2] {
			case "snapshots", "custom-extension-params", "pause", "resume", "fork":
				return "/sandboxes/{sandbox_id}/" + parts[2]
			}
		}
	case "nodes":
		if len(parts) == 2 {
			return "/nodes/{node_id}"
		}
	case "proxy":
		return "/proxy/*"
	}
	return "unmatched"
}
