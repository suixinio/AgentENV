package gateway

import (
	"context"
	"net/http"

	"go.uber.org/zap"
)

// This file decides where user-facing REST goes.
//
// # There is one position, not two
//
// Every user-facing REST call — a create or a template build with no sandbox
// in it, or a sandbox-scoped call like `POST /sandboxes/{id}/pause` — goes to
// one address: the api half, which owns sandboxes and drives the machines
// itself over the node service. The gateway asks the scheduler nothing for
// these requests — placement is the api half's decision, and calling Schedule
// only to discard the answer would consume a placement and move the
// strategy's cursor for a request that never went there.
//
// `services/shared/config`'s `Config.Validate` refuses a gateway config with
// `rest_upstream_addr` empty: nodes run `aenv-node` and never serve
// user-facing REST, so an empty value could only draw 404s from every node in
// the fleet. See `TestTheRestUpstreamIsAlwaysSetBecauseNodesNeverServeRest`.
//
// `ServerOptions` still accepts an empty value so this package's own tests can
// use an unconfigured server as a fixture for the data-plane routing paths a
// bare `*Server` exercises; a REST call against that fixture answers 502.
//
// # 🔴 What this switch never carries
//
//   - **Data-plane traffic.** A request routed by proxy headers or by a sandbox
//     proxy domain goes to the node holding the sandbox whatever this says. The
//     api half runs no sandboxes; there is nothing there to proxy to. The
//     predicate below refuses those two route sources rather than relying on
//     the path, because a data-plane request can be addressed to any path at
//     all — including `/sandboxes/...` — and the routing headers are the only
//     thing that says what it is.
//   - **The gateway's own aggregations over the control plane.**
//     `GET /nodes` and `GET /registry/sandboxes` are answered by this process
//     out of the scheduler — the observed-node state and the paused registry —
//     and have already returned before this decision is reached. The api half
//     holds neither, so this switch does not move them and no position of it
//     ever will.
//
//     `GET /sandboxes` and `GET /v2/sandboxes` are *not* in that list. They
//     are aggregations over the nodes rather than over the scheduler, and the
//     api half owns the cluster ledger they aggregate:
//     `isUserFacingRestRequest` claims both routes unconditionally, so they
//     are always forwarded here, and no other code path in this package
//     builds a cluster-wide listing out of the nodes.

// restUpstreamTarget labels which upstream served a REST call.
//
// 🔴 `restUpstreamNode` (the label value "node") used to be declared here too,
// for dashboard and alert label-set compatibility with 阶段 3a, when both arms
// moved and a scrape's own control was `{upstream="node"}` flat at zero while
// `{upstream="api"}` climbed. handleProxy's node-routing fallback for
// user-facing REST is deleted, not merely unreachable behind a switch, so
// `{upstream="node"}` will not be exported by any build of this package —
// its absence is not itself evidence of anything any more, the way its
// presence would have been. The constant is gone; the value it named still
// appears as a literal in rest_upstream_test.go's regression sentinel, which
// asserts nothing may ever record against it again.
const restUpstreamAPI = "api"

// isUserFacingRestRequest reports whether this exchange is one of the routes the
// api half exists to answer: the `sandboxes`, `snapshots` and `templates`
// groups, which are exactly the routes `aenv-node` answers 404 on.
//
// Decided from the route source rather than from the path, because the route
// source is what already distinguishes "this request is about a sandbox" from
// "this request is addressed to something inside a sandbox". The two header
// checks are belt and braces on top of that: a request carrying data-plane
// routing headers can only have produced routeSourceHeader or routeSourceHost,
// so this is stating the rule twice rather than covering a case — and it is
// stated twice deliberately, because the cost of the two disagreeing one day is
// user traffic sent to a process with no sandbox on it.
func isUserFacingRestRequest(r *http.Request, hostRoute *hostRoute, source routeSource) bool {
	if hostRoute != nil || hasProxyRoutingHeaders(r.Header) {
		return false
	}
	switch source {
	case routeSourcePath, routeSourceSchedule:
		return true
	default:
		return false
	}
}

// forwardToRestUpstream hands one REST exchange to the api half.
//
// 🔴 No node, no assignment, no fencing plan, and each of the three is a
// decision rather than a simplification:
//
//   - No node, because none served it. `proxyRequest` reads the node only for
//     the debug header and for log fields, and a synthesised one there would put
//     a node id in an operator's logs that names nothing in the cluster.
//
//   - No assignment, because the gateway has nothing to record. A routing
//     projection maps a sandbox to the machine running it; the api half is not
//     one, and writing this address into a binding would send the next
//     data-plane request for that sandbox to a process that cannot serve it.
//
//     🔴 That had a visible consequence, and the owner has since taken it up.
//     A create forwarded here writes no binding, so the sandbox used to be
//     routable only once the node holding it said so — its next heartbeat
//     roster, or its ReportSandboxEvent, whichever landed first. Before this
//     switch the gateway closed that window itself, because it knew the node:
//     it had just picked it. Now it does not, and the only party that does is
//     the api half.
//
//     🔴 The api half now makes the call: `NodePlacement::record_placement`,
//     from the stub that has just had a create or a resume acknowledged
//     (`src/node_client/scheduler_placement.rs`). It is best-effort there, so
//     the heartbeat roster is still the repair path — but the window is no
//     longer the normal case. What it cost while it was: a sandbox created and
//     paused inside one heartbeat interval could be neither deleted nor
//     resumed, because a pause drops the api half's handle and every call after
//     it has to ask the scheduler where the sandbox is.
//
//   - No fencing plan, because no lookup was made and so no authoritative
//     incarnation exists to compare against. The zero plan stamps nothing and
//     reads nothing back, which is the same thing the control plane got before
//     this existed — the control plane resolved an incarnation and never acted
//     on it.
//
// The control-plane token is still stamped: `stampOutboundGatewayHeaders` does
// it unconditionally, and the api half sits behind the same gate the nodes do.
func (s *Server) forwardToRestUpstream(
	w http.ResponseWriter,
	r *http.Request,
	routingCtx context.Context,
	sandboxID string,
	longLived bool,
) {
	// The path is forwarded as it arrived. Only data-plane requests are
	// rewritten onto the upstream's /proxy sub-tree, and this function is
	// unreachable for those — see isUserFacingRestRequest.
	upstreamURL, err := joinUpstream(s.restUpstream, r.URL.Path, requestEscapedPath(r), r.URL.RawQuery)
	if err != nil {
		s.logger.Warn("could not build a url for the api half",
			zap.String("rest_upstream", s.restUpstream),
			zap.String("path", r.URL.Path),
			zap.Error(err),
		)
		http.Error(w, "invalid api upstream endpoint", http.StatusBadGateway)
		return
	}

	s.logger.Debug("gateway sent a rest request to the api half",
		zap.String("method", r.Method),
		zap.String("path", r.URL.Path),
		zap.String("sandbox_id", sandboxID),
		zap.String("upstream_endpoint", s.restUpstream),
	)

	upstreamCtx, cancelUpstream := requestContextForProxy(r, routingCtx, longLived)
	defer cancelUpstream()

	s.proxyRequest(
		w,
		r.Clone(upstreamCtx),
		r.Context(),
		upstreamURL,
		nil,
		proxyRequestOptions{
			assignment:       assignmentRouteNone,
			flushImmediately: longLived,
			sandboxID:        sandboxID,
		},
	)
}
