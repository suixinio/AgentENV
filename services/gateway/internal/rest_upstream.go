package gateway

import (
	"context"
	"net/http"

	"go.uber.org/zap"
)

// This file is the gateway's half of 阶段 3a: where user-facing REST goes.
//
// # The two positions
//
// Off — no `rest_upstream_addr` — is what has always run. A REST call with no
// sandbox in it (a create, a template build) is placed by the scheduler and
// forwarded to whichever node it named; a REST call about one sandbox
// (`POST /sandboxes/{id}/pause`) is resolved to the node holding it and
// forwarded there. Every node in the fleet answers those routes because every
// node is `--role all`.
//
// On, the same calls go to one address instead: the api half, which owns
// sandboxes and drives the machines itself over the node service. The gateway
// asks the scheduler nothing for these requests — placement is the api half's
// decision, and calling Schedule only to discard the answer would consume a
// placement and move the strategy's cursor for a request that never went there.
//
// # 🔴 Why this is a value and not a manifest
//
// 3a's whole value is the shape of its rollback. The nodes stay `--role all`
// for the whole of it, so they never stop being able to serve REST, so putting
// the traffic back is emptying this one value and rolling the gateway —
// seconds, and nothing touches the DaemonSet. 3b, where the DaemonSet moves to
// `--role node`, is the step whose rollback is a serial roll with an hour of
// grace per machine. Anything that makes enabling or disabling 3a a manifest
// change spends 3b's cost to buy 3a's, which is the one trade this staging
// exists to refuse (`_sd-impl-phase3-role.md` §11.1, §11.2).
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
//   - **The gateway's own aggregations.** `GET /sandboxes`, `GET /v2/sandboxes`,
//     `GET /nodes`, `GET /registry/sandboxes` are answered by this process out
//     of the scheduler and have already returned before this decision is
//     reached. Pointing them at the api half would change the cluster-listing
//     read path, which is a different release with a switch of its own.

// restUpstreamTarget labels which upstream served a REST call.
//
// 🔴 Both positions are counted, and that is the point of the series rather
// than an accident of its shape. 3a's acceptance criterion is "no user-facing
// REST is served by a node any more", and a counter that only counted the api
// side could not tell that apart from a gateway that had stopped receiving REST
// at all. With both, one scrape carries its own control: `{upstream="node"}`
// flat at zero is evidence exactly when `{upstream="api"}` in the same scrape is
// not.
const (
	restUpstreamAPI  = "api"
	restUpstreamNode = "node"
)

// isUserFacingRestRequest reports whether this exchange is one of the routes the
// api half exists to answer: the `sandboxes`, `snapshots` and `templates`
// groups, which are exactly the routes `--role node` answers 404 on.
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
//     🔴 That has a visible consequence, and it is the one to watch for on a
//     cluster. A create forwarded here writes no binding, so the sandbox is
//     routable only once the node holding it says so — its next heartbeat
//     roster, or its ReportSandboxEvent, whichever lands first. Before this
//     switch the gateway closed that window itself, because it knew the node:
//     it had just picked it. Now it does not, and the only party that does is
//     the api half. So this is an omission with an owner rather than a gap —
//     but until that owner records the assignment, expect a sub-heartbeat
//     window after every create in which a data-plane request for the new
//     sandbox falls through to the scheduler's roster path.
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
