package gateway

import (
	"context"
	"net/http"

	"go.uber.org/zap"
)

// This file is the gateway's half of 阶段 3a: where user-facing REST goes.
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
// 🔴 An empty `rest_upstream_addr` used to be a second, supported position:
// the scheduler placed the call and forwarded it to whichever node it named,
// because every node was still the pre-split single process and answered
// those routes itself. That premise is retired — nodes run `aenv-node` now
// and never serve user-facing REST under any configuration — so an empty
// value would only ever produce a 404 from every node in the fleet, never a
// working rollback. `services/shared/config`'s `Config.Validate` refuses to
// load a gateway config with `rest_upstream_addr` (or `resume_addr`) empty
// for exactly that reason: see
// `TestTheRestUpstreamIsAlwaysSetBecauseNodesNeverServeRest`. By the time a
// `*Server` exists in this process, `s.restUpstream` is always a real
// address; the package-level `ServerOptions` type still *accepts* an empty
// one, purely so the test suite in this package can keep using an
// unconfigured server as a fixture for the data-plane routing paths —
// unrelated to this switch — that a bare `*Server` also exercises. There is
// no longer a second, node-routing position of `handleProxy` for that
// fixture to reach for a REST call: the fallback was deleted outright once
// nothing reachable from a validated deployment could take it, so a REST
// call against an unconfigured `*Server` now answers 502 rather than
// exercising anything.
//
// # 🔴 Why this was a value and not a manifest, while it was still a switch
//
// 3a's whole value, while it had two positions, was the shape of its
// rollback. The nodes stayed pre-split for the whole of it, so they never
// stopped being able to serve REST, so putting the traffic back was emptying
// this one value and rolling the gateway — seconds, and nothing touched the
// DaemonSet. 3b, where the DaemonSet moved to `aenv-node`, was the step whose
// rollback is a serial roll with an hour of grace per machine
// (`_sd-impl-phase3-role.md` §11.1, §11.2). Now that 3b has shipped and the
// nodes no longer serve REST in any configuration, that rollback shape no
// longer exists either way — see `services/README.md` for the rollback this
// value now has instead.
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
//     🔴 `GET /sandboxes` and `GET /v2/sandboxes` are *not* in that list, and
//     used not to be in this one either. They are aggregations over the nodes
//     rather than over the scheduler, and the api half owns the cluster ledger
//     they aggregate. While this switch still had two positions they moved
//     with it: unset, a fan-out that lived in cluster_list.go asked every node
//     for its own rows and merged them; set, the two routes were forwarded
//     here like any other REST call. That fan-out — and the position of the
//     switch it existed for — is gone: `isUserFacingRestRequest` claims both
//     routes unconditionally now, so they are always forwarded here, and there
//     is no longer a second code path in this package that builds a
//     cluster-wide listing out of the nodes at all.

// restUpstreamTarget labels which upstream served a REST call.
//
// 🔴 Only `restUpstreamAPI` is ever recorded now. `restUpstreamNode` is kept
// declared for dashboard and alert label-set compatibility with 阶段 3a, when
// both arms moved and a scrape's own control was `{upstream="node"}` flat at
// zero while `{upstream="api"}` climbed. That control no longer applies:
// handleProxy's node-routing fallback for user-facing REST is deleted, not
// merely unreachable behind a switch, so `{upstream="node"}` will not be
// exported by any build of this package — its absence is not itself evidence
// of anything any more, the way its presence would have been.
const (
	restUpstreamAPI  = "api"
	restUpstreamNode = "node"
)

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
