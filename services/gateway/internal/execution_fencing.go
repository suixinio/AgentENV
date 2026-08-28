package gateway

import (
	"encoding/json"
	"fmt"
	"net/http"

	schedulerv1 "agentenv/services/api/proto"
	"agentenv/services/shared/config"
	"agentenv/services/shared/routing"

	"go.uber.org/zap"
)

// The four headers this file owns, all lower case to match the routing headers
// the gateway has always spoken (x-agentenv-sandbox-id, x-agentenv-target-port).
const (
	// headerControlPlane proves to a node that a request came through the
	// gateway. Stamped on everything the gateway forwards, read by the node's
	// control-plane gate.
	headerControlPlane = "x-agentenv-control-plane"
	// headerExpectExecutionID says which incarnation the gateway routed this
	// request against. Stamped only on data-plane requests, only when the
	// scheduler answered with an authoritative one, and only under enforce —
	// the only mode besides off that remains.
	//
	// 🔴 A third mode, observe, used to reach this path without stamping it:
	// stamping is what arms the node's refusal, and a refusal the node has
	// already produced cannot be withdrawn by a gateway that was only meant to
	// be watching — it can only be translated, which is still a 409 the client
	// did not get before. That reasoning is why observe existed as a distinct
	// state at all; the state itself was deleted once the rollout it existed
	// for finished (deploy/k8s/base/kustomization.yaml's
	// execution-fencing-config comment records the cluster reaching enforce).
	headerExpectExecutionID = "x-agentenv-expect-execution-id"
	// headerExecutionID is the node's echo: the incarnation alive on that node
	// for this sandbox, right now. The node sends it on refusals as well as on
	// passes, which is what makes its absence mean "this node does not do
	// fencing" rather than "this node had nothing to say".
	headerExecutionID = "x-agentenv-execution-id"
	// headerRefusal accompanies the node's 412 and is an internal signal.
	//
	// 🔴 It never reaches a client, and neither does the 412: see
	// refusalNodeStatusCode below.
	headerRefusal = "x-agentenv-refusal"
)

// refusalCodeExecutionSuperseded is the one string this refusal has, anywhere it
// has to be written down — the header value, the JSON body, the log field.
//
// One string, not three: diagnosing a refusal means following it across the
// gateway, the node and the scheduler, and three words for one fact cannot be
// grepped into a single trail. The Go and Rust types are still called
// ExecutionFenced; those are internal symbols that never go on the wire.
const refusalCodeExecutionSuperseded = "sandbox_execution_superseded"

// refusalNodeStatusCode is what a node answers with when it refuses a request
// stamped with an incarnation older than the one it is running.
//
// 412 is an internal signal chosen because it is the one free slot on the node's
// /proxy status table. 404 and 410 are both already spoken there, and 409 is the
// shape the client gets instead — the translation in fenceProxyResponse is what
// keeps the two apart.
const refusalNodeStatusCode = http.StatusPreconditionFailed

// fencingMode is the gateway half of the three switches; the type and its
// parsing live in shared/config so the value the process validated at startup
// and the value this package branches on cannot be two different things.
type fencingMode = config.GatewayExecutionFencing

const (
	fencingOff     = config.GatewayExecutionFencingOff
	fencingEnforce = config.GatewayExecutionFencingEnforce
)

// fencingPlane splits the two kinds of traffic that reach handleProxy, because
// the answer to "may this be refused" is different for each and does not depend
// on anything else.
type fencingPlane string

const (
	// fencingPlaneData is proxied sandbox traffic, reached by routing header or
	// by proxy host name. This is the interactive traffic A5 protects.
	fencingPlaneData fencingPlane = "data"
	// fencingPlaneControl is pause / resume / delete / fork and the rest of the
	// sandbox control surface.
	//
	// 🔴 Not one of these is ever refused here, and not one of them carries the
	// expect header. resume is by definition the moment the incarnation changes,
	// so refusing it in front would kill the operation that mints the new one;
	// and the registry's own transaction is the authoritative check, which the
	// gateway's copy — read from a lookup that may be a heartbeat behind — can
	// only contradict.
	fencingPlaneControl fencingPlane = "control"
)

// The closed set of decisions this gateway records, one per resolved request.
//
// Closed on purpose, the same way the sandbox-location labels are: every value
// is a constant in this file, so a newer scheduler or node cannot grow the
// cardinality of the series by answering something unexpected.
const (
	// fencingDecisionOff — the switch is off; nothing was stamped or read.
	fencingDecisionOff = "off"
	// fencingDecisionObserved — control plane. Resolved and logged, never acted on.
	fencingDecisionObserved = "observed"
	// fencingDecisionPending — the node is about to mint a new incarnation
	// (PLACED / PINNED). Nothing to expect yet.
	fencingDecisionPending = "pending"
	// fencingDecisionUnfencedNoAuthority — the scheduler cannot name the current
	// incarnation. Passed through, and counted: the absolute value of this
	// series is the size of the coverage gap left by not giving running
	// sandboxes a registry row, which is a decision with a bill, not noise.
	fencingDecisionUnfencedNoAuthority = "unfenced_no_authority"
	// fencingDecisionUnfencedNodeSilent — the node returned no echo, so it does
	// not implement fencing. This is the only signal that the first gate was
	// rolled back or never deployed; it must read zero before enforce.
	fencingDecisionUnfencedNodeSilent = "unfenced_node_silent"
	// fencingDecisionUnfencedNodeAhead — the node's incarnation is newer than
	// the one the gateway expected.
	//
	// 🔴 Passed through, always. A node ahead of the centre is an ordinary
	// same-machine changeover: idle pause runs on a one second tick and the data
	// plane resumes on demand, so pause→resume on one node happens constantly,
	// and refusing it would turn every one of them into a 409.
	fencingDecisionUnfencedNodeAhead = "unfenced_node_ahead"
	// fencingDecisionEnforcedPass — expected and observed named the same
	// incarnation.
	fencingDecisionEnforcedPass = "enforced_pass"
	// fencingDecisionRefusedPreflight — the node refused before doing anything.
	// This is the only refusal that happens before the side effects.
	//
	// 🔴 Only enforce can produce it: it takes a stamp to arm the node's gate,
	// and off never reaches decideFencing's compare/stamp branch at all. A
	// non-zero count while the fleet is on off means something other than this
	// gateway put an expect header on the wire.
	fencingDecisionRefusedPreflight = "refused_preflight"
	// fencingDecisionRefusedEcho — the gateway caught it on the way back, after
	// the node had already run it. Detection, not prevention.
	fencingDecisionRefusedEcho = "refused_echo"
)

// Who refused, as it appears in the body. Two values, so a client that sees a
// refusal can tell a preflight one (nothing ran) from an echo one (it ran).
const (
	refusedByNode    = "node"
	refusedByGateway = "gateway"
)

// fencingStageGatewayRoute names this segment of the three-segment trail in
// logs. The other two are registry_write and node_proxy.
const fencingStageGatewayRoute = "gateway_route"

// fencingPlan is what one lookup answer licenses, decided once, before the
// request goes out, and carried through to the response.
type fencingPlan struct {
	// stamp says whether the expect header goes on the outbound request — gate 1,
	// which is a refusal delegated to the node.
	//
	// 🔴 Only enforce sets it. The delegation is one-way: once a node has answered
	// 412 the gateway can translate that refusal but cannot undo it, so a mode
	// that promises to refuse nothing cannot afford to hand it out.
	stamp bool
	// compare says whether the node's echo is read back and measured — gate 2.
	//
	// It is deliberately not the same field as stamp, even though the two
	// states remaining today (off, enforce) always agree on it: off leaves
	// both false by returning out of decideFencing before either is set, and
	// enforce sets both true together. The node echoes the incarnation it is
	// actually running on every response, whether or not anything was
	// expected of it, so gate 2 needs nothing from gate 1 — that
	// independence is what let a third mode, observe, measure every mismatch
	// enforce would refuse while putting no refusal anywhere in reach,
	// through the rollout that proved enforce was safe to turn on
	// everywhere. That rollout is over and observe is gone, but the fields
	// stay separate on the same reasoning: a mode that compares without
	// refusing is one gate 2 alone should be able to express again.
	compare bool
	// refuse says whether a mismatch caught by gate 2 ends the request rather
	// than just counting it.
	refuse bool
	// expect is the incarnation the scheduler named, whether or not it may be
	// used. It is never empty when compare is true, and it is populated for the
	// control plane too, where it exists only to be logged.
	//
	// 🔴 Read it through stampedExecutionID for anything that goes on the wire.
	expect string
	// decision is the label to record when the request phase settles the matter
	// on its own. Empty means the answer comes back from the node.
	decision string
	// authority is carried for the log line only.
	authority schedulerv1.ExecutionAuthority
}

// stampedExecutionID is the only way the expect value reaches a header.
//
// The plan carries a resolved incarnation the wire must not always see — the
// control plane resolves and logs one but never delegates it to a node. Going
// through this accessor is what stops that case from being stamped by a call
// site that only meant to read "the expect". (A second such case, observe —
// which compared against an incarnation but never delegated it either — used
// to share this reasoning; the mode is retired, and off never resolves an
// expect to withhold in the first place.)
func (p fencingPlan) stampedExecutionID() string {
	if !p.stamp {
		return ""
	}
	return p.expect
}

// fencingPlaneFor maps a resolved route to its plane.
//
// The two data-plane entries — routing header and proxy host name — have already
// converged into one sandbox id and one route source by the time handleProxy
// resolves anything, so both are covered by this single answer rather than by
// two parallel branches that could drift.
func fencingPlaneFor(source routeSource) fencingPlane {
	if isDataPlaneRouteSource(source) {
		return fencingPlaneData
	}
	return fencingPlaneControl
}

// decideFencing turns one LookupNode answer into one plan. Pure: every input is
// a value and it performs no IO, so the whole decision table can be enumerated
// in a test and every mutation of it shows up in exactly one place.
func decideFencing(mode fencingMode, plane fencingPlane, resp *schedulerv1.LookupNodeResponse) fencingPlan {
	// 🔴 The rollback is this line and nothing else. Spreading the check across
	// the stamping site, the response site and the metrics site would make "off"
	// itself a piece of new code that has to be verified before it can be
	// trusted, which is the opposite of what a rollback is for.
	if mode == fencingOff {
		return fencingPlan{decision: fencingDecisionOff}
	}

	authority := resp.GetExecutionAuthority()
	if plane != fencingPlaneData {
		// Resolved and recorded, so an operator can see which incarnation a
		// control-plane call was routed against — but not put on the wire. Once
		// it is a header somebody will naturally have the node check it too, and
		// then there are two gates disagreeing, with the staler one in front.
		return fencingPlan{
			decision:  fencingDecisionObserved,
			expect:    normalizeExecutionID(resp.GetExecutionId()),
			authority: authority,
		}
	}

	switch authority {
	case schedulerv1.ExecutionAuthority_EXECUTION_AUTHORITY_REGISTRY:
		expect := normalizeExecutionID(resp.GetExecutionId())
		if expect == "" {
			// The contract says REGISTRY never arrives empty. If it does, the
			// answer is unusable rather than authoritative — stamping an empty
			// expect would ask the node to compare against nothing.
			return fencingPlan{decision: fencingDecisionUnfencedNoAuthority, authority: authority}
		}
		// 🔴 mode is guaranteed fencingEnforce here, not merely likely: off
		// already returned at the top of this function, and no other value
		// exists any more. A third value, observe, used to reach this same
		// branch and arm only gate 2 (compare) while leaving gate 1 (stamp)
		// and the refusal (refuse) off — gate 1 is a refusal carried out by
		// the node, and the node cannot be asked to compare and then not act
		// on the comparison, so that asymmetry is what made observe a dry run
		// rather than a differently-worded enforce. That rollout is over and
		// the mode is gone, so both gates now arm together, unconditionally,
		// every time this branch is reached.
		return fencingPlan{
			stamp:     true,
			compare:   true,
			refuse:    true,
			expect:    expect,
			authority: authority,
		}
	case schedulerv1.ExecutionAuthority_EXECUTION_AUTHORITY_PENDING:
		// PLACED and PINNED. The node is about to mint a new incarnation — the
		// data plane resumes a paused sandbox on demand — so any value here names
		// the previous one, and expecting it would refuse every auto-resume.
		return fencingPlan{decision: fencingDecisionPending, authority: authority}
	default:
		// UNKNOWN, UNSPECIFIED, and any value a newer scheduler grows. All three
		// mean the centre cannot name the incarnation, and all three pass.
		return fencingPlan{decision: fencingDecisionUnfencedNoAuthority, authority: authority}
	}
}

// normalizeExecutionID is routing.NormalizeExecutionID, and nothing else.
//
// 🔴 The rule belongs to the package both processes share, for the reason
// written on it: the comparison is lexicographic — a UUIDv7 sorts in the order
// it was minted — and '0'-'9' < 'A'-'F' < 'a'-'f', so one upper-case value
// compared against a lower-case one orders backwards. The scheduler's own id
// validator accepts either case, so the gateway cannot assume the normalisation
// already happened, and a second copy of the rule here would be free to stop
// agreeing with the one the scheduler orders by. What is left is a name this
// file already reads well with.
func normalizeExecutionID(raw string) string {
	return routing.NormalizeExecutionID(raw)
}

// stampGatewayHeader writes one of the gateway's own outbound headers over
// whatever the client sent under that name.
//
// 🔴 Unconditional. "Set it if it is not already there" would not be a weaker
// version of this — it would be a passthrough for a client-supplied credential
// wearing the gateway's header name. An empty value deletes rather than sets an
// empty one, so "we have nothing to stamp" and "we stamp nothing" are the same
// wire fact.
func stampGatewayHeader(h http.Header, name string, value string) {
	h.Del(name)
	if value != "" {
		h.Set(name, value)
	}
}

// stampOutboundGatewayHeaders is the single exit for everything the gateway
// asserts about itself on the way to a node. Both forwarding paths call it — the
// reverse proxy and the cluster-list fan-out — so neither can grow a header the
// other does not sanitise.
//
// 🔴 It runs whatever the fencing mode is, and that is one deliberate departure
// from "off behaves exactly as before". Off rolls back the gateway's own
// fencing; it does not roll back the nodes, which keep whatever gate the fleet
// was rolled out with. Forwarding a client's expect header while the gateway is
// not stamping one would hand any caller the ability to make a node refuse a
// request — the switch would turn a protection into a weapon. The deviation is
// one direction only: headers are removed, never added.
//
// A second mode, observe, used to lean on the same reasoning: it stamped
// nothing either, so the only expect header that could reach a node from an
// observing gateway was a forged one, and this is where it died. Observe is
// retired now; off is the only mode left that never stamps.
func (s *Server) stampOutboundGatewayHeaders(h http.Header, expectExecutionID string) {
	stampGatewayHeader(h, headerControlPlane, s.controlPlaneToken)
	stampGatewayHeader(h, headerExpectExecutionID, expectExecutionID)
	// Response-direction headers. A client has no business sending either, and
	// leaving them on the request would let one seed the node's view of an
	// exchange it is not part of.
	h.Del(headerExecutionID)
	h.Del(headerRefusal)
}

// fenceProxyResponse is the second gate: it reads the node's echo and decides
// what the client gets.
//
// It runs inside ModifyResponse, which is reached before an upgrade is handled,
// so a refusal also stops a WebSocket handshake rather than letting it complete
// against a superseded incarnation. Refusals are returned as an error rather
// than written in place: the reverse proxy closes the upstream body and hands
// the error to the error handler, which is what guarantees the node's body never
// reaches the client — and it is the only way to refuse a 101 without racing the
// upgrade path.
func (s *Server) fenceProxyResponse(plan fencingPlan, sandboxID string, node *schedulerv1.Node, resp *http.Response) error {
	if !plan.compare {
		// No authoritative incarnation to compare against, so no response header
		// is read at all. This is the branch off, the control plane and every
		// unfenced answer take, and it is what keeps off at exactly one
		// comparison, touching no header in either direction.
		//
		// 🔴 Gated on compare, not on stamp — a distinction that mattered while
		// observe existed: it stamped nothing and still measured everything, so
		// gating this on stamp instead would have produced no evidence, the
		// only thing that dry run was for. Observe is retired, but compare and
		// stamp are kept as separate fields (and this stays gated on compare)
		// for the same reason: a future mode that measures without delegating
		// should not have to re-derive this branch.
		return nil
	}

	observed := normalizeExecutionID(resp.Header.Get(headerExecutionID))

	// The node refused before running anything. Its 412 and its refusal header
	// are internal, so they are translated here and never forwarded.
	//
	// 🔴 Live in every mode that compares. While observe existed, this branch
	// was unreachable there by construction — observe stamped nothing and a
	// node with nothing to compare against cannot refuse — and it was kept
	// anyway because the two failure costs were not symmetric: an unreachable
	// branch costs one comparison, while a missing one hands a client the
	// internal 412 and the internal refusal header the first time anything
	// else in the fleet produces them — a mode flipped under a request
	// already in flight, a second gateway on enforce sharing the fleet, a
	// middlebox replaying a refusal. "It cannot happen" was a statement about
	// how the fleet was wired at the time, not an invariant this function was
	// in any position to enforce. Observe is retired, so this branch is no
	// longer merely unreachable-by-one-mode; it is exercised on every enforce
	// request that hits it, which is the mode both live paths run under today.
	//
	// Translating is also the only option available: the node has already
	// answered, so the choice is between an internal signal reaching the client
	// and the one external shape reaching it. It is not a choice about whether to
	// refuse.
	if resp.StatusCode == refusalNodeStatusCode &&
		resp.Header.Get(headerRefusal) == refusalCodeExecutionSuperseded {
		// Refused unconditionally: the return below is unconditional, so this
		// line says "refused" unconditionally too.
		s.logExecutionMismatch(sandboxID, node, plan.expect, observed, refusedByNode, true)
		recordExecutionFencing(fencingPlaneData, fencingDecisionRefusedPreflight)
		return executionSupersededRefusal(sandboxID, plan.expect, observed, refusedByNode)
	}

	switch {
	case observed == "":
		// No echo at all: this node does not implement fencing. Passing is the
		// only safe answer, and the count is the only way anyone finds out that
		// the first gate is not actually installed everywhere.
		recordExecutionFencing(fencingPlaneData, fencingDecisionUnfencedNodeSilent)
		return nil
	case observed == plan.expect:
		recordExecutionFencing(fencingPlaneData, fencingDecisionEnforcedPass)
		return nil
	case observed > plan.expect:
		// The node is ahead of the centre — an ordinary same-machine
		// pause→resume that the scheduler has not heard about yet. It is also
		// the node this request was routed to, so there is no second live copy
		// to protect anything from.
		recordExecutionFencing(fencingPlaneData, fencingDecisionUnfencedNodeAhead)
		return nil
	}

	// observed < expect: the node is running an incarnation the control plane
	// has moved past.
	// 🔴 plan.refuse, not a constant. While observe existed this was the record
	// of a request that was measured and then served, and calling that
	// "refused" would have made that round's log unreadable — the reason the
	// two messages in logExecutionMismatch are still split rather than
	// collapsed to one. Today plan.refuse is always true wherever plan.compare
	// is (only enforce reaches this line; off never reaches fenceProxyResponse's
	// compare branch at all), so this call always logs "refused" in practice —
	// but it reads the field rather than a literal so a future mode that
	// compares without refusing does not have to touch this line to get it
	// right.
	s.logExecutionMismatch(sandboxID, node, plan.expect, observed, refusedByGateway, plan.refuse)
	recordExecutionFencing(fencingPlaneData, fencingDecisionRefusedEcho)
	if !plan.refuse {
		// Unreachable today for the same reason as the comment above: nothing
		// this build can configure sets compare true without also setting
		// refuse true. Kept for the same "it cannot happen is a statement
		// about today's wiring, not an invariant this function enforces"
		// reasoning as the preflight branch above — if a mode ever compares
		// without refusing again, this is where it stays a dry run rather
		// than silently starting to refuse.
		return nil
	}
	return executionSupersededRefusal(sandboxID, plan.expect, observed, refusedByGateway)
}

// executionRefusalBody is the shape a fencing refusal has on the outside.
type executionRefusalBody struct {
	Code                string `json:"code"`
	Message             string `json:"message"`
	SandboxID           string `json:"sandboxID"`
	ExpectedExecutionID string `json:"expectedExecutionID"`
	ObservedExecutionID string `json:"observedExecutionID,omitempty"`
	RefusedBy           string `json:"refusedBy"`
}

// executionSupersededStatusCode is the one status a fencing refusal may carry.
//
// 🔴 Four codes are excluded and the reasons are not interchangeable:
//
//   - 404 is the hard one. The platform treats a 404 from resume as proof that
//     the sandbox is gone and that it may rebuild the workspace from scratch,
//     and the 404 it reads is produced here, by writeSchedulerError, without the
//     request ever reaching a node. A fencing refusal wearing a 404 therefore
//     ends with a user's workspace deleted and nothing logged as an error.
//   - 410 already means "not proxyable in its current state" on the node's own
//     /proxy. A second meaning on the same code cannot be told from the first.
//   - 503 is reserved, by the comment on writeSchedulerError, for "the scheduler
//     could not look" and "the only node that could serve this will not" — both
//     things that may have changed a moment later. A superseded incarnation is a
//     settled fact, not a temporary inability.
//   - 502 says the upstream is broken. It is not; we are declining to use it.
//
// 409 fits on every count: the state of the resource conflicts with the request,
// retrying is the correct response because a retry re-resolves onto the current
// incarnation, the platform already maps it to a conflict meaning "come back
// shortly", and the node's /proxy table does not use it — so a 409 from this
// path is unambiguous.
const executionSupersededStatusCode = http.StatusConflict

// executionSupersededRefusal builds the refusal.
//
// It is a constructor rather than a write so the status it produces can be
// asserted on its own: a test that only goes through the call sites would go
// green again the moment somebody adds a call site that writes something else.
func executionSupersededRefusal(sandboxID string, expected string, observed string, refusedBy string) *proxyResponseError {
	body, err := json.Marshal(executionRefusalBody{
		Code: refusalCodeExecutionSuperseded,
		Message: fmt.Sprintf(
			"this request reached an execution of sandbox %s that the control plane has superseded; retry to be routed to the current one",
			sandboxID),
		SandboxID:           sandboxID,
		ExpectedExecutionID: expected,
		ObservedExecutionID: observed,
		RefusedBy:           refusedBy,
	})
	if err != nil {
		// Nothing in the struct can fail to marshal, but a refusal that could
		// not be encoded still has to be a refusal — falling through to the
		// node's response would hand the client a superseded answer.
		body = []byte(`{"code":"` + refusalCodeExecutionSuperseded + `"}`)
	}

	header := http.Header{}
	header.Set(headerRefusal, refusalCodeExecutionSuperseded)
	return &proxyResponseError{
		statusCode:  executionSupersededStatusCode,
		message:     refusalCodeExecutionSuperseded,
		contentType: "application/json",
		body:        body,
		headers:     header,
	}
}

// The two messages one mismatch can be written down under.
//
// 🔴 Two, not one, and the field set is identical between them — the six fields
// are the frozen half of this contract (the three-stage trail in
// `_impl-plan-control-plane-phase3.md` §11.1(g), which the node and the registry
// write out under the same names), the message text is not. So splitting here is
// the change that costs nothing downstream, and the field set is what a test
// pins. One message for both outcomes is what the observe round used to leave
// an operator with: `grep refused` during that dry run returned a page of
// requests that were all answered 200, because observe counted and logged
// exactly what enforce refuses and then delivered the response anyway. A
// refusal log that is right half the time is worse than no log, because it is
// the one an incident is triaged with. Observe is retired, so in practice only
// logMsgExecutionRefused is written today — see logExecutionMismatch's own
// doc comment for why the split, and logMsgExecutionObserved, are kept anyway.
const (
	logMsgExecutionRefused  = "gateway refused a request against a superseded execution"
	logMsgExecutionObserved = "gateway observed a request against a superseded execution and let it through"
)

// logExecutionMismatch writes the one line an operator has for a mismatch.
//
// refused says which of the two things happened, and it is a parameter rather
// than a re-derivation from the mode because the two call sites still do not
// derive it identically: the echo gate refuses only when plan.refuse is set,
// while a refusal the node has already produced is translated and passed on
// unconditionally. Both currently agree in practice — off never reaches
// either call site, and enforce is the only mode left that does — but a third
// mode, observe, used to make the two call sites disagree for real (comparing
// without refusing, so the echo gate's refused was sometimes false while the
// preflight gate's was always true), which is why the parameter is not simply
// `mode == fencingEnforce`. Observe is retired; the parameter stays a
// parameter so a future mode that reintroduces that disagreement does not
// have to touch this function at all.
func (s *Server) logExecutionMismatch(sandboxID string, node *schedulerv1.Node, expected string, observed string, refusedBy string, refused bool) {
	message := logMsgExecutionObserved
	if refused {
		message = logMsgExecutionRefused
	}
	s.logger.Warn(message,
		zap.String("sandbox_id", sandboxID),
		zap.String("node_id", node.GetNodeId()),
		zap.String("expected_execution_id", expected),
		zap.String("observed_execution_id", observed),
		zap.String("refusal_code", refusalCodeExecutionSuperseded),
		zap.String("fencing_stage", fencingStageGatewayRoute),
		zap.String("refused_by", refusedBy),
	)
}

// executionIDFromResponse reads a node's echo off a response it just produced,
// for the assignment record. Read-only, and absent is an ordinary answer.
func executionIDFromResponse(h http.Header) string {
	return normalizeExecutionID(h.Get(headerExecutionID))
}
