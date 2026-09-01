// Package resume asks the API half to wake a paused sandbox.
//
// # Where this sits
//
// The gateway resolves a sandbox to a node out of its routing projection. When
// that misses, the sandbox is paused, gone, or somewhere the projection has not
// caught up with — and the gateway cannot tell which from anything it holds.
// Before this package, the only thing it could do was ask the scheduler for a
// node and forward the request there, where the node woke the sandbox itself.
// That is the arrangement `aenv-node` exists to end (`_sd-impl-phase3-role.md`
// §6.1): a node that decides when a sandbox should be alive is not an executor.
//
// So the question goes to the half that owns sandboxes, over one RPC, and this
// package is the caller.
//
// # 🔴 Three states, not two
//
// The single most important property here, and the one that has already cost
// this programme real data. A wake-up call has three outcomes and they are not
// interchangeable:
//
//   - [VerdictWoken] — the sandbox is running, at the named node.
//   - [VerdictGone] — the API half positively says there is no such sandbox.
//   - [VerdictUndecided] — nobody could be asked.
//
// The third is not a degenerate form of the second. "I could not reach the API
// half" says nothing whatever about whether the sandbox exists, and the
// downstream contract for "it does not exist" is that the platform rebuilds it
// from its template — which resets the user's workspace. Conflating those two
// is what deleted a paused sandbox's registry row in 阶段 2c while its bytes sat
// intact in object storage and both nodes reported it gone.
//
// [VerdictUndecided] is deliberately the zero value: a `Result` nobody filled
// in must not read as "woken" or as "gone".
//
// # 🔴 A refusal is not a reason to try elsewhere
//
// [VerdictRefused] carries a reason lifted from a response *trailer* rather
// than parsed out of a message. Three of those reasons mean the only copy of
// the sandbox's bytes is on a machine that cannot serve it right now, and for
// such a sandbox there is no second copy to try: retrying elsewhere does not
// fail, it *succeeds*, by rebuilding from whatever older snapshot did reach
// shared storage. So no verdict from this package ever means "ask another
// node".
package resume

import (
	"context"
	"time"

	schedulerv1 "agentenv/services/api/proto"
	apiproxyv1 "agentenv/services/api/proto/apiproxy"
	"agentenv/services/shared/routing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	// The metadata keys the API half reads off the request. Spelled the same as
	// the HTTP headers they are lifted from, on purpose: it is the same fact
	// travelling one hop further. Kept in step with `crate::proto::apiproxy`.
	metadataTargetPort  = "x-agentenv-target-port"
	metadataAccessToken = "x-access-token"

	// The trailers the API half answers a refusal with.
	//
	// 🔴 Trailers and not status details. The Rust side's
	// `Status::with_details` writes raw bytes into `grpc-status-details-bin`,
	// and this end's `status.Details()` reads that trailer as a marshalled
	// `google.rpc.Status` — the two do not meet, so a detail would arrive here
	// as an undecodable blob.
	trailerReason = "x-agentenv-resume-refusal"
	trailerOrigin = "x-agentenv-resume-origin-node"
)

// Verdict is what came back, read as one of three states plus a refusal.
type Verdict int

const (
	// VerdictUndecided means nobody could be asked, and is never an answer
	// about whether the sandbox exists. It ends the request as a 502: the api
	// half is the only thing the gateway asks, so there is nothing to fall
	// through to.
	//
	// 🔴 The zero value, deliberately. A Result that was never filled in has to
	// read as "I do not know", because the two things it must not be mistaken
	// for both destroy something: "woken" routes a request at a node that is
	// not running the sandbox, and "gone" rebuilds a live workspace from
	// scratch.
	VerdictUndecided Verdict = iota
	// VerdictWoken means the sandbox is running at the named node.
	VerdictWoken
	// VerdictGone means the API half consulted everything it has and there is
	// no such sandbox.
	VerdictGone
	// VerdictRefused means the wake-up was declined for a named reason. Never
	// a reason to try another node.
	VerdictRefused
)

func (v Verdict) String() string {
	switch v {
	case VerdictWoken:
		return "woken"
	case VerdictGone:
		return "gone"
	case VerdictRefused:
		return "refused"
	default:
		return "undecided"
	}
}

// Request is what the gateway knows about the traffic that triggered the
// wake-up.
type Request struct {
	SandboxID string
	// TargetPort is the port the data-plane request was addressed to, as it
	// appeared on the wire. Empty when the request did not say.
	//
	// 🔴 Passed through as a string rather than parsed here. The API half
	// treats an unparseable port the same as an absent one — as *possibly*
	// envd traffic, which is the strict direction — and re-deciding that here
	// would put the credential check's edge case in two places that could
	// disagree.
	TargetPort string
	// EnvdAccessToken is the caller's token, empty when it presented none.
	EnvdAccessToken string
}

// Result is one wake-up attempt, read.
type Result struct {
	Verdict Verdict

	// Set when Verdict is VerdictWoken.
	NodeID      string
	NodeAddress string
	ExecutionID string

	// Set when Verdict is VerdictRefused: the reason from the trailer, the node
	// a pinned refusal named, and the status as it arrived.
	Reason       string
	OriginNodeID string
	Status       *status.Status
}

// LookupResponse renders a woken sandbox in the shape the rest of the routing
// path already speaks, so nothing downstream needs to know a wake-up happened.
//
// 🔴 BOUND, matching what routing.Synthesize answers for a projection hit. The
// sandbox is running on a named node under a named incarnation, which is what
// BOUND means; PLACED or PINNED would describe a decision about where it should
// go, and that decision has already been taken and acted on by the time this is
// called.
func (r Result) LookupResponse() *schedulerv1.LookupNodeResponse {
	return &schedulerv1.LookupNodeResponse{
		Node: &schedulerv1.Node{
			NodeId:   r.NodeID,
			Endpoint: r.NodeAddress,
		},
		Location:           schedulerv1.SandboxLocation_SANDBOX_LOCATION_BOUND,
		ExecutionId:        r.ExecutionID,
		ExecutionAuthority: routing.AuthorityFor(r.ExecutionID),
	}
}

// Client is the gateway's half of the wake-up RPC.
type Client struct {
	client  apiproxyv1.SandboxResumeServiceClient
	timeout time.Duration
}

// New builds a client over an established connection.
//
// A zero timeout means the caller's context is the only bound, which is what
// the gateway wants: its request context already carries the deadline the user
// is waiting against.
func New(conn grpc.ClientConnInterface, timeout time.Duration) *Client {
	return &Client{
		client:  apiproxyv1.NewSandboxResumeServiceClient(conn),
		timeout: timeout,
	}
}

// NewFromClient builds a Client over an existing service client. Used by tests
// and by any caller that already holds one.
func NewFromClient(client apiproxyv1.SandboxResumeServiceClient, timeout time.Duration) *Client {
	return &Client{client: client, timeout: timeout}
}

// Wake asks the API half to bring a paused sandbox back up.
//
// It never returns an error: every failure is one of the verdicts, because the
// whole point of this call is that the *kind* of failure decides what the
// gateway does next, and an `error` return invites a caller to treat them all
// alike.
func (c *Client) Wake(ctx context.Context, req Request) Result {
	if c == nil || c.client == nil {
		// No client configured. Undecided rather than Gone: a gateway that
		// cannot ask knows nothing about this sandbox.
		return Result{Verdict: VerdictUndecided}
	}

	if c.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}

	pairs := make([]string, 0, 4)
	if req.TargetPort != "" {
		pairs = append(pairs, metadataTargetPort, req.TargetPort)
	}
	if req.EnvdAccessToken != "" {
		pairs = append(pairs, metadataAccessToken, req.EnvdAccessToken)
	}
	if len(pairs) > 0 {
		ctx = metadata.AppendToOutgoingContext(ctx, pairs...)
	}

	var trailer metadata.MD
	resp, err := c.client.ResumeSandbox(
		ctx,
		&apiproxyv1.SandboxResumeRequest{SandboxId: req.SandboxID},
		grpc.Trailer(&trailer),
	)
	if err == nil {
		return Result{
			Verdict:     VerdictWoken,
			NodeID:      resp.GetNodeId(),
			NodeAddress: resp.GetNodeAddress(),
			ExecutionID: resp.GetExecutionId(),
		}
	}

	st, _ := status.FromError(err)
	result := Result{
		Status:       st,
		Reason:       firstTrailerValue(trailer, trailerReason),
		OriginNodeID: firstTrailerValue(trailer, trailerOrigin),
	}

	switch st.Code() {
	case codes.NotFound:
		// 🔴 The only code that means the sandbox is gone. The API half sends
		// it only after consulting the placement source, its own records and
		// the cluster row, and it is careful never to answer NotFound for
		// "could not be asked" — which is the whole reason this end can afford
		// to treat it as final.
		result.Verdict = VerdictGone
	case codes.Unavailable, codes.DeadlineExceeded, codes.Canceled:
		// 🔴 The third state. The API half was unreachable, too slow, or said
		// so itself. None of that is evidence about the sandbox, so the gateway
		// falls back to the path it used before this package existed rather
		// than answering the client anything at all.
		result.Verdict = VerdictUndecided
	case codes.PermissionDenied, codes.FailedPrecondition, codes.ResourceExhausted,
		codes.InvalidArgument, codes.Internal, codes.Unimplemented:
		result.Verdict = VerdictRefused
	default:
		// An unrecognised code is a refusal rather than an absence or a retry.
		// Refusing surfaces as a 502 the operator can see; guessing "gone"
		// would rebuild a workspace on the strength of a code this build does
		// not know.
		result.Verdict = VerdictRefused
	}
	return result
}

func firstTrailerValue(md metadata.MD, key string) string {
	values := md.Get(key)
	if len(values) == 0 {
		return ""
	}
	return values[0]
}
