package resume

import (
	"context"
	"net"
	"testing"
	"time"

	apiproxyv1 "agentenv/services/api/proto/apiproxy"
	"agentenv/services/shared/routing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// ---------------------------------------------------------------------------
// An api half on a real socket
// ---------------------------------------------------------------------------

// fakeAPI answers whatever the test needs and records what it was asked.
type fakeAPI struct {
	apiproxyv1.UnimplementedSandboxResumeServiceServer

	response *apiproxyv1.SandboxResumeResponse
	err      error
	// Trailers to send alongside err, as the real api half does for a refusal.
	trailer metadata.MD

	// What arrived.
	gotSandboxID   string
	gotTargetPort  []string
	gotAccessToken []string
	calls          int
}

func (f *fakeAPI) ResumeSandbox(ctx context.Context, req *apiproxyv1.SandboxResumeRequest) (*apiproxyv1.SandboxResumeResponse, error) {
	f.calls++
	f.gotSandboxID = req.GetSandboxId()
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		f.gotTargetPort = md.Get(metadataTargetPort)
		f.gotAccessToken = md.Get(metadataAccessToken)
	}
	if len(f.trailer) > 0 {
		_ = grpc.SetTrailer(ctx, f.trailer)
	}
	if f.err != nil {
		return nil, f.err
	}
	return f.response, nil
}

// serve starts fake on a real socket and returns a Client pointed at it.
//
// A real socket rather than a direct call, because half of what this client
// does lives in the transport: the refusal reason arrives as a trailer and the
// port and token leave as metadata, and neither is exercised by calling a
// method.
func serve(t *testing.T, fake *fakeAPI) *Client {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := grpc.NewServer()
	apiproxyv1.RegisterSandboxResumeServiceServer(server, fake)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	conn, err := grpc.NewClient(
		listener.Addr().String(),
		grpc.WithTransportCredentials(insecureCreds()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return New(conn, 5*time.Second)
}

// ---------------------------------------------------------------------------
// 🔴 The three states
// ---------------------------------------------------------------------------

// 🔴 The test this package exists for.
//
// "The api half says there is no such sandbox" and "the api half could not be
// asked" are the two answers that must never merge, and they are asserted here
// next to a success so that neither is proved by a client that simply fails at
// everything. The cost of merging them is not theoretical: the same conflation
// deleted a paused sandbox's registry row in 阶段 2c while its bytes sat intact
// in object storage.
func TestWakeReadsThreeDistinctStates(t *testing.T) {
	t.Run("a named node is a wake-up", func(t *testing.T) {
		client := serve(t, &fakeAPI{response: &apiproxyv1.SandboxResumeResponse{
			NodeId:      "node-a",
			NodeAddress: "http://node-a:8000",
			ExecutionId: "01a02fe3-1f29-7420-91ec-d6c001c3910d",
		}})

		got := client.Wake(context.Background(), Request{SandboxID: "sb-1"})
		if got.Verdict != VerdictWoken {
			t.Fatalf("verdict = %v, want woken", got.Verdict)
		}
		if got.NodeID != "node-a" || got.NodeAddress != "http://node-a:8000" {
			t.Fatalf("node = %q at %q", got.NodeID, got.NodeAddress)
		}
		if got.ExecutionID == "" {
			t.Fatal("🔴 no incarnation: the request that caused this wake-up is " +
				"about to be forwarded, and without one it travels unfenced")
		}
	})

	t.Run("NotFound is the sandbox being gone", func(t *testing.T) {
		client := serve(t, &fakeAPI{err: status.Error(codes.NotFound, "sandbox sb-1 not found")})

		if got := client.Wake(context.Background(), Request{SandboxID: "sb-1"}); got.Verdict != VerdictGone {
			t.Fatalf("verdict = %v, want gone", got.Verdict)
		}
	})

	t.Run("Unavailable is not an answer about the sandbox", func(t *testing.T) {
		client := serve(t, &fakeAPI{err: status.Error(codes.Unavailable, "api half is restarting")})

		got := client.Wake(context.Background(), Request{SandboxID: "sb-1"})
		if got.Verdict != VerdictUndecided {
			t.Fatalf("🔴 verdict = %v, want undecided. Answering 'gone' here tells "+
				"the platform to rebuild a sandbox that may be perfectly alive, "+
				"which resets the user's workspace", got.Verdict)
		}
	})

	t.Run("a dead api half is undecided, not gone", func(t *testing.T) {
		// Nothing listening: the closest thing to the real failure, and the one
		// most likely to be mistaken for an answer.
		conn, err := grpc.NewClient("127.0.0.1:1", grpc.WithTransportCredentials(insecureCreds()))
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		t.Cleanup(func() { _ = conn.Close() })

		got := New(conn, 500*time.Millisecond).Wake(context.Background(), Request{SandboxID: "sb-1"})
		if got.Verdict != VerdictUndecided {
			t.Fatalf("verdict = %v, want undecided", got.Verdict)
		}
	})
}

// 🔴 The zero value has to be the safe one.
//
// Every other verdict is something the api half told us. Undecided is what we
// have before it has told us anything, and a Result that was default-
// constructed — by a future refactor, by a map lookup that missed, by a
// struct literal that forgot a field — must read as "I do not know" rather
// than as either of the two answers that destroy something.
func TestUndecidedIsTheZeroValue(t *testing.T) {
	var empty Result
	if empty.Verdict != VerdictUndecided {
		t.Fatalf("zero Result verdict = %v, want undecided", empty.Verdict)
	}
	if VerdictWoken == VerdictUndecided || VerdictGone == VerdictUndecided {
		t.Fatal("woken and gone must not share the zero value")
	}
}

// A client with nothing behind it is undecided, never gone.
//
// This is the switch-off path: a gateway with no wake-up endpoint configured
// knows nothing about the sandbox and must fall back to what it did before.
func TestNilClientIsUndecided(t *testing.T) {
	var client *Client
	if got := client.Wake(context.Background(), Request{SandboxID: "sb-1"}); got.Verdict != VerdictUndecided {
		t.Fatalf("verdict = %v, want undecided", got.Verdict)
	}
	if got := (&Client{}).Wake(context.Background(), Request{SandboxID: "sb-1"}); got.Verdict != VerdictUndecided {
		t.Fatalf("verdict = %v, want undecided", got.Verdict)
	}
}

// ---------------------------------------------------------------------------
// 🔴 Refusals, and why Unimplemented is not one of the retryable ones
// ---------------------------------------------------------------------------

// 🔴 §12 P3's control C in unit form.
//
// The probe stubs this RPC out with Unimplemented and requires the data plane
// to *fail*; if it succeeded, that would prove a second wake-up path was
// quietly doing the work. So Unimplemented must be a refusal, which ends the
// request — and specifically must not be Undecided, which falls through to the
// scheduler and lets the old path answer.
//
// Asserted next to Unavailable, which *is* Undecided, because that pair is the
// whole distinction: both mean "this surface is not working", and only one of
// them may be papered over.
func TestUnimplementedRefusesWhileUnavailableFallsBack(t *testing.T) {
	unimplemented := serve(t, &fakeAPI{err: status.Error(codes.Unimplemented, "not implemented")})
	if got := unimplemented.Wake(context.Background(), Request{SandboxID: "sb-1"}); got.Verdict != VerdictRefused {
		t.Fatalf("🔴 Unimplemented verdict = %v, want refused. Falling through to "+
			"the scheduler here would make P3's control C pass while a second "+
			"wake-up path did the work — the exact thing it exists to detect", got.Verdict)
	}

	unavailable := serve(t, &fakeAPI{err: status.Error(codes.Unavailable, "restarting")})
	if got := unavailable.Wake(context.Background(), Request{SandboxID: "sb-1"}); got.Verdict != VerdictUndecided {
		t.Fatalf("Unavailable verdict = %v, want undecided", got.Verdict)
	}
}

// 🔴 The refusal reason arrives in a trailer, and the client reads it.
//
// The reason is what decides how long the caller waits, and the three
// FAILED_PRECONDITION reasons mean completely different waits. A client that
// could not read them would back off wrongly in one direction or the other for
// every paused sandbox in the cluster.
func TestRefusalReasonAndOriginTravelInTrailers(t *testing.T) {
	client := serve(t, &fakeAPI{
		err: status.Error(codes.FailedPrecondition, `sandbox is local_only on node "node-b", which is not accepting work`),
		trailer: metadata.Pairs(
			trailerReason, "origin_not_accepting_work",
			trailerOrigin, "node-b",
		),
	})

	got := client.Wake(context.Background(), Request{SandboxID: "sb-1"})
	if got.Verdict != VerdictRefused {
		t.Fatalf("verdict = %v, want refused", got.Verdict)
	}
	if got.Reason != "origin_not_accepting_work" {
		t.Fatalf("reason = %q, want origin_not_accepting_work", got.Reason)
	}
	if got.OriginNodeID != "node-b" {
		t.Fatalf("origin = %q, want node-b", got.OriginNodeID)
	}

	// The non-empty half: a refusal that carries no trailers is still a
	// refusal. If this came back Undecided, the assertions above would be
	// proving that trailers arrive rather than that they are required.
	bare := serve(t, &fakeAPI{err: status.Error(codes.FailedPrecondition, "no trailers here")})
	bareGot := bare.Wake(context.Background(), Request{SandboxID: "sb-1"})
	if bareGot.Verdict != VerdictRefused {
		t.Fatalf("trailerless verdict = %v, want refused", bareGot.Verdict)
	}
	if bareGot.Reason != "" {
		t.Fatalf("reason = %q, want empty", bareGot.Reason)
	}
}

// ---------------------------------------------------------------------------
// What leaves this process
// ---------------------------------------------------------------------------

// 🔴 The port and the token travel as metadata, not as proto fields.
//
// A credential in a message body ends up in every request log that prints the
// request. And the port is what decides whether the api half runs the envd
// credential check at all, so a client that dropped it would turn every
// wake-up into one the check treats as possibly-envd — which is the safe
// direction, but silently changes the contract.
func TestRequestMetadataIsForwarded(t *testing.T) {
	withBoth := &fakeAPI{response: &apiproxyv1.SandboxResumeResponse{NodeId: "node-a"}}
	client := serve(t, withBoth)

	client.Wake(context.Background(), Request{
		SandboxID:       "sb-1",
		TargetPort:      "49983",
		EnvdAccessToken: "the-token",
	})

	if withBoth.gotSandboxID != "sb-1" {
		t.Fatalf("sandbox id = %q", withBoth.gotSandboxID)
	}
	if len(withBoth.gotTargetPort) != 1 || withBoth.gotTargetPort[0] != "49983" {
		t.Fatalf("target port metadata = %v", withBoth.gotTargetPort)
	}
	if len(withBoth.gotAccessToken) != 1 || withBoth.gotAccessToken[0] != "the-token" {
		t.Fatalf("access token metadata = %v", withBoth.gotAccessToken)
	}

	// The other half: absent stays absent rather than becoming an empty string.
	//
	// 🔴 This is not cosmetic. The api half reads an *absent* port as "possibly
	// envd" and applies the credential check; sending "" would arrive as a
	// present-but-unparseable value, which it also reads as possibly-envd — but
	// only because that end is careful. Sending nothing keeps the two ends from
	// depending on that care.
	withNeither := &fakeAPI{response: &apiproxyv1.SandboxResumeResponse{NodeId: "node-a"}}
	bare := serve(t, withNeither)
	bare.Wake(context.Background(), Request{SandboxID: "sb-2"})

	if len(withNeither.gotTargetPort) != 0 {
		t.Fatalf("target port metadata = %v, want absent", withNeither.gotTargetPort)
	}
	if len(withNeither.gotAccessToken) != 0 {
		t.Fatalf("access token metadata = %v, want absent", withNeither.gotAccessToken)
	}
}

// 🔴 An unparseable port is forwarded verbatim rather than dropped.
//
// The api half maps both "absent" and "unparseable" to "possibly envd" and
// applies the credential check. A gateway that sanitised the value would be
// re-deciding that here, in a second place, and a gateway that dropped it would
// hand a caller exactly the skip the check exists to prevent.
func TestAnUnparseableTargetPortIsForwardedUntouched(t *testing.T) {
	fake := &fakeAPI{response: &apiproxyv1.SandboxResumeResponse{NodeId: "node-a"}}
	client := serve(t, fake)

	client.Wake(context.Background(), Request{SandboxID: "sb-1", TargetPort: "banana"})

	if len(fake.gotTargetPort) != 1 || fake.gotTargetPort[0] != "banana" {
		t.Fatalf("target port metadata = %v, want [banana] forwarded as-is", fake.gotTargetPort)
	}
}

// ---------------------------------------------------------------------------
// The shape handed back to the routing path
// ---------------------------------------------------------------------------

// A woken sandbox is Bound: it is running on a named node under a named
// incarnation. Placed or Pinned would describe a decision about where it should
// go, and by this point that decision has been taken and acted on.
func TestAnswerIsBoundWithTheIncarnation(t *testing.T) {
	result := Result{
		Verdict:     VerdictWoken,
		NodeID:      "node-a",
		NodeAddress: "http://node-a:8000",
		ExecutionID: "01a02fe3-1f29-7420-91ec-d6c001c3910d",
	}

	answer := result.Answer()
	if answer.Node.ID != "node-a" {
		t.Fatalf("node id = %q", answer.Node.ID)
	}
	if answer.Node.Endpoint != "http://node-a:8000" {
		t.Fatalf("endpoint = %q", answer.Node.Endpoint)
	}
	if answer.Location != routing.LocationBound {
		t.Fatalf("location = %s, want bound", answer.Location)
	}
	if answer.ExecutionID != result.ExecutionID {
		t.Fatalf("execution id = %q", answer.ExecutionID)
	}
	// The authority must not claim more than the incarnation supports; an empty
	// one has to degrade rather than be asserted.
	empty := Result{Verdict: VerdictWoken, NodeID: "node-a"}.Answer()
	if empty.Authority == answer.Authority {
		t.Fatal("an answer naming no incarnation must not carry the same " +
			"authority as one that names a real incarnation")
	}
}

// insecureCreds keeps the import local to the test file.
func insecureCreds() credentials.TransportCredentials {
	return insecure.NewCredentials()
}
