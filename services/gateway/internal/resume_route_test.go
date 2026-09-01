package gateway

import (
	"context"
	"net/http"
	"testing"
	"time"

	apiproxyv1 "agentenv/services/api/proto/apiproxy"
	"agentenv/services/gateway/internal/resume"
	"agentenv/services/shared/routing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// stubResumeService answers the wake-up RPC without a socket. The transport
// itself is covered in the resume package's own tests, over a real one.
type stubResumeService struct {
	response *apiproxyv1.SandboxResumeResponse
	err      error
	trailer  metadata.MD

	calls         int
	gotSandboxID  string
	gotTargetPort []string
}

func (s *stubResumeService) ResumeSandbox(
	ctx context.Context,
	req *apiproxyv1.SandboxResumeRequest,
	opts ...grpc.CallOption,
) (*apiproxyv1.SandboxResumeResponse, error) {
	s.calls++
	s.gotSandboxID = req.GetSandboxId()
	if md, ok := metadata.FromOutgoingContext(ctx); ok {
		s.gotTargetPort = md.Get("x-agentenv-target-port")
	}
	// Honour grpc.Trailer so the refusal reason reaches the caller the same way
	// a real server would send it.
	for _, opt := range opts {
		if trailer, ok := opt.(grpc.TrailerCallOption); ok && trailer.TrailerAddr != nil {
			*trailer.TrailerAddr = s.trailer
		}
	}
	if s.err != nil {
		return nil, s.err
	}
	return s.response, nil
}

func withResumeClient(service apiproxyv1.SandboxResumeServiceClient) testServerOption {
	return func(options *ServerOptions) {
		options.ResumeClient = resume.NewFromClient(service, time.Second)
	}
}

func missingProjection() *stubProjectionReader {
	return &stubProjectionReader{records: map[string]routing.Record{}}
}

// runningAt answers the resume RPC with a sandbox running on one node under a
// fixed incarnation, whether the api half found it running or woke it.
func runningAt(nodeID string, endpoint string) *stubResumeService {
	return &stubResumeService{response: &apiproxyv1.SandboxResumeResponse{
		NodeId:      nodeID,
		NodeAddress: endpoint,
		ExecutionId: "0198b7cc-1111-7000-8000-000000000001",
	}}
}

// refusingResumeService fails the test if the api half is asked at all.
type refusingResumeService struct {
	t   *testing.T
	why string
}

func (s refusingResumeService) ResumeSandbox(
	context.Context,
	*apiproxyv1.SandboxResumeRequest,
	...grpc.CallOption,
) (*apiproxyv1.SandboxResumeResponse, error) {
	s.t.Fatalf("the api half must not be asked: %s", s.why)
	return nil, nil
}

func refusingResume(t *testing.T, why string) apiproxyv1.SandboxResumeServiceClient {
	t.Helper()
	return refusingResumeService{t: t, why: why}
}

// 🔴 The pair that makes either half mean anything.
//
// Both assertions here are of the shape that passes trivially against a broken
// build: "the request was proxied" is true of a gateway that routes everything
// somewhere, and "the request failed" is true of a gateway that routes nothing.
// Neither is evidence alone. Together — the same request, the same projection
// miss, the same everything except what the api half answered — they say the
// branch is live and that it branches.
func TestAWakeUpAnswerDecidesWhetherTheRequestIsRoutedAtAll(t *testing.T) {
	t.Run("woken: the api half's node is used", func(t *testing.T) {
		upstream, hits := newUpstream(t)
		service := runningAt("node-a", upstream.URL)

		server := newTestServer(t, 5*time.Second, 1<<20,
			withProjectionReader(missingProjection()),
			withResumeClient(service),
		)
		resp := serveDataPlaneRequest(t, server.Handler(), "sbx-1")
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200", resp.StatusCode)
		}
		if service.calls != 1 {
			t.Fatalf("wake-up calls = %d, want 1", service.calls)
		}
		if service.gotSandboxID != "sbx-1" {
			t.Fatalf("woke %q", service.gotSandboxID)
		}
		if hits.get() != 1 {
			t.Fatalf("upstream hits = %d, want 1: the request must reach the node "+
				"the api half named", hits.get())
		}
	})

	t.Run("undecided: the request fails, and nothing else is asked", func(t *testing.T) {
		_, hits := newUpstream(t)
		service := &stubResumeService{err: status.Error(codes.Unavailable, "api half is restarting")}

		server := newTestServer(t, 5*time.Second, 1<<20,
			withProjectionReader(missingProjection()),
			withResumeClient(service),
		)
		resp := serveDataPlaneRequest(t, server.Handler(), "sbx-1")
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusBadGateway {
			t.Fatalf("🔴 status = %d, want 502. The api half is the only thing the "+
				"gateway asks; when it cannot be asked the request fails, and it "+
				"fails as a transport problem rather than as anything about the "+
				"sandbox", resp.StatusCode)
		}
		if service.calls != 1 {
			t.Fatalf("wake-up calls = %d, want 1", service.calls)
		}
		if hits.get() != 0 {
			t.Fatalf("upstream hits = %d, want 0: nothing named a node", hits.get())
		}
	})
}

// 🔴 An api half that cannot be asked is a 502 and never a 404.
//
// Every way the resume client can come back undecided — the api half is
// unavailable, too slow, the call was cancelled, or there is no client at
// all — lands on the same status. 502 says the thing behind the gateway is
// not answering; 404 would tell the platform to rebuild a sandbox nobody
// established anything about, and 503 would advertise a retry against a
// gateway that has nowhere else to look.
func TestAnUnreachableApiHalfIsFiveOhTwo(t *testing.T) {
	for name, service := range map[string]apiproxyv1.SandboxResumeServiceClient{
		"unavailable":       &stubResumeService{err: status.Error(codes.Unavailable, "api half is restarting")},
		"deadline exceeded": &stubResumeService{err: status.Error(codes.DeadlineExceeded, "too slow")},
		"canceled":          &stubResumeService{err: status.Error(codes.Canceled, "gone away")},
	} {
		t.Run(name, func(t *testing.T) {
			_, hits := newUpstream(t)
			server := newTestServer(t, 5*time.Second, 1<<20,
				withProjectionReader(missingProjection()),
				withResumeClient(service),
			)
			resp := serveDataPlaneRequest(t, server.Handler(), "sbx-1")
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusBadGateway {
				t.Fatalf("status = %d, want 502", resp.StatusCode)
			}
			if hits.get() != 0 {
				t.Fatalf("upstream hits = %d, want 0", hits.get())
			}
		})
	}
}

// 🔴 "Gone" ends the request; "could not ask" does not.
//
// The two are asserted together because the failure this guards against is
// precisely that they become the same code path. A 404 on a resume is the end
// of that sandbox as far as any client is concerned — the platform's contract
// for it is "rebuild from the template", which resets the user's workspace —
// and the same conflation already deleted a paused sandbox's registry row in
// 阶段 2c with its bytes intact in object storage.
func TestOnlyAPositiveGoneAnswers404(t *testing.T) {
	t.Run("gone: 404", func(t *testing.T) {
		service := &stubResumeService{err: status.Error(codes.NotFound, "sandbox sbx-1 not found")}

		server := newTestServer(t, 5*time.Second, 1<<20,
			withProjectionReader(missingProjection()),
			withResumeClient(service),
		)
		resp := serveDataPlaneRequest(t, server.Handler(), "sbx-1")
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("status = %d, want 404", resp.StatusCode)
		}
	})

	t.Run("unreachable: never 404", func(t *testing.T) {
		service := &stubResumeService{err: status.Error(codes.DeadlineExceeded, "too slow")}

		server := newTestServer(t, 5*time.Second, 1<<20,
			withProjectionReader(missingProjection()),
			withResumeClient(service),
		)
		resp := serveDataPlaneRequest(t, server.Handler(), "sbx-1")
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusNotFound {
			t.Fatal("🔴 a wake-up that timed out answered 404. That tells the " +
				"platform to rebuild a sandbox nobody established anything about")
		}
		if resp.StatusCode != http.StatusBadGateway {
			t.Fatalf("status = %d, want 502", resp.StatusCode)
		}
	})
}

// 🔴 A pin refusal is a 503 and is never retried against another node.
//
// For a sandbox whose snapshot never reached shared storage there is no second
// copy: waking it elsewhere would not fail, it would *succeed*, by rebuilding
// from an older snapshot and losing the last pause. So the refusal ends the
// request.
func TestAPinRefusalIs503AndNeverRoutedAnywhere(t *testing.T) {
	service := &stubResumeService{
		err: status.Error(codes.FailedPrecondition,
			`sandbox is local_only on node "node-b", which is not accepting work`),
		trailer: metadata.Pairs(
			"x-agentenv-resume-refusal", "origin_not_accepting_work",
			"x-agentenv-resume-origin-node", "node-b",
		),
	}

	server := newTestServer(t, 5*time.Second, 1<<20,
		withProjectionReader(missingProjection()),
		withResumeClient(service),
	)
	resp := serveDataPlaneRequest(t, server.Handler(), "sbx-1")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", resp.StatusCode)
	}
	// 🔴 No Retry-After on a pin refusal. It clears when a machine comes back,
	// which may be never, and a one-second hint would have every client in the
	// cluster hammering a node that is not coming.
	if retry := resp.Header.Get("Retry-After"); retry != "" {
		t.Fatalf("Retry-After = %q on a pin refusal, want none", retry)
	}
}

// The transient refusal is the one that carries Retry-After — asserted against
// the pin refusal above, which must not.
func TestATransitionInProgressCarriesRetryAfter(t *testing.T) {
	service := &stubResumeService{
		err: status.Error(codes.FailedPrecondition, "sandbox is being resumed by node 'node-a'"),
		trailer: metadata.Pairs(
			"x-agentenv-resume-refusal", "transition_in_progress",
			"x-agentenv-resume-origin-node", "node-a",
		),
	}

	server := newTestServer(t, 5*time.Second, 1<<20,
		withProjectionReader(missingProjection()),
		withResumeClient(service),
	)
	resp := serveDataPlaneRequest(t, server.Handler(), "sbx-1")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", resp.StatusCode)
	}
	if resp.Header.Get("Retry-After") == "" {
		t.Fatal("🔴 a transition in progress clears in a moment and must say so; " +
			"without it a client backs off as though a machine were missing")
	}
}

// 🔴 §12 P3's control C, at the gateway.
//
// The probe stubs the RPC out with Unimplemented and requires the data plane to
// fail: a request that succeeded would prove a second wake-up path was doing
// the work, which is exactly what the control is designed to detect.
func TestAnUnimplementedWakeUpFails(t *testing.T) {
	service := &stubResumeService{err: status.Error(codes.Unimplemented, "not implemented")}

	server := newTestServer(t, 5*time.Second, 1<<20,
		withProjectionReader(missingProjection()),
		withResumeClient(service),
	)
	resp := serveDataPlaneRequest(t, server.Handler(), "sbx-1")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", resp.StatusCode)
	}
}

// A gateway with no resume client has nobody to ask on a miss: the request
// fails rather than being routed anywhere.
func TestNoResumeClientFailsTheMissRatherThanRoutingAnywhere(t *testing.T) {
	server := newTestServer(t, 5*time.Second, 1<<20,
		withProjectionReader(missingProjection()),
	)
	resp := serveDataPlaneRequest(t, server.Handler(), "sbx-1")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", resp.StatusCode)
	}
}

// The port the data-plane request was addressed to reaches the api half, which
// is what decides whether the envd credential check runs there at all.
func TestTheTargetPortReachesTheApiHalf(t *testing.T) {
	upstream, _ := newUpstream(t)
	service := runningAt("node-a", upstream.URL)

	server := newTestServer(t, 5*time.Second, 1<<20,
		withProjectionReader(missingProjection()),
		withResumeClient(service),
	)
	resp := serveDataPlaneRequest(t, server.Handler(), "sbx-1")
	defer resp.Body.Close()

	if len(service.gotTargetPort) != 1 {
		t.Fatalf("target port metadata = %v, want exactly one value", service.gotTargetPort)
	}
}

// 🔴 `autoResume: {enabled: false}` is a 410, not the 503 its code would earn.
//
// It arrives as a FailedPrecondition like the two refusals above, and the three
// are told apart only by the trailer — so this is asserted beside them rather
// than alone: a build that mapped the whole FailedPrecondition arm to 410 would
// pass this test and break both of those.
//
// The distinction is not cosmetic. A 503 says "try again", and every client
// that believes it will retry forever against a sandbox whose owner asked that
// traffic never wake it. 410 is also what a node answers for a paused sandbox
// it will not wake (`src/api/proxy.rs`'s SandboxUnavailable), so the flag reads
// the same whichever half fields the request.
func TestAutoResumeDisabledIs410AndNotRetryable(t *testing.T) {
	service := &stubResumeService{
		err: status.Error(codes.FailedPrecondition,
			"this sandbox was created with auto-resume off and does not wake on data-plane traffic"),
		trailer: metadata.Pairs(
			"x-agentenv-resume-refusal", "auto_resume_disabled",
			"x-agentenv-resume-origin-node", "",
		),
	}

	server := newTestServer(t, 5*time.Second, 1<<20,
		withProjectionReader(missingProjection()),
		withResumeClient(service),
	)
	resp := serveDataPlaneRequest(t, server.Handler(), "sbx-1")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusGone {
		t.Fatalf("status = %d, want 410", resp.StatusCode)
	}
	// 🔴 And no Retry-After. The flag does not clear on its own; the sandbox
	// comes back when somebody calls resume, not when a client waits.
	if retry := resp.Header.Get("Retry-After"); retry != "" {
		t.Fatalf("Retry-After = %q on auto-resume-disabled, want none", retry)
	}
}
