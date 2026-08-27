package gateway

import (
	"context"
	"net/http"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"
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

// refusingScheduler fails the test if the scheduler is consulted at all.
func refusingScheduler(t *testing.T, why string) stubSchedulerClient {
	t.Helper()
	return stubSchedulerClient{
		lookupNodeFunc: func(context.Context, *schedulerv1.LookupNodeRequest, ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
			t.Fatalf("the scheduler must not be consulted: %s", why)
			return nil, nil
		},
	}
}

// 🔴 The pair that makes either half mean anything.
//
// Both assertions here are of the shape that passes trivially against a broken
// build: "the scheduler was not called" is true of a gateway that never routes
// anything, and "the scheduler was called" is true of a gateway that ignores
// the wake-up client entirely. Neither is evidence alone. Together — the same
// request, the same projection miss, the same everything except what the api
// half answered — they say the branch is live and that it branches.
func TestAWakeUpAnswerDecidesWhetherTheSchedulerIsAskedAtAll(t *testing.T) {
	t.Run("woken: the api half's node is used and the scheduler is not asked", func(t *testing.T) {
		upstream, hits := newUpstream(t)
		service := &stubResumeService{response: &apiproxyv1.SandboxResumeResponse{
			NodeId:      "node-a",
			NodeAddress: upstream.URL,
			ExecutionId: "0198b7cc-1111-7000-8000-000000000001",
		}}

		server := newTestServer(t,
			refusingScheduler(t, "the api half already said where the sandbox is"),
			5*time.Second, 1<<20,
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

	t.Run("undecided: the scheduler answers, exactly as before this branch existed", func(t *testing.T) {
		upstream, hits := newUpstream(t)
		service := &stubResumeService{err: status.Error(codes.Unavailable, "api half is restarting")}
		lookups := 0
		scheduler := stubSchedulerClient{
			lookupNodeFunc: func(_ context.Context, req *schedulerv1.LookupNodeRequest, _ ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
				lookups++
				return &schedulerv1.LookupNodeResponse{
					Node:     &schedulerv1.Node{NodeId: "node-a", Endpoint: upstream.URL},
					Location: schedulerv1.SandboxLocation_SANDBOX_LOCATION_BOUND,
				}, nil
			},
		}

		server := newTestServer(t, scheduler, 5*time.Second, 1<<20,
			withProjectionReader(missingProjection()),
			withResumeClient(service),
		)
		resp := serveDataPlaneRequest(t, server.Handler(), "sbx-1")
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("🔴 status = %d, want 200. An api half that cannot be reached "+
				"must cost latency, not availability: the fallback is the same "+
				"LookupNode this gateway called for every projection miss before "+
				"the wake-up client existed", resp.StatusCode)
		}
		if service.calls != 1 {
			t.Fatalf("wake-up calls = %d, want 1", service.calls)
		}
		if lookups != 1 || hits.get() != 1 {
			t.Fatalf("lookups = %d, upstream hits = %d, want 1 and 1", lookups, hits.get())
		}
	})
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
	t.Run("gone: 404, and the scheduler is not given a second opinion", func(t *testing.T) {
		service := &stubResumeService{err: status.Error(codes.NotFound, "sandbox sbx-1 not found")}

		server := newTestServer(t,
			refusingScheduler(t, "the api half already said the sandbox does not exist"),
			5*time.Second, 1<<20,
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
		upstream, _ := newUpstream(t)
		service := &stubResumeService{err: status.Error(codes.DeadlineExceeded, "too slow")}
		scheduler := stubSchedulerClient{
			lookupNodeFunc: func(context.Context, *schedulerv1.LookupNodeRequest, ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
				return &schedulerv1.LookupNodeResponse{
					Node:     &schedulerv1.Node{NodeId: "node-a", Endpoint: upstream.URL},
					Location: schedulerv1.SandboxLocation_SANDBOX_LOCATION_BOUND,
				}, nil
			},
		}

		server := newTestServer(t, scheduler, 5*time.Second, 1<<20,
			withProjectionReader(missingProjection()),
			withResumeClient(service),
		)
		resp := serveDataPlaneRequest(t, server.Handler(), "sbx-1")
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusNotFound {
			t.Fatal("🔴 a wake-up that timed out answered 404. That tells the " +
				"platform to rebuild a sandbox nobody established anything about")
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200 from the fallback", resp.StatusCode)
		}
	})
}

// 🔴 A pin refusal is a 503 and is never retried against another node.
//
// For a sandbox whose snapshot never reached shared storage there is no second
// copy: waking it elsewhere would not fail, it would *succeed*, by rebuilding
// from an older snapshot and losing the last pause. So the refusal ends the
// request rather than falling through to a scheduler that would happily name a
// different machine.
func TestAPinRefusalIs503AndNeverReachesTheScheduler(t *testing.T) {
	service := &stubResumeService{
		err: status.Error(codes.FailedPrecondition,
			`sandbox is local_only on node "node-b", which is not accepting work`),
		trailer: metadata.Pairs(
			"x-agentenv-resume-refusal", "origin_not_accepting_work",
			"x-agentenv-resume-origin-node", "node-b",
		),
	}

	server := newTestServer(t,
		refusingScheduler(t, "no other node has this sandbox's bytes"),
		5*time.Second, 1<<20,
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

	server := newTestServer(t,
		refusingScheduler(t, "somebody else is already waking this sandbox"),
		5*time.Second, 1<<20,
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
// fail. If the gateway fell through to the scheduler here the request would
// succeed — proving only that a second wake-up path was doing the work, which
// is exactly what the control is designed to detect.
func TestAnUnimplementedWakeUpFailsRatherThanFallingBack(t *testing.T) {
	service := &stubResumeService{err: status.Error(codes.Unimplemented, "not implemented")}

	server := newTestServer(t,
		refusingScheduler(t, "control C requires the data plane to fail when the surface is stubbed out"),
		5*time.Second, 1<<20,
		withProjectionReader(missingProjection()),
		withResumeClient(service),
	)
	resp := serveDataPlaneRequest(t, server.Handler(), "sbx-1")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", resp.StatusCode)
	}
}

// The switch off is today's behaviour exactly: no wake-up call, straight to the
// scheduler. This is 阶段 3a's rollback, and it is a configuration value rather
// than a deploy, which is what makes that rollback seconds rather than a
// DaemonSet roll.
func TestNoResumeClientNeverWakesAnything(t *testing.T) {
	upstream, hits := newUpstream(t)
	lookups := 0
	scheduler := stubSchedulerClient{
		lookupNodeFunc: func(context.Context, *schedulerv1.LookupNodeRequest, ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
			lookups++
			return &schedulerv1.LookupNodeResponse{
				Node:     &schedulerv1.Node{NodeId: "node-a", Endpoint: upstream.URL},
				Location: schedulerv1.SandboxLocation_SANDBOX_LOCATION_BOUND,
			}, nil
		},
	}

	server := newTestServer(t, scheduler, 5*time.Second, 1<<20,
		withProjectionReader(missingProjection()),
	)
	resp := serveDataPlaneRequest(t, server.Handler(), "sbx-1")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if lookups != 1 || hits.get() != 1 {
		t.Fatalf("lookups = %d, upstream hits = %d, want 1 and 1", lookups, hits.get())
	}
}

// The port the data-plane request was addressed to reaches the api half, which
// is what decides whether the envd credential check runs there at all.
func TestTheTargetPortReachesTheApiHalf(t *testing.T) {
	upstream, _ := newUpstream(t)
	service := &stubResumeService{response: &apiproxyv1.SandboxResumeResponse{
		NodeId:      "node-a",
		NodeAddress: upstream.URL,
		ExecutionId: "0198b7cc-1111-7000-8000-000000000001",
	}}

	server := newTestServer(t,
		refusingScheduler(t, "the api half answered"),
		5*time.Second, 1<<20,
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

	server := newTestServer(t,
		refusingScheduler(t, "a sandbox that declines to wake must not be routed anywhere"),
		5*time.Second, 1<<20,
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
