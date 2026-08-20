package scheduler

import (
	"testing"

	schedulerv1 "agentenv/services/api/proto"
	"agentenv/services/shared/routing"

	"google.golang.org/protobuf/proto"
)

// TestSynthesizedAnswerMatchesSchedulerAnswer is the one test holding the two
// halves of the read path together.
//
// The gateway may answer a sandbox route straight out of the routing
// projection, without asking the scheduler at all. For that to be safe, the
// response it builds from a flat record has to be the response the scheduler
// would have built from the same record — not "close enough", but field for
// field, because everything downstream of the lookup (the fencing decision, the
// location log, the assignment write) reads those fields individually.
//
// 🔴 Two implementations of one response shape drift. This is the test that
// notices: any field added to the binding-hit exit in lookup.go and not to
// routing.Synthesize fails here, on the commit that adds it, rather than in a
// cluster where a gateway silently stops fencing.
func TestSynthesizedAnswerMatchesSchedulerAnswer(t *testing.T) {
	cases := []struct {
		name   string
		record routing.Record
	}{
		{
			name: "named incarnation",
			record: routing.Record{
				Node:        Node{ID: "node-a", Endpoint: "http://node-a"},
				ExecutionID: "0198b7cc-1111-7000-8000-000000000001",
			},
		},
		{
			name: "no incarnation",
			record: routing.Record{
				Node: Node{ID: "node-b", Endpoint: "http://node-b"},
			},
		},
		{
			name: "pod-aliased node",
			record: routing.Record{
				Node:        Node{ID: "node-c", Endpoint: "http://node-c", PodName: "agentenv-node-abcde"},
				ExecutionID: "0198b7cc-2222-7000-8000-000000000002",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Exactly the expression at lookup.go's binding hit — the single
			// exit routing.Synthesize stands in for.
			deps := lookupDeps{}
			want := deps.answer(
				tc.record.Node,
				schedulerv1.SandboxLocation_SANDBOX_LOCATION_BOUND,
				"",
				tc.record.ExecutionID,
				authorityFor(tc.record.ExecutionID),
			)
			got := routing.Synthesize(tc.record)

			if !proto.Equal(got, want) {
				t.Fatalf("the gateway's synthesized answer differs from the scheduler's:\n got %v\nwant %v", got, want)
			}
		})
	}
}

// TestAuthorityForIsTheSharedRule is two assertions, and the first is the one
// that matters.
//
// 🔴 authorityFor delegates to routing.AuthorityFor today, so comparing the two
// is comparing a function with itself — it would go on passing if the rule
// itself inverted. The expected values are therefore spelled out, and the
// cross-check is kept beside them as a drift guard for the day somebody
// reintroduces a local implementation: from that commit on the two assertions
// stop being the same one.
func TestAuthorityForIsTheSharedRule(t *testing.T) {
	cases := map[string]schedulerv1.ExecutionAuthority{
		"":                                     schedulerv1.ExecutionAuthority_EXECUTION_AUTHORITY_UNKNOWN,
		"   ":                                  schedulerv1.ExecutionAuthority_EXECUTION_AUTHORITY_UNKNOWN,
		"0198b7cc-1111-7000-8000-000000000001": schedulerv1.ExecutionAuthority_EXECUTION_AUTHORITY_REGISTRY,
	}
	for executionID, want := range cases {
		if got := authorityFor(executionID); got != want {
			t.Fatalf("authorityFor(%q) = %v, want %v", executionID, got, want)
		}
		if got, shared := authorityFor(executionID), routing.AuthorityFor(executionID); got != shared {
			t.Fatalf("authorityFor(%q) = %v, the shared rule says %v", executionID, got, shared)
		}
	}
}

// TestSilentExecutionIsNotVisibleToADirectReader records the one thing the
// direct read path cannot reproduce, so nobody later mistakes it for an
// oversight.
//
// The scheduler's rollback for the incarnation axis blanks both incarnation
// fields on the way out of a lookup answer. A gateway reading the projection
// itself never passes through that code and so never sees the blanking. There
// is no mechanism proposed for it: the two switches are flipped together as an
// operational rule, and this test exists to make the gap explicit rather than
// to close it.
func TestSilentExecutionIsNotVisibleToADirectReader(t *testing.T) {
	record := routing.Record{
		Node:        Node{ID: "node-a", Endpoint: "http://node-a"},
		ExecutionID: "0198b7cc-1111-7000-8000-000000000001",
	}
	rollback := lookupDeps{silentExecution: true}
	viaScheduler := rollback.answer(
		record.Node,
		schedulerv1.SandboxLocation_SANDBOX_LOCATION_BOUND,
		"",
		record.ExecutionID,
		authorityFor(record.ExecutionID),
	)
	if viaScheduler.GetExecutionId() != "" {
		t.Fatal("precondition failed: the rollback mode is supposed to blank the incarnation")
	}

	viaProjection := routing.Synthesize(record)
	if viaProjection.GetExecutionId() == "" {
		t.Fatal("a direct read is not expected to reproduce the rollback; if it now does, the pairing rule in the config comments is obsolete and should be deleted")
	}
}
