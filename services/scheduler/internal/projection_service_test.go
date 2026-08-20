package scheduler

import (
	"context"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	"go.uber.org/zap"
)

func newProjectionService(t *testing.T, store BindingStore, opts ...ServiceOption) *Service {
	t.Helper()
	registry := NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, defaultObservedReportTTL)
	return NewService(zap.NewNop(), registry, NewStrategy("round_robin"), store, opts...)
}

func authoritative(maxTTL time.Duration) ServiceOption {
	return WithAuthoritativeProjection(maxTTL)
}

func recordAssignment(t *testing.T, service *Service, sandboxID string, executionID string, ttlSecs uint32) {
	t.Helper()
	_, err := service.RecordAssignment(context.Background(), &schedulerv1.RecordAssignmentRequest{
		SandboxId:         sandboxID,
		Node:              (&Node{ID: "node-a", Endpoint: "http://node-a"}).ToProto(),
		ExecutionId:       executionID,
		ProjectionTtlSecs: ttlSecs,
	})
	if err != nil {
		t.Fatalf("record assignment failed: %v", err)
	}
}

func reportEvent(t *testing.T, service *Service, event *schedulerv1.SandboxEvent) {
	t.Helper()
	_, err := service.ReportSandboxEvent(context.Background(), &schedulerv1.ReportSandboxEventRequest{
		NodeId: "node-a",
		Events: []*schedulerv1.SandboxEvent{event},
	})
	if err != nil {
		// 🔴 Never an error, whatever happened inside: the node has no queue to
		// retry from, and the heartbeat is the repair path.
		t.Fatalf("ReportSandboxEvent must never fail the caller, got %v", err)
	}
}

func bound(t *testing.T, store BindingStore, sandboxID string) bool {
	t.Helper()
	_, ok, err := store.Get(sandboxID, time.Now())
	if err != nil {
		t.Fatalf("get %s failed: %v", sandboxID, err)
	}
	return ok
}

// TestReportSandboxEventIsANoOpWhileTheSwitchIsOff is the property that makes
// the code default safe.
//
// 🔴 Every node in the fleet already sends these events. A scheduler that
// upgraded and started acting on them without being asked would change what a
// cluster does to its own routing table at the moment a pod restarted. Off has
// to be indistinguishable from the build before this one.
func TestReportSandboxEventIsANoOpWhileTheSwitchIsOff(t *testing.T) {
	store := NewInMemoryBindingStore(30 * time.Second)
	service := newProjectionService(t, store)

	recordAssignment(t, service, "sbx-1", execA, 0)
	reportEvent(t, service, &schedulerv1.SandboxEvent{
		SandboxId:   "sbx-1",
		EventType:   schedulerv1.SandboxEventType_SANDBOX_EVENT_TYPE_DELETE,
		ExecutionId: execA,
	})

	if !bound(t, store, "sbx-1") {
		t.Fatal("the switch is off and the record was deleted anyway")
	}
}

func TestReportSandboxEventDeletesOnMatchingIncarnation(t *testing.T) {
	store := NewInMemoryBindingStore(30 * time.Second)
	service := newProjectionService(t, store, authoritative(25*time.Hour))

	for _, eventType := range []schedulerv1.SandboxEventType{
		schedulerv1.SandboxEventType_SANDBOX_EVENT_TYPE_DELETE,
		schedulerv1.SandboxEventType_SANDBOX_EVENT_TYPE_PAUSE,
	} {
		recordAssignment(t, service, "sbx-1", execA, 0)
		if !bound(t, store, "sbx-1") {
			t.Fatalf("%v: precondition failed, nothing recorded", eventType)
		}
		reportEvent(t, service, &schedulerv1.SandboxEvent{
			SandboxId:   "sbx-1",
			EventType:   eventType,
			ExecutionId: execA,
		})
		if bound(t, store, "sbx-1") {
			t.Fatalf("%v: the record survived a matching event", eventType)
		}
	}
}

// TestReportSandboxEventRefusesAStaleIncarnation is the guard, driven end to
// end through the RPC.
//
// The sequence it stands for is real: a sandbox is created under one
// incarnation, deleted, and a sandbox with the same id starts again elsewhere
// under a second. The first delete's event, arriving late, must not take the
// second one's record with it.
func TestReportSandboxEventRefusesAStaleIncarnation(t *testing.T) {
	store := NewInMemoryBindingStore(30 * time.Second)
	service := newProjectionService(t, store, authoritative(25*time.Hour))

	recordAssignment(t, service, "sbx-1", execB, 0)
	reportEvent(t, service, &schedulerv1.SandboxEvent{
		SandboxId:   "sbx-1",
		EventType:   schedulerv1.SandboxEventType_SANDBOX_EVENT_TYPE_DELETE,
		ExecutionId: execA,
	})

	if !bound(t, store, "sbx-1") {
		t.Fatal("a stale event deleted the live record")
	}
}

// TestReportSandboxEventIgnoresAnUnnamedIncarnation: a reporter too old to name
// one gets no unguarded delete. It also sends no long TTL, so its records
// expire on their own within binding_ttl and nothing is leaked by waiting.
//
// 🔴 Two fixtures, and the second one is the test. Against a record that names
// an incarnation, an unnamed event is refused by the *store* — its guard sees
// "somebody else's" and answers rejected_stale — so a version of this with only
// that fixture passes with the service's guard deleted, which is how it
// shipped. The case the guard alone covers is an unnamed event against a record
// whose incumbent is unknown too: there the store's rule is that an unknown
// incumbent yields to a named challenger, and an empty challenger would match
// it and take the record with it.
//
// 🔴 The outcome is asserted as well as the record, because the two stores
// refuse this at different depths — the in-memory one now defends itself and
// the Lua one always did — and "the record survived" cannot tell a guard that
// fired from a store that quietly did nothing. `ignored_unknown_execution` is
// the row §5.3 names, and only the service's guard can produce it.
func TestReportSandboxEventIgnoresAnUnnamedIncarnation(t *testing.T) {
	store := NewInMemoryBindingStore(30 * time.Second)
	service := newProjectionService(t, store, authoritative(25*time.Hour))

	// A record that names an incarnation, and one that does not — which is
	// what a create recorded by a gateway that read no incarnation off the
	// response looks like.
	recordAssignment(t, service, "sbx-1", execA, 0)
	recordAssignment(t, service, "sbx-2", "", 0)
	if !bound(t, store, "sbx-2") {
		t.Fatal("precondition failed: nothing recorded for the unnamed-incumbent fixture")
	}

	for _, sandboxID := range []string{"sbx-1", "sbx-2"} {
		for _, executionID := range []string{"", "   ", "not-a-uuid"} {
			before := sandboxEventCount(t, "delete", sandboxEventIgnoredUnknownExecution)
			reportEvent(t, service, &schedulerv1.SandboxEvent{
				SandboxId:   sandboxID,
				EventType:   schedulerv1.SandboxEventType_SANDBOX_EVENT_TYPE_DELETE,
				ExecutionId: executionID,
			})
			if !bound(t, store, sandboxID) {
				t.Fatalf("%s: an event carrying %q deleted the record without a guard", sandboxID, executionID)
			}
			if after := sandboxEventCount(t, "delete", sandboxEventIgnoredUnknownExecution); after != before+1 {
				t.Fatalf("%s: an event carrying %q was counted as something other than %s (%v -> %v)",
					sandboxID, executionID, sandboxEventIgnoredUnknownExecution, before, after)
			}
		}
	}
}

// sandboxEventCount reads one series of the sandbox-event counter.
func sandboxEventCount(t *testing.T, eventType string, outcome string) float64 {
	t.Helper()
	return counterSeriesValue(t, schedulerSandboxEvent,
		"agentenv_scheduler_sandbox_event_total",
		map[string]string{"event_type": eventType, "outcome": outcome})
}

// TestReportSandboxEventLeavesTheOtherEventTypesAlone: create, resume and fork
// have their projection written by the gateway, synchronously, on the response.
// Acting on them here would be a second writer with none of that ordering.
func TestReportSandboxEventLeavesTheOtherEventTypesAlone(t *testing.T) {
	store := NewInMemoryBindingStore(30 * time.Second)
	service := newProjectionService(t, store, authoritative(25*time.Hour))

	recordAssignment(t, service, "sbx-1", execA, 0)
	for _, eventType := range []schedulerv1.SandboxEventType{
		schedulerv1.SandboxEventType_SANDBOX_EVENT_TYPE_CREATE,
		schedulerv1.SandboxEventType_SANDBOX_EVENT_TYPE_RESUME,
		schedulerv1.SandboxEventType_SANDBOX_EVENT_TYPE_FORK,
		schedulerv1.SandboxEventType_SANDBOX_EVENT_TYPE_UNSPECIFIED,
	} {
		reportEvent(t, service, &schedulerv1.SandboxEvent{
			SandboxId:   "sbx-1",
			EventType:   eventType,
			ExecutionId: execA,
		})
		if !bound(t, store, "sbx-1") {
			t.Fatalf("%v removed a record it has no business touching", eventType)
		}
	}
}

// TestReportSandboxEventSurvivesAFailingStore: best effort means best effort.
func TestReportSandboxEventSurvivesAFailingStore(t *testing.T) {
	service := newProjectionService(t, failingBindingStore{}, authoritative(25*time.Hour))
	reportEvent(t, service, &schedulerv1.SandboxEvent{
		SandboxId:   "sbx-1",
		EventType:   schedulerv1.SandboxEventType_SANDBOX_EVENT_TYPE_DELETE,
		ExecutionId: execA,
	})
}

// TestResolveProjectionTTL walks the three sources, and pins the rule that has
// gone wrong elsewhere.
func TestResolveProjectionTTL(t *testing.T) {
	cases := []struct {
		name       string
		opts       []ServiceOption
		raw        time.Duration
		wantTTL    time.Duration
		wantSource string
	}{
		{
			name:       "switch off ignores the node's budget entirely",
			raw:        time.Hour,
			wantTTL:    0,
			wantSource: projectionTTLSourceDefault,
		},
		{
			name:       "the node's budget is used as sent",
			opts:       []ServiceOption{authoritative(25 * time.Hour)},
			raw:        time.Hour,
			wantTTL:    time.Hour,
			wantSource: projectionTTLSourceNode,
		},
		{
			// 🔴 Zero is "no budget offered", which lands on the store's own
			// binding_ttl. It is never "no expiry": the one shipped
			// implementation that read it the other way did so by dividing to
			// zero and handing the result straight to a SET with no TTL.
			name:       "zero means the receiver's default",
			opts:       []ServiceOption{authoritative(25 * time.Hour)},
			raw:        0,
			wantTTL:    0,
			wantSource: projectionTTLSourceDefault,
		},
		{
			name:       "a negative budget means the receiver's default too",
			opts:       []ServiceOption{authoritative(25 * time.Hour)},
			raw:        -time.Hour,
			wantTTL:    0,
			wantSource: projectionTTLSourceDefault,
		},
		{
			name:       "over the ceiling is clamped, not refused",
			opts:       []ServiceOption{authoritative(time.Hour)},
			raw:        48 * time.Hour,
			wantTTL:    time.Hour,
			wantSource: projectionTTLSourceClamped,
		},
		{
			name:       "exactly the ceiling is not clamped",
			opts:       []ServiceOption{authoritative(time.Hour)},
			raw:        time.Hour,
			wantTTL:    time.Hour,
			wantSource: projectionTTLSourceNode,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			service := newProjectionService(t, NewInMemoryBindingStore(30*time.Second), tc.opts...)
			gotTTL, gotSource := service.resolveProjectionTTL(tc.raw)
			if gotTTL != tc.wantTTL || gotSource != tc.wantSource {
				t.Fatalf("got (%s, %s), want (%s, %s)", gotTTL, gotSource, tc.wantTTL, tc.wantSource)
			}
		})
	}
}

// TestProjectionTTLFromSecsNeverMeansForever pins the wire conversion on its
// own: no value of the unsigned field may produce something a store could read
// as "keep this key until somebody deletes it".
func TestProjectionTTLFromSecsNeverMeansForever(t *testing.T) {
	if got := projectionTTLFromSecs(0); got != 0 {
		t.Fatalf("zero seconds = %s, want 0 so the receiver falls back to its default", got)
	}
	if got := projectionTTLFromSecs(1); got != time.Second {
		t.Fatalf("one second = %s", got)
	}
	if got := projectionTTLFromSecs(86460); got != 86460*time.Second {
		t.Fatalf("a day plus grace = %s", got)
	}
}

// assertServiceExpiry pins a record's deadline from both sides.
//
// 🔴 Both sides, always. A one-sided assertion on this value cannot tell the
// number it is checking from two of the failures it exists to catch: a lower
// bound passes a record with no deadline at all, and an upper bound passes one
// that collapsed to the receiver's 30-second default — which is exactly what a
// clamp implemented as "ignore it" would produce, under a test named for
// clamping. The tolerance covers the wall clock the service reads inside.
func assertServiceExpiry(t *testing.T, store *InMemoryBindingStore, sandboxID string, reference time.Time, want time.Duration) {
	t.Helper()

	store.mu.Lock()
	record, ok := store.bindings[sandboxID]
	store.mu.Unlock()
	if !ok {
		t.Fatalf("no record for %s", sandboxID)
	}
	got := record.expiresAt.Sub(reference)
	const tolerance = 5 * time.Second
	if got < want-tolerance || got > want+tolerance {
		t.Fatalf("%s expires %s after the write, want %s (+/- %s)", sandboxID, got, want, tolerance)
	}
}

// TestRecordAssignmentCarriesTheBudgetToTheStore is the create path end to end.
func TestRecordAssignmentCarriesTheBudgetToTheStore(t *testing.T) {
	store := NewInMemoryBindingStore(30 * time.Second)
	service := newProjectionService(t, store, authoritative(25*time.Hour))

	before := time.Now()
	recordAssignment(t, service, "sbx-1", execA, 3600)
	assertServiceExpiry(t, store, "sbx-1", before, time.Hour)
}

// TestRecordAssignmentIgnoresTheBudgetWhileTheSwitchIsOff keeps "off" equal to
// today: a record written by a new node against an unswitched scheduler gets
// binding_ttl, exactly as before the field existed.
func TestRecordAssignmentIgnoresTheBudgetWhileTheSwitchIsOff(t *testing.T) {
	store := NewInMemoryBindingStore(30 * time.Second)
	service := newProjectionService(t, store)

	before := time.Now()
	recordAssignment(t, service, "sbx-1", execA, 86460)
	assertServiceExpiry(t, store, "sbx-1", before, 30*time.Second)
}

// TestHeartbeatRosterCarriesTheBudget covers the repair path: a projection
// write that was lost has to be reinstalled with the sandbox's real budget, not
// with the receiver's default.
func TestHeartbeatRosterCarriesTheBudget(t *testing.T) {
	store := NewInMemoryBindingStore(30 * time.Second)
	service := newProjectionService(t, store, authoritative(25*time.Hour))

	before := time.Now()
	_, err := service.Heartbeat(context.Background(), &schedulerv1.HeartbeatRequest{
		NodeId:            "node-a",
		ClusterId:         "cluster-1",
		ServiceInstanceId: "svc-1",
		Roster: []*schedulerv1.SandboxRosterEntry{
			{SandboxId: "sbx-1", ExecutionId: execA, ProjectionTtlSecs: 3600},
			{SandboxId: "sbx-2", ExecutionId: execB},
		},
	})
	if err != nil {
		t.Fatalf("heartbeat failed: %v", err)
	}

	assertServiceExpiry(t, store, "sbx-1", before, time.Hour)
	assertServiceExpiry(t, store, "sbx-2", before, 30*time.Second)
}

// TestHeartbeatRosterBudgetIsClamped: the ceiling is the storage owner's limit
// on writers, and clamping degrades to a lookup miss rather than to a failure.
func TestHeartbeatRosterBudgetIsClamped(t *testing.T) {
	store := NewInMemoryBindingStore(30 * time.Second)
	service := newProjectionService(t, store, authoritative(time.Hour))

	before := time.Now()
	_, err := service.Heartbeat(context.Background(), &schedulerv1.HeartbeatRequest{
		NodeId:            "node-a",
		ClusterId:         "cluster-1",
		ServiceInstanceId: "svc-1",
		Roster:            []*schedulerv1.SandboxRosterEntry{{SandboxId: "sbx-1", ExecutionId: execA, ProjectionTtlSecs: 86460}},
	})
	if err != nil {
		t.Fatalf("heartbeat failed: %v", err)
	}

	// 🔴 The ceiling, from both sides. "Clamped" means the record got the
	// ceiling — not that it got something below it, which a clamp collapsing to
	// the 30-second default would also satisfy under this name.
	assertServiceExpiry(t, store, "sbx-1", before, time.Hour)
}

// TestSandboxEventTypeLabelIsClosed keeps the metric's label set from opening
// up to whatever a newer node sends.
func TestSandboxEventTypeLabelIsClosed(t *testing.T) {
	want := map[schedulerv1.SandboxEventType]string{
		schedulerv1.SandboxEventType_SANDBOX_EVENT_TYPE_CREATE: "create",
		schedulerv1.SandboxEventType_SANDBOX_EVENT_TYPE_DELETE: "delete",
		schedulerv1.SandboxEventType_SANDBOX_EVENT_TYPE_PAUSE:  "pause",
		schedulerv1.SandboxEventType_SANDBOX_EVENT_TYPE_RESUME: "resume",
		schedulerv1.SandboxEventType_SANDBOX_EVENT_TYPE_FORK:   "fork",
	}
	for eventType, label := range want {
		if got := sandboxEventTypeLabel(eventType); got != label {
			t.Fatalf("%v labelled %q, want %q", eventType, got, label)
		}
	}
	if got := sandboxEventTypeLabel(schedulerv1.SandboxEventType(999)); got != "other" {
		t.Fatalf("an unknown type labelled %q, want other", got)
	}
}

// TestNormalizeExecutionIDReasonDoesNotTouchTheRosterCounter reads the counter
// it is named for.
//
// The event path has to use the quiet normaliser, because the counter the other
// one increments is named and documented for rosters and is read to decide
// whether the fleet has finished upgrading. Merging the two facts into one
// number would make that reading wrong in a way nothing else would show — so
// the counter is sampled either side of each call, and the shape assertions
// stay alongside.
func TestNormalizeExecutionIDReasonDoesNotTouchTheRosterCounter(t *testing.T) {
	cases := []struct {
		raw        string
		want       string
		wantReason string
	}{
		{raw: "  " + execA + "  ", want: execA, wantReason: ""},
		{raw: "", want: "", wantReason: "no_execution"},
		{raw: "nope", want: "", wantReason: "bad_uuid"},
	}
	for _, tc := range cases {
		before := map[string]float64{
			"no_execution": rosterDroppedCount(t, "no_execution"),
			"bad_uuid":     rosterDroppedCount(t, "bad_uuid"),
		}
		got, reason := normalizeExecutionIDReason(tc.raw)
		if got != tc.want || reason != tc.wantReason {
			t.Fatalf("normalizeExecutionIDReason(%q) = (%q, %q), want (%q, %q)", tc.raw, got, reason, tc.want, tc.wantReason)
		}
		for label, was := range before {
			if now := rosterDroppedCount(t, label); now != was {
				t.Fatalf("normalizeExecutionIDReason(%q) moved the roster counter %s from %v to %v", tc.raw, label, was, now)
			}
		}
	}

	// The control face: the roster normaliser over the same inputs *does* move
	// it, which is what makes the assertion above a statement about these two
	// functions rather than about a counter nothing ever increments.
	before := rosterDroppedCount(t, "bad_uuid")
	if got := normalizeExecutionID("nope"); got != "" {
		t.Fatalf("normalizeExecutionID(%q) = %q, want empty", "nope", got)
	}
	if now := rosterDroppedCount(t, "bad_uuid"); now != before+1 {
		t.Fatalf("the roster normaliser did not count a drop: %v -> %v", before, now)
	}
}
