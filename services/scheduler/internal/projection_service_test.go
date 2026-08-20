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
func TestReportSandboxEventIgnoresAnUnnamedIncarnation(t *testing.T) {
	store := NewInMemoryBindingStore(30 * time.Second)
	service := newProjectionService(t, store, authoritative(25*time.Hour))

	recordAssignment(t, service, "sbx-1", execA, 0)
	for _, executionID := range []string{"", "   ", "not-a-uuid"} {
		reportEvent(t, service, &schedulerv1.SandboxEvent{
			SandboxId:   "sbx-1",
			EventType:   schedulerv1.SandboxEventType_SANDBOX_EVENT_TYPE_DELETE,
			ExecutionId: executionID,
		})
		if !bound(t, store, "sbx-1") {
			t.Fatalf("an event carrying %q deleted the record without a guard", executionID)
		}
	}
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

// TestRecordAssignmentCarriesTheBudgetToTheStore is the create path end to end.
func TestRecordAssignmentCarriesTheBudgetToTheStore(t *testing.T) {
	store := NewInMemoryBindingStore(30 * time.Second)
	service := newProjectionService(t, store, authoritative(25*time.Hour))

	before := time.Now()
	recordAssignment(t, service, "sbx-1", execA, 3600)

	store.mu.Lock()
	record := store.bindings["sbx-1"]
	store.mu.Unlock()
	if record.expiresAt.Before(before.Add(59 * time.Minute)) {
		t.Fatalf("the node's budget did not reach the store: expires at %s", record.expiresAt)
	}
}

// TestRecordAssignmentIgnoresTheBudgetWhileTheSwitchIsOff keeps "off" equal to
// today: a record written by a new node against an unswitched scheduler gets
// binding_ttl, exactly as before the field existed.
func TestRecordAssignmentIgnoresTheBudgetWhileTheSwitchIsOff(t *testing.T) {
	store := NewInMemoryBindingStore(30 * time.Second)
	service := newProjectionService(t, store)

	before := time.Now()
	recordAssignment(t, service, "sbx-1", execA, 86460)

	store.mu.Lock()
	record := store.bindings["sbx-1"]
	store.mu.Unlock()
	if record.expiresAt.After(before.Add(31 * time.Second)) {
		t.Fatalf("the switch is off but the budget was honoured: expires at %s", record.expiresAt)
	}
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

	store.mu.Lock()
	withBudget := store.bindings["sbx-1"]
	withoutBudget := store.bindings["sbx-2"]
	store.mu.Unlock()

	if withBudget.expiresAt.Before(before.Add(59 * time.Minute)) {
		t.Fatalf("the roster's budget did not reach the store: expires at %s", withBudget.expiresAt)
	}
	if withoutBudget.expiresAt.After(before.Add(31 * time.Second)) {
		t.Fatalf("an entry with no budget must fall back to binding_ttl, got %s", withoutBudget.expiresAt)
	}
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

	store.mu.Lock()
	record := store.bindings["sbx-1"]
	store.mu.Unlock()
	if record.expiresAt.After(before.Add(time.Hour + time.Minute)) {
		t.Fatalf("the ceiling was not applied: expires at %s", record.expiresAt)
	}
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

// TestNormalizeExecutionIDReasonDoesNotTouchTheRosterCounter is a shape
// assertion, not a metric one: the event path has to use the quiet normaliser,
// because the counter the other one increments is named and documented for
// rosters and is read to decide whether the fleet has finished upgrading.
func TestNormalizeExecutionIDReasonDoesNotTouchTheRosterCounter(t *testing.T) {
	if got, reason := normalizeExecutionIDReason("  " + execA + "  "); got != execA || reason != "" {
		t.Fatalf("got (%q, %q), want the canonical id and no drop", got, reason)
	}
	if got, reason := normalizeExecutionIDReason(""); got != "" || reason != "no_execution" {
		t.Fatalf("got (%q, %q), want an empty value and no_execution", got, reason)
	}
	if got, reason := normalizeExecutionIDReason("nope"); got != "" || reason != "bad_uuid" {
		t.Fatalf("got (%q, %q), want an empty value and bad_uuid", got, reason)
	}
}
