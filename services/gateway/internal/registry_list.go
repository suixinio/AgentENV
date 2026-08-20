package gateway

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// registrySandboxItem is one row of the node-owned paused registry, as the
// gateway renders it.
//
// The two lease timestamps are pointers so a NULL column stays a JSON null.
// Rendering NULL as 0 would be actively misleading: a NULL lease means "already
// expired" to the nodes, while a NULL sandbox deadline means "never expires" —
// two opposite meanings that would both come out as the same number.
type registrySandboxItem struct {
	SandboxID              string `json:"sandboxID"`
	ClusterID              string `json:"clusterID"`
	State                  string `json:"state"`
	Generation             int64  `json:"generation"`
	OriginNodeID           string `json:"originNodeID"`
	ClaimedByNodeID        string `json:"claimedByNodeID"`
	SnapshotID             string `json:"snapshotID"`
	HolderNodeID           string `json:"holderNodeID"`
	PausedAtUnixMs         int64  `json:"pausedAtUnixMs"`
	UpdatedAtUnixMs        int64  `json:"updatedAtUnixMs"`
	LeaseExpiresAtUnixMs   *int64 `json:"leaseExpiresAtUnixMs"`
	SandboxExpiresAtUnixMs *int64 `json:"sandboxExpiresAtUnixMs"`
	// ExecutionID is the incarnation this row is fenced against, empty when the
	// row's state pins the column to NULL.
	//
	// Read-only, and deliberately not filterable: registryListQueryParams is a
	// closed set and this is not in it. A parameter that selects by incarnation
	// is one step from a caller supplying one, and an incarnation supplied by a
	// caller is stale by construction.
	ExecutionID string `json:"executionID"`
}

type registryListResponse struct {
	Sandboxes []registrySandboxItem `json:"sandboxes"`
	// NextToken is absent on the last page.
	NextToken string `json:"nextToken,omitempty"`
	// DatabaseTimeUnixMs is the database clock the rows were read against.
	// Every lease field above is only meaningful against this, never against
	// the reader's own clock.
	DatabaseTimeUnixMs int64 `json:"databaseTimeUnixMs"`
}

// headerRegistryAPIKey is the credential this endpoint requires. The name is
// the one the nodes already check on their own API (`src/api/impls/auth.rs`),
// so an operator holding a key for the fleet does not need a second one to read
// this.
const headerRegistryAPIKey = "X-API-Key"

// registryListQueryParams is the closed set of query parameters this endpoint
// understands. Anything else is refused rather than ignored — see
// rejectUnknownRegistryListParams.
var registryListQueryParams = map[string]struct{}{
	"state":     {},
	"nodeID":    {},
	"limit":     {},
	"nextToken": {},
}

func isRegistryListRequest(r *http.Request) bool {
	if r.Method != http.MethodGet {
		return false
	}
	return strings.TrimRight(strings.TrimSpace(r.URL.Path), "/") == "/registry/sandboxes"
}

// rejectUnknownRegistryListParams refuses any query parameter outside the four
// this endpoint documents.
//
// 🔴 The alternative is what this replaces: `?nodeId=` (lower-case d) was
// dropped on the floor and the caller got every row in the cluster back with a
// 200, which reads exactly like "the filter matched everything". A filter that
// silently does not apply is worse than no filter, because the answer still
// looks like an answer.
func rejectUnknownRegistryListParams(r *http.Request) error {
	unknown := make([]string, 0, 1)
	for name := range r.URL.Query() {
		if _, ok := registryListQueryParams[name]; !ok {
			unknown = append(unknown, name)
		}
	}
	if len(unknown) == 0 {
		return nil
	}
	supported := make([]string, 0, len(registryListQueryParams))
	for name := range registryListQueryParams {
		supported = append(supported, name)
	}
	// Both sorted, so the message does not depend on map iteration order — and
	// the supported list is read off the same map the check uses, so it cannot
	// advertise a parameter that would be refused.
	sort.Strings(unknown)
	sort.Strings(supported)
	return fmt.Errorf("unknown query parameter(s) %s; supported: %s",
		strings.Join(unknown, ", "), strings.Join(supported, ", "))
}

func (s *Server) handleRegistryList(w http.ResponseWriter, r *http.Request, routingCtx context.Context) {
	// 🔴 The gateway has no authentication middleware: /nodes and this endpoint
	// are both served by the gateway itself, and only the paths proxied to a
	// node are checked at all — by that node. This one listing carries every
	// sandbox id in the cluster along with which machine holds it, so it does
	// not go out unauthenticated while that is being sorted out.
	//
	// Presence of a non-empty key is the whole check, which is the same bar the
	// nodes apply today. It is a door, not a lock: it keeps the listing off an
	// unauthenticated fetch, and it is not a substitute for the gateway
	// growing real credential validation.
	if strings.TrimSpace(r.Header.Get(headerRegistryAPIKey)) == "" {
		http.Error(w, headerRegistryAPIKey+" is required", http.StatusUnauthorized)
		return
	}

	if err := rejectUnknownRegistryListParams(r); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	pageSize, err := parseRegistryListLimit(r)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid limit: %v", err), http.StatusBadRequest)
		return
	}

	rpcStart := time.Now()
	resp, err := s.scheduler.ListRegistrySandboxes(routingCtx, &schedulerv1.ListRegistrySandboxesRequest{
		State:     strings.TrimSpace(r.URL.Query().Get("state")),
		NodeId:    strings.TrimSpace(r.URL.Query().Get("nodeID")),
		PageSize:  pageSize,
		PageToken: strings.TrimSpace(r.URL.Query().Get("nextToken")),
	})
	recordGatewaySchedulerRPC("ListRegistrySandboxes", rpcStart, err)
	if err != nil {
		s.writeRegistryError(w, err)
		return
	}

	items := make([]registrySandboxItem, 0, len(resp.GetSandboxes()))
	for _, sandbox := range resp.GetSandboxes() {
		items = append(items, registrySandboxItem{
			SandboxID:              sandbox.GetSandboxId(),
			ClusterID:              sandbox.GetClusterId(),
			State:                  sandbox.GetState(),
			Generation:             sandbox.GetGeneration(),
			OriginNodeID:           sandbox.GetOriginNodeId(),
			ClaimedByNodeID:        sandbox.GetClaimedByNodeId(),
			SnapshotID:             sandbox.GetSnapshotId(),
			HolderNodeID:           sandbox.GetHolderNodeId(),
			PausedAtUnixMs:         sandbox.GetPausedAtUnixMs(),
			UpdatedAtUnixMs:        sandbox.GetUpdatedAtUnixMs(),
			LeaseExpiresAtUnixMs:   optionalUnixMs(sandbox.GetLeaseExpiresAtUnixMs()),
			SandboxExpiresAtUnixMs: optionalUnixMs(sandbox.GetSandboxExpiresAtUnixMs()),
			ExecutionID:            sandbox.GetExecutionId(),
		})
	}

	s.writeJSON(w, http.StatusOK, registryListResponse{
		Sandboxes:          items,
		NextToken:          resp.GetNextPageToken(),
		DatabaseTimeUnixMs: resp.GetDatabaseNowUnixMs(),
	})
}

// writeRegistryError adds the one code this endpoint can produce that the
// shared mapping does not cover.
//
// FailedPrecondition here means this deployment was never pointed at a paused
// registry, which is a permanent property of the configuration rather than
// something a retry can fix — so it is 501, not the 503 that Unavailable (the
// registry exists but could not be read) maps to. Collapsing the two would
// leave an operator unable to tell "we do not run this" from "the database is
// down".
func (s *Server) writeRegistryError(w http.ResponseWriter, err error) {
	if st, ok := status.FromError(err); ok && st.Code() == codes.FailedPrecondition {
		http.Error(w, st.Message(), http.StatusNotImplemented)
		return
	}
	s.writeSchedulerError(w, err)
}

func parseRegistryListLimit(r *http.Request) (int32, error) {
	raw := strings.TrimSpace(r.URL.Query().Get("limit"))
	if raw == "" {
		return 0, nil
	}
	limit, err := strconv.ParseInt(raw, 10, 32)
	if err != nil {
		return 0, err
	}
	if limit < 0 {
		return 0, fmt.Errorf("must not be negative")
	}
	return int32(limit), nil
}

func optionalUnixMs(value int64) *int64 {
	if value == 0 {
		return nil
	}
	return &value
}
