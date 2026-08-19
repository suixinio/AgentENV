package gateway

import (
	"context"
	"fmt"
	"net/http"
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

func isRegistryListRequest(r *http.Request) bool {
	if r.Method != http.MethodGet {
		return false
	}
	return strings.TrimRight(strings.TrimSpace(r.URL.Path), "/") == "/registry/sandboxes"
}

func (s *Server) handleRegistryList(w http.ResponseWriter, r *http.Request, routingCtx context.Context) {
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
