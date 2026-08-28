package gateway

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	"go.uber.org/zap"
)

const maxCursorSandboxID = "ffffffff-ffff-ffff-ffff-ffffffffffff"

type listedSandbox struct {
	TemplateID  string            `json:"templateID"`
	Alias       *string           `json:"alias,omitempty"`
	SandboxID   string            `json:"sandboxID"`
	ClientID    string            `json:"clientID"`
	StartedAt   time.Time         `json:"startedAt"`
	EndAt       time.Time         `json:"endAt"`
	CPUCount    uint32            `json:"cpuCount"`
	MemoryMB    uint32            `json:"memoryMB"`
	DiskSizeMB  uint32            `json:"diskSizeMB"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	State       string            `json:"state"`
	EnvdVersion string            `json:"envdVersion"`
	// ExecutionID names which run of this sandbox the row describes.
	//
	// 🔴 It has to be declared here even though the gateway only passes it
	// through: this struct is decoded from the node's JSON, and a field it does
	// not name is discarded without a word. The endpoint most likely to be used
	// to find a sandbox live in two places would then be the one endpoint unable
	// to show it apart.
	//
	// Read-only, and omitted when the node did not send one — which is what a
	// node from before this field looks like.
	ExecutionID string `json:"executionID,omitempty"`
}

type clusterListResult struct {
	items []listedSandbox
	err   error
}

type clusterListStatusError struct {
	statusCode int
	message    string
}

func (e *clusterListStatusError) Error() string {
	return e.message
}

func isClusterListRequest(r *http.Request) bool {
	if r.Method != http.MethodGet {
		return false
	}
	switch canonicalClusterListPath(r.URL.Path) {
	case "/sandboxes", "/v2/sandboxes":
		return true
	default:
		return false
	}
}

// fansOutClusterList reports whether this gateway still builds the cluster-wide
// sandbox listing itself, by asking every node for its own rows.
//
// 🔴 One value decides it, and it is the same value that decides every other
// user-facing REST call: `rest_upstream_addr`. Unset, this fan-out is the only
// thing in the cluster that can answer `GET /sandboxes` — no single process
// holds every node's sandboxes — so it falls back to the merge, the
// deduplication and the pagination below. Set, the api half owns the cluster
// ledger and answers the same list from one read, so a cluster-list request is
// claimed by nothing here and falls through to `forwardToRestUpstream` with
// the rest of the REST surface.
//
// 🔴 `rest_upstream_addr` can no longer be unset in a validated deployment —
// `services/shared/config`'s `Config.Validate` refuses to load a gateway
// config with it empty, because 阶段 3b already flipped the DaemonSet to
// `aenv-node`, and the role gate answers `GET /sandboxes` and
// `GET /v2/sandboxes` with 404 there, which are exactly the two routes this
// fan-out would call. So this function is expected to always return false on
// a real gateway; the fan-out below survives only because this package's own
// tests still construct an unconfigured `*Server` to exercise it, and because
// deleting it here would mean deleting the belt-and-braces coverage those
// tests give the merge/dedup/pagination logic — see rest_upstream.go for why
// an empty value is kept constructible at all.
//
// 🔴 Why the listing moved with that value rather than getting a switch of its
// own, while both still existed: the fan-out is all-or-nothing
// (`fetchClusterList` cancels the rest on the first failure) and
// `handleClusterList` passes a 4xx through verbatim, so a node fleet on
// `aenv-node` turns the user's `GET /sandboxes` into a bare 404 rather than
// into a degraded list. A separate switch would have meant 3b's correctness
// depending on two values being flipped in the right order; with one, the
// position that took REST off the nodes was the position that stopped asking
// nodes for this list.
func (s *Server) fansOutClusterList(r *http.Request) bool {
	return isClusterListRequest(r) && s.restUpstream == ""
}

func canonicalClusterListPath(path string) string {
	trimmed := strings.TrimRight(strings.TrimSpace(path), "/")
	if trimmed == "" {
		return "/"
	}
	return trimmed
}

func (s *Server) handleClusterList(w http.ResponseWriter, r *http.Request, routingCtx context.Context) {
	rpcStart := time.Now()
	resp, err := s.scheduler.ListNodes(routingCtx, &schedulerv1.ListNodesRequest{})
	recordGatewaySchedulerRPC("ListNodes", rpcStart, err)
	if err != nil {
		s.writeSchedulerError(w, err)
		return
	}

	items, err := s.fetchClusterList(routingCtx, r, resp.GetNodes())
	if err != nil {
		var statusErr *clusterListStatusError
		if errors.As(err, &statusErr) && statusErr.statusCode >= 400 && statusErr.statusCode < 500 {
			http.Error(w, statusErr.message, statusErr.statusCode)
			return
		}

		s.logger.Warn("cluster sandbox list failed",
			zap.Error(err),
			zap.String("method", r.Method),
			zap.String("path", r.URL.Path),
		)
		http.Error(w, "cluster list unavailable", http.StatusBadGateway)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if canonicalClusterListPath(r.URL.Path) != "/v2/sandboxes" {
		s.writeJSON(w, http.StatusOK, items)
		return
	}

	limit, err := parseClusterListLimit(r)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid limit: %v", err), http.StatusBadRequest)
		return
	}

	page, nextToken, err := paginateListedSandboxes(items, r.URL.Query().Get("nextToken"), limit)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid next token: %v", err), http.StatusBadRequest)
		return
	}
	if nextToken != "" {
		w.Header().Set("x-next-token", nextToken)
	}
	s.writeJSON(w, http.StatusOK, page)
}

func (s *Server) fetchClusterList(ctx context.Context, incoming *http.Request, nodes []*schedulerv1.Node) ([]listedSandbox, error) {
	if len(nodes) == 0 {
		return nil, fmt.Errorf("no scheduler nodes available")
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	results := make(chan clusterListResult, len(nodes))
	var wg sync.WaitGroup

	for _, node := range nodes {
		node := node
		wg.Add(1)
		go func() {
			defer wg.Done()
			items, err := s.fetchNodeClusterList(ctx, incoming, node)
			if err != nil {
				cancel()
				results <- clusterListResult{
					err: fmt.Errorf("node %s list failed: %w", node.GetNodeId(), err),
				}
				return
			}
			results <- clusterListResult{items: items}
		}()
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	merged := make([]listedSandbox, 0)
	var firstErr error
	var errOnce sync.Once
	for result := range results {
		if result.err != nil {
			errOnce.Do(func() { firstErr = result.err })
			continue
		}
		merged = append(merged, result.items...)
	}
	if firstErr != nil {
		return nil, firstErr
	}

	sortListedSandboxes(merged)
	return dedupListedSandboxes(merged), nil
}

func (s *Server) fetchNodeClusterList(ctx context.Context, incoming *http.Request, node *schedulerv1.Node) ([]listedSandbox, error) {
	target, err := joinUpstream(
		node.GetEndpoint(),
		incoming.URL.Path,
		requestEscapedPath(incoming),
		clusterListRawQuery(incoming),
	)
	if err != nil {
		return nil, fmt.Errorf("build upstream url: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nil, fmt.Errorf("build upstream request: %w", err)
	}
	req.Header = incoming.Header.Clone()
	req.Host = incoming.Host
	injectForwardedHeaders(req.Header, incoming)
	// 🔴 This fan-out does not go through the reverse proxy, so it does not get
	// the Rewrite hook's headers. Two consequences, both silent if missed: the
	// client's own copy of these headers would be forwarded verbatim, and the
	// gateway would not identify itself — and this endpoint is all-or-nothing,
	// so one node refusing takes the whole cluster listing with it rather than
	// one node's rows. No incarnation is stamped: this is not a data-plane
	// request against a single sandbox.
	s.stampOutboundGatewayHeaders(req.Header, "")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("perform upstream request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, &clusterListStatusError{
			statusCode: resp.StatusCode,
			message:    http.StatusText(resp.StatusCode),
		}
	}

	items := make([]listedSandbox, 0)
	if err := json.NewDecoder(resp.Body).Decode(&items); err != nil {
		return nil, fmt.Errorf("decode upstream response: %w", err)
	}
	return items, nil
}

func clusterListRawQuery(r *http.Request) string {
	if canonicalClusterListPath(r.URL.Path) != "/v2/sandboxes" {
		return r.URL.RawQuery
	}

	query := r.URL.Query()
	query.Del("nextToken")
	query.Del("limit")
	return query.Encode()
}

func parseClusterListLimit(r *http.Request) (*int, error) {
	raw := strings.TrimSpace(r.URL.Query().Get("limit"))
	if raw == "" {
		return nil, nil
	}
	limit, err := strconv.Atoi(raw)
	if err != nil {
		return nil, err
	}
	return &limit, nil
}

func sortListedSandboxes(items []listedSandbox) {
	sort.Slice(items, func(i, j int) bool {
		if items[i].StartedAt.Equal(items[j].StartedAt) {
			return items[i].SandboxID < items[j].SandboxID
		}
		return items[i].StartedAt.After(items[j].StartedAt)
	})
}

// Why a duplicate was resolved the way it was. Two values, closed.
const (
	// clusterListDuplicateByExecution — the rows named different incarnations
	// and the newer one won.
	clusterListDuplicateByExecution = "by_execution"
	// clusterListDuplicateKeepFirst — the rows could not be told apart, so the
	// sort order decided. This is the old behaviour, kept for the rows that
	// still cannot supply an incarnation.
	clusterListDuplicateKeepFirst = "keep_first"
)

// dedupListedSandboxes collapses rows for the same sandbox returned by more than
// one node.
//
// 🔴 The two rows are indistinguishable by everything the sort orders on:
// startedAt comes from the sandbox's creation time and is only reset by a fork,
// so a resume carries the original one, and the sandbox id is the same by
// definition. The comparison therefore answers false in both directions, and
// sort.Slice is not stable — the surviving row was whichever one the sort
// happened to leave in front, and two calls against the same cluster could
// answer differently, with the state, the end time and the metadata all coming
// from a run that is over.
//
// The incarnation is what settles it: a UUIDv7 sorts in the order it was minted,
// so the larger one is the later one. When neither row can name one, the old
// keep-first fallback stands — it is no worse than before, and it is counted.
//
// 🔴 The registry is not consulted, and must not be. It holds no row for a
// sandbox that has never been paused, which is the most common kind, so using it
// to decide who is authoritative would delete those sandboxes from the listing
// outright. The tie-break has to be carried by the rows themselves.
func dedupListedSandboxes(items []listedSandbox) []listedSandbox {
	if len(items) < 2 {
		return items
	}

	// The winner replaces the loser in place rather than being appended, so the
	// order established by the sort survives the deduplication.
	at := make(map[string]int, len(items))
	deduped := make([]listedSandbox, 0, len(items))
	for _, item := range items {
		index, seen := at[item.SandboxID]
		if !seen {
			at[item.SandboxID] = len(deduped)
			deduped = append(deduped, item)
			continue
		}

		kept := deduped[index]
		incoming := normalizeExecutionID(item.ExecutionID)
		existing := normalizeExecutionID(kept.ExecutionID)
		if incoming == "" || existing == "" || incoming == existing {
			recordClusterListDuplicate(clusterListDuplicateKeepFirst)
			continue
		}
		recordClusterListDuplicate(clusterListDuplicateByExecution)
		if incoming > existing {
			deduped[index] = item
		}
	}
	return deduped
}

func paginateListedSandboxes(items []listedSandbox, nextToken string, limit *int) ([]listedSandbox, string, error) {
	cursorTime, cursorID, err := parseClusterListNextToken(nextToken)
	if err != nil {
		return nil, "", err
	}

	page := make([]listedSandbox, 0, len(items))
	for _, item := range items {
		if item.StartedAt.Before(cursorTime) || (item.StartedAt.Equal(cursorTime) && item.SandboxID > cursorID) {
			page = append(page, item)
		}
	}

	if limit != nil && *limit < len(page) {
		if *limit <= 0 {
			page = page[:0]
		} else {
			page = page[:*limit]
		}
	}

	return page, nextClusterListToken(page, limit), nil
}

func parseClusterListNextToken(token string) (time.Time, string, error) {
	token = strings.TrimSpace(token)
	if token == "" {
		return time.Now(), maxCursorSandboxID, nil
	}

	decoded, err := base64.URLEncoding.DecodeString(token)
	if err != nil {
		return time.Time{}, "", fmt.Errorf("error decoding cursor: %w", err)
	}

	parts := strings.SplitN(string(decoded), "__", 2)
	if len(parts) != 2 {
		return time.Time{}, "", fmt.Errorf("invalid cursor format")
	}

	cursorTime, err := time.Parse(time.RFC3339Nano, parts[0])
	if err != nil {
		return time.Time{}, "", fmt.Errorf("invalid timestamp format in cursor: %w", err)
	}
	if !isValidSandboxID(parts[1]) {
		return time.Time{}, "", fmt.Errorf("invalid sandbox id in cursor: %s", parts[1])
	}

	return cursorTime.UTC(), parts[1], nil
}

func nextClusterListToken(items []listedSandbox, limit *int) string {
	if limit == nil || *limit <= 0 || len(items) != *limit {
		return ""
	}
	last := items[len(items)-1]
	raw := fmt.Sprintf("%s__%s", last.StartedAt.UTC().Format(time.RFC3339Nano), last.SandboxID)
	return base64.URLEncoding.EncodeToString([]byte(raw))
}

func isValidSandboxID(id string) bool {
	if len(id) != 36 {
		return false
	}
	for i, ch := range id {
		switch i {
		case 8, 13, 18, 23:
			if ch != '-' {
				return false
			}
		default:
			if !isHexDigit(ch) {
				return false
			}
		}
	}
	return true
}

func isHexDigit(ch rune) bool {
	return (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f') || (ch >= 'A' && ch <= 'F')
}
