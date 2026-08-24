package gateway

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/grpc"
)

// recordingListUpstream answers a cluster-list request with rows of its own and
// remembers the whole query it was asked, so a test can say *which* upstream
// built a listing and what reached it — not merely that a listing came back.
//
// The same type stands in for a node and for the api half on purpose: the two
// positions of the switch then differ by the address the gateway was given and
// by nothing else in the test.
type recordingListUpstream struct {
	server *httptest.Server
	rows   []listedSandbox
	// nextToken, when non-empty, is returned in x-next-token. Only the api half
	// ever paginates for itself; a node fake leaves this empty, which is what a
	// node answering the fan-out does.
	nextToken string

	mu       sync.Mutex
	requests []*url.URL
}

func newRecordingListUpstream(t *testing.T, nextToken string, rows ...listedSandbox) *recordingListUpstream {
	t.Helper()
	upstream := &recordingListUpstream{rows: rows, nextToken: nextToken}
	upstream.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstream.mu.Lock()
		captured := *r.URL
		upstream.requests = append(upstream.requests, &captured)
		upstream.mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		if upstream.nextToken != "" {
			w.Header().Set("x-next-token", upstream.nextToken)
		}
		_ = json.NewEncoder(w).Encode(upstream.rows)
	}))
	t.Cleanup(upstream.server.Close)
	return upstream
}

func (u *recordingListUpstream) hits() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return len(u.requests)
}

func (u *recordingListUpstream) lastRequest(t *testing.T) *url.URL {
	t.Helper()
	u.mu.Lock()
	defer u.mu.Unlock()
	if len(u.requests) == 0 {
		t.Fatal("this upstream was never asked for a listing")
	}
	return u.requests[len(u.requests)-1]
}

const (
	nodeListRowID = "00000000-0000-0000-0000-0000000000a1"
	apiListRowID  = "00000000-0000-0000-0000-0000000000b1"
)

// 🔴 The pair, and neither half is evidence without the other.
//
// "The api half built the listing" passes against a gateway that forwards
// everything, the gateway's own `/nodes` aggregation included. "A node built it"
// passes against a gateway that ignores the switch — which is exactly the bug
// this is here about, and the one that turns the first `GET /sandboxes` of
// 阶段 3b into a 404. The same request, through the same handler, against the
// same two fakes, differing only in whether an api address is configured, is
// what says the branch is live and that it branches.
//
// 🔴 Every "was not asked" below is read as a count that does not move while the
// other fake's count does, in this same run. A fake that was never wired up
// answers "was not asked" just as convincingly.
func TestTheRestUpstreamSwitchDecidesWhoAnswersTheClusterSandboxList(t *testing.T) {
	for _, path := range []string{"/sandboxes", "/v2/sandboxes"} {
		t.Run(path, func(t *testing.T) {
			node := newRecordingListUpstream(t, "",
				mustListedSandbox(nodeListRowID, "2026-01-01T00:00:01Z", "running", "envd-from-the-node"))
			api := newRecordingListUpstream(t, "",
				mustListedSandbox(apiListRowID, "2026-01-01T00:00:02Z", "running", "envd-from-the-api-half"))

			listNodes := 0
			scheduler := stubSchedulerClient{
				listNodesFunc: func(context.Context, *schedulerv1.ListNodesRequest, ...grpc.CallOption) (*schedulerv1.ListNodesResponse, error) {
					listNodes++
					return &schedulerv1.ListNodesResponse{
						Nodes: []*schedulerv1.Node{{NodeId: "node-a", Endpoint: node.server.URL}},
					}, nil
				},
			}

			off := newTestServer(t, scheduler, 5*time.Second, 1<<20)
			offResp := serve(t, off, httptest.NewRequest(http.MethodGet, path, nil))
			defer offResp.Body.Close()

			if offResp.StatusCode != http.StatusOK {
				t.Fatalf("with no api half configured, %s answered %d", path, offResp.StatusCode)
			}
			if got := sandboxIDs(decodeListedSandboxResponse(t, offResp.Body)); !equalStrings(got, []string{nodeListRowID}) {
				t.Fatalf("rows = %v, want the node's row: with no api half configured the fan-out is the only thing that can build this list", got)
			}
			if node.hits() != 1 {
				t.Fatalf("node hits = %d, want 1", node.hits())
			}
			if api.hits() != 0 {
				t.Fatalf("api hits = %d, want 0: nothing is configured to send it anything", api.hits())
			}
			if listNodes != 1 {
				t.Fatalf("ListNodes calls = %d, want 1: the fan-out walks the nodes the scheduler knows", listNodes)
			}

			on := newTestServer(t, scheduler, 5*time.Second, 1<<20, withRestUpstream(api.server.URL))
			onResp := serve(t, on, httptest.NewRequest(http.MethodGet, path, nil))
			defer onResp.Body.Close()

			if onResp.StatusCode != http.StatusOK {
				t.Fatalf("with an api half configured, %s answered %d", path, onResp.StatusCode)
			}
			if got := sandboxIDs(decodeListedSandboxResponse(t, onResp.Body)); !equalStrings(got, []string{apiListRowID}) {
				t.Fatalf("rows = %v, want the api half's row: it owns the cluster ledger and answers this from one read", got)
			}
			if api.hits() != 1 {
				t.Fatalf("api hits = %d, want 1", api.hits())
			}
			if got := api.lastRequest(t).Path; got != path {
				t.Fatalf("the api half was asked for %q, want %q: a REST path must arrive unrewritten", got, path)
			}
			// The counts that did not move, each against a fake the run above
			// proved is reachable.
			if node.hits() != 1 {
				t.Fatalf("node hits = %d, want the 1 it already had: no node may be asked for this list once the api half owns it", node.hits())
			}
			if listNodes != 1 {
				t.Fatalf("ListNodes calls = %d, want the 1 it already had: there is no fan-out left to walk the nodes for", listNodes)
			}
		})
	}
}

// 🔴 Both arms of the upstream counter move for this one route, which is what
// makes either arm readable on a cluster.
//
// The listing is the only user-facing REST route this process can serve out of
// the nodes without forwarding anything, so it is the one route that could
// quietly go on being served by nodes while the counter said no node served
// REST any more. 阶段 3b's rollback — the DaemonSet on `--role node` with this
// value emptied — is exactly that state, and it 404s.
func TestBothArmsOfTheUpstreamCounterMoveForTheClusterSandboxList(t *testing.T) {
	apiBefore := testutil.ToFloat64(gatewayRestUpstream.WithLabelValues(restUpstreamAPI))
	nodeBefore := testutil.ToFloat64(gatewayRestUpstream.WithLabelValues(restUpstreamNode))

	node := newRecordingListUpstream(t, "",
		mustListedSandbox(nodeListRowID, "2026-01-01T00:00:01Z", "running", "envd-from-the-node"))
	api := newRecordingListUpstream(t, "",
		mustListedSandbox(apiListRowID, "2026-01-01T00:00:02Z", "running", "envd-from-the-api-half"))
	scheduler := stubSchedulerClient{
		listNodesFunc: func(context.Context, *schedulerv1.ListNodesRequest, ...grpc.CallOption) (*schedulerv1.ListNodesResponse, error) {
			return &schedulerv1.ListNodesResponse{
				Nodes: []*schedulerv1.Node{{NodeId: "node-a", Endpoint: node.server.URL}},
			}, nil
		},
	}

	off := newTestServer(t, scheduler, 5*time.Second, 1<<20)
	offResp := serve(t, off, httptest.NewRequest(http.MethodGet, "/sandboxes", nil))
	_ = offResp.Body.Close()

	on := newTestServer(t, scheduler, 5*time.Second, 1<<20, withRestUpstream(api.server.URL))
	onResp := serve(t, on, httptest.NewRequest(http.MethodGet, "/sandboxes", nil))
	_ = onResp.Body.Close()

	if got := testutil.ToFloat64(gatewayRestUpstream.WithLabelValues(restUpstreamNode)) - nodeBefore; got != 1 {
		t.Fatalf("node arm moved by %v, want 1: a fanned-out listing is served by nodes and has to be counted against them", got)
	}
	if got := testutil.ToFloat64(gatewayRestUpstream.WithLabelValues(restUpstreamAPI)) - apiBefore; got != 1 {
		t.Fatalf("api arm moved by %v, want 1", got)
	}
}

// 🔴 Who paginates moves with the same value, and the two answers are opposite.
//
// The fan-out cannot forward `limit` or `nextToken` — it strips both, because a
// per-node limit is not a cluster limit — and mints the token itself from the
// merged rows. The api half is the whole listing already, so it must receive
// both verbatim and its own token must reach the client untouched. Getting this
// backwards is silent: a `nextToken` swallowed by the gateway looks like a first
// page, forever.
func TestWhoPaginatesTheClusterSandboxListMovesWithTheSwitch(t *testing.T) {
	const clientToken = "MjAyNi0wMS0wMVQwMDowMDowOVpfXzAwMDAwMDAwLTAwMDAtMDAwMC0wMDAwLTAwMDAwMDAwMDAwOQ=="
	const apiToken = "a-token-only-the-api-half-could-have-minted"
	const query = "?metadata=team%3Dalpha&state=running&limit=1&nextToken=" + clientToken

	node := newRecordingListUpstream(t, "",
		mustListedSandbox("00000000-0000-0000-0000-000000000003", "2026-01-01T00:00:03Z", "running", "envd-a"),
		mustListedSandbox("00000000-0000-0000-0000-000000000002", "2026-01-01T00:00:02Z", "running", "envd-b"),
	)
	api := newRecordingListUpstream(t, apiToken,
		mustListedSandbox(apiListRowID, "2026-01-01T00:00:02Z", "running", "envd-from-the-api-half"),
	)
	scheduler := stubSchedulerClient{
		listNodesFunc: func(context.Context, *schedulerv1.ListNodesRequest, ...grpc.CallOption) (*schedulerv1.ListNodesResponse, error) {
			return &schedulerv1.ListNodesResponse{
				Nodes: []*schedulerv1.Node{{NodeId: "node-a", Endpoint: node.server.URL}},
			}, nil
		},
	}

	off := newTestServer(t, scheduler, 5*time.Second, 1<<20)
	offResp := serve(t, off, httptest.NewRequest(http.MethodGet, "/v2/sandboxes"+query, nil))
	defer offResp.Body.Close()

	asked := node.lastRequest(t).Query()
	if asked.Get("metadata") != "team=alpha" || asked.Get("state") != "running" {
		t.Fatalf("the node was asked with %q: the filters are the node's to apply", node.lastRequest(t).RawQuery)
	}
	if asked.Get("limit") != "" || asked.Get("nextToken") != "" {
		t.Fatalf("the node was asked with %q: a per-node limit or cursor is not a cluster one", node.lastRequest(t).RawQuery)
	}
	offToken := offResp.Header.Get("x-next-token")
	if offToken == "" {
		t.Fatal("the fan-out minted no cursor for a page it filled to the limit")
	}
	if offToken == apiToken {
		t.Fatalf("x-next-token = %q, which is the api half's: nothing sent it this request", offToken)
	}

	on := newTestServer(t, scheduler, 5*time.Second, 1<<20, withRestUpstream(api.server.URL))
	onResp := serve(t, on, httptest.NewRequest(http.MethodGet, "/v2/sandboxes"+query, nil))
	defer onResp.Body.Close()

	forwarded := api.lastRequest(t).Query()
	if forwarded.Get("metadata") != "team=alpha" || forwarded.Get("state") != "running" {
		t.Fatalf("the api half was asked with %q, want the filters intact", api.lastRequest(t).RawQuery)
	}
	if forwarded.Get("limit") != "1" {
		t.Fatalf("limit reached the api half as %q, want 1: it pages the whole listing, so the limit is its to apply", forwarded.Get("limit"))
	}
	if forwarded.Get("nextToken") != clientToken {
		t.Fatalf("nextToken reached the api half as %q, want the client's cursor verbatim", forwarded.Get("nextToken"))
	}
	if got := onResp.Header.Get("x-next-token"); got != apiToken {
		t.Fatalf("x-next-token = %q, want the api half's own cursor: the gateway must not re-mint one it did not compute", got)
	}
}

// 🔴 One cursor shape, two rules about the last page — the difference is real,
// and it is pinned here rather than papered over.
//
// Shape: both sides render base64url(RFC3339 instant + "__" + sandbox id) and
// both parse either rendering, so a cursor minted on one side of the switch
// stays readable after a flip mid-pagination. Go's RFC3339Nano trims trailing
// zeros from the fraction and `src/api/impls/pagination.rs` always writes nine
// digits; both parsers accept both, which is what makes the flip survivable.
//
// Last page: the fan-out below mints a cursor whenever the page it returns is
// exactly `limit` long, so a full final page hands the client a cursor for a
// page that turns out to be empty. `PaginationCursor::paginate_sorted` in
// `src/api/impls/pagination.rs` mints one only when strictly more rows remain,
// so the api half ends the same listing one round trip earlier. Both are
// correct for a client that stops on an empty page; a client that stops on an
// absent cursor sees one extra request before the flip and none after it.
func TestTheClusterListCursorIsOneShapeWithTwoRulesForTheLastPage(t *testing.T) {
	limitOf := func(n int) *int { return &n }
	rows := []listedSandbox{
		mustListedSandbox("00000000-0000-0000-0000-000000000003", "2026-01-01T00:00:03Z", "running", "envd-a"),
		mustListedSandbox("00000000-0000-0000-0000-000000000002", "2026-01-01T00:00:02Z", "running", "envd-b"),
	}

	t.Run("the shape both sides render is the shape this side reads", func(t *testing.T) {
		_, minted, err := paginateListedSandboxes(rows, "", limitOf(1))
		if err != nil {
			t.Fatalf("paginate failed: %v", err)
		}
		decoded, err := base64.URLEncoding.DecodeString(minted)
		if err != nil {
			t.Fatalf("the fan-out's own cursor is not base64url: %v", err)
		}
		if string(decoded) != "2026-01-01T00:00:03Z__00000000-0000-0000-0000-000000000003" {
			t.Fatalf("cursor decoded to %q, want <instant>__<sandbox id>", string(decoded))
		}

		// The same position, rendered the way the api half renders it: nine
		// fractional digits rather than none.
		asTheApiHalfWritesIt := base64.URLEncoding.EncodeToString(
			[]byte("2026-01-01T00:00:03.000000000Z__00000000-0000-0000-0000-000000000003"))
		page, _, err := paginateListedSandboxes(rows, asTheApiHalfWritesIt, limitOf(10))
		if err != nil {
			t.Fatalf("the fan-out refused a cursor the api half would have minted: %v", err)
		}
		if got := sandboxIDs(page); !equalStrings(got, []string{"00000000-0000-0000-0000-000000000002"}) {
			t.Fatalf("that cursor positioned the listing at %v, want the row after the one it names", got)
		}

		// The non-empty half of "the shape is read": a cursor in no shape either
		// side mints is refused rather than silently treated as the beginning.
		if _, _, err := paginateListedSandboxes(rows, base64.URLEncoding.EncodeToString([]byte("not-a-cursor")), limitOf(10)); err == nil {
			t.Fatal("a cursor in an unknown shape was accepted; a listing that restarts from the top on a bad cursor is a listing that never ends")
		}
	})

	t.Run("a full final page still carries a cursor here and would not there", func(t *testing.T) {
		// The contrast face: the same non-empty cursor, once meaning "there is
		// more" and once meaning "there is not". That the fan-out cannot tell
		// them apart is the difference from the api half, stated as a run.
		page, more, err := paginateListedSandboxes(rows, "", limitOf(1))
		if err != nil {
			t.Fatalf("paginate failed: %v", err)
		}
		if len(page) != 1 || more == "" {
			t.Fatalf("page = %d rows, cursor = %q, want one row and a cursor", len(page), more)
		}
		following, _, err := paginateListedSandboxes(rows, more, limitOf(1))
		if err != nil {
			t.Fatalf("paginate failed: %v", err)
		}
		if len(following) != 1 {
			t.Fatalf("following that cursor returned %d rows, want the one that remains", len(following))
		}

		exact, alsoMore, err := paginateListedSandboxes(rows, "", limitOf(2))
		if err != nil {
			t.Fatalf("paginate failed: %v", err)
		}
		if len(exact) != 2 {
			t.Fatalf("page = %d rows, want both", len(exact))
		}
		if alsoMore == "" {
			t.Fatal("the fan-out stopped minting a cursor for a page it filled exactly; that would make it agree with the api half, and this test is what says they do not")
		}
		empty, _, err := paginateListedSandboxes(rows, alsoMore, limitOf(2))
		if err != nil {
			t.Fatalf("paginate failed: %v", err)
		}
		if len(empty) != 0 {
			t.Fatalf("following the final cursor returned %d rows, want none", len(empty))
		}
	})
}
