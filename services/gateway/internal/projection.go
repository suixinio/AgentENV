package gateway

import (
	"context"

	schedulerv1 "agentenv/services/api/proto"
	"agentenv/services/shared/routing"

	"go.uber.org/zap"
)

// projectionReader is the read half of the routing projection, narrowed to the
// one call this package makes. routing.Reader satisfies it; so does a fake.
type projectionReader interface {
	Get(ctx context.Context, sandboxID string) (routing.Record, bool, error)
}

// resolveFromProjection answers a sandbox route out of the projection, or says
// it could not.
//
// 🔴 Three outcomes, and only one of them is an answer. A miss is not an
// absence and an error is not a failure: both mean "ask the api half", which
// is the resume RPC the caller makes next.
//
// 🔴 In particular this never produces a status code. 404 and 503 have exactly
// one source in this package — writeResumeError, on the api half's own
// answer — and that has to stay true. A gateway that answered 404 from a miss
// would be turning "I did not find a cached record" into "this sandbox does not
// exist", which for a resume is the end of that sandbox as far as any client is
// concerned. It would also skip everything the api half walks before it says
// gone — the binding, the heartbeat roster, the paused registry — which is
// what covers the window a heartbeat is late for and the window another node's
// reconciliation dropped a binding this node still lists.
func (s *Server) resolveFromProjection(ctx context.Context, sandboxID string) *schedulerv1.LookupNodeResponse {
	if s.projectionReader == nil {
		// The read switch is off. Nothing is counted here: the caller counts
		// the answer it is about to get from the api half, and counting a
		// read that was never attempted would put a decision in the series
		// nobody made.
		return nil
	}

	record, ok, err := s.projectionReader.Get(ctx, sandboxID)
	switch {
	case err != nil:
		// 🔴 Redis must not become a second thing that can kill the data
		// plane. The whole point of this stage is one fewer dependency on the
		// request path, and a read failure that refused the request would be
		// the opposite.
		recordRouteResolution(routeResolutionRedisError)
		s.logger.Warn("routing projection read failed, asking the api half",
			zap.String("sandbox_id", sandboxID),
			zap.Error(err),
		)
		return nil
	case ok:
		// Counted by the caller, as routeResolutionRedisHit: a hit is the
		// answer, so it belongs in the same place the api half's answer is
		// counted rather than half a level down.
		return routing.Synthesize(record)
	default:
		recordRouteResolution(routeResolutionRedisMiss)
		return nil
	}
}
