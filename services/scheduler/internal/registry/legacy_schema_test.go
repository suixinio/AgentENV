package registry

import (
	"fmt"
	"sync/atomic"
)

// legacyNodeSchemaDDL is the shape of `paused_sandboxes` as the AgentENV nodes
// built it, frozen as it stood before D11 removed the node-side `postgres`
// backend.
//
// 🔴 It is a fossil, and it never follows SchemaDDL. Not following is the whole
// of what it is for: every dev and test cluster that has ever run the old
// backend has a table of exactly this shape, and the tests below use it to ask
// two questions this build has to keep answering — that migrating an *empty*
// table of this shape works, and that migrating a *populated* one is refused
// with something an operator can act on.
//
// It used to exist twice, once in contract_test.go and once in
// postgres_integration_test.go, both with a comment saying they were copied
// verbatim from a Rust file that no longer exists. One copy, named for what it
// actually is.
const legacyNodeSchemaDDL = `
CREATE TABLE IF NOT EXISTS paused_sandboxes (
    sandbox_id     UUID        PRIMARY KEY,
    cluster_id     UUID        NOT NULL,
    state          TEXT        NOT NULL,
    generation     BIGINT      NOT NULL,
    origin_node_id TEXT        NOT NULL,
    snapshot_id    UUID,
    metadata       JSONB       NOT NULL,
    paused_at      TIMESTAMPTZ NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL
);
ALTER TABLE paused_sandboxes ADD COLUMN IF NOT EXISTS claimed_by_node_id TEXT;
ALTER TABLE paused_sandboxes DROP CONSTRAINT IF EXISTS paused_sandboxes_state_check;
ALTER TABLE paused_sandboxes ADD CONSTRAINT paused_sandboxes_state_check
    CHECK (state IN ('publishing', 'paused', 'resuming', 'local_only', 'running'));
ALTER TABLE paused_sandboxes ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMPTZ;
ALTER TABLE paused_sandboxes ADD COLUMN IF NOT EXISTS sandbox_expires_at TIMESTAMPTZ;
CREATE INDEX IF NOT EXISTS paused_sandboxes_origin_node_idx ON paused_sandboxes (origin_node_id);
CREATE INDEX IF NOT EXISTS paused_sandboxes_updated_at_idx ON paused_sandboxes (updated_at);
`

// testExecutionCounter backs newExecutionID.
var testExecutionCounter atomic.Uint64

// newExecutionID mints an incarnation id shaped like the ones the nodes send:
// canonical, lower case, and — the part the routing half depends on —
// increasing in the same direction as time, the way a v7 uuid is.
//
// 🔴 The ordering is not decoration. The arbitration downstream picks the
// larger of two ids as the newer one, so a test that minted unordered ids would
// exercise the comparison with the inputs it is not supposed to see and
// would pass or fail by luck.
func newExecutionID() string {
	n := testExecutionCounter.Add(1)
	return fmt.Sprintf("00000001-0000-7000-8000-%012x", n)
}
