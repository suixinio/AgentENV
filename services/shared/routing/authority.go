package routing

import (
	"strings"

	schedulerv1 "agentenv/services/api/proto"
)

// NormalizeExecutionID puts an incarnation into the one shape the ordering is
// defined over.
//
// 🔴 Lower case is not cosmetic. The whole comparison is lexicographic — a
// UUIDv7 sorts in the order it was minted — and '0'-'9' < 'A'-'F' < 'a'-'f', so
// one upper-case value compared against a lower-case one orders backwards.
func NormalizeExecutionID(raw string) string {
	return strings.ToLower(strings.TrimSpace(raw))
}

// AuthorityFor is the one place an incarnation becomes an authority.
//
// 🔴 REGISTRY is never reported with an empty value. "Authoritatively, no
// incarnation" is a sentence the caller would have to compare an empty string
// against; when this side cannot name one, the honest answer is that it does
// not know.
//
// 🔴 It lives here, and only here. It used to live beside the scheduler's
// lookup, which is the only place that needed it — until the gateway started
// reading the projection directly and had to derive the same authority from the
// same field. Two copies of a three-line rule drift silently, and the drift
// shows up as a gateway refusing an exchange the scheduler would have allowed.
func AuthorityFor(executionID string) schedulerv1.ExecutionAuthority {
	if strings.TrimSpace(executionID) == "" {
		return schedulerv1.ExecutionAuthority_EXECUTION_AUTHORITY_UNKNOWN
	}
	return schedulerv1.ExecutionAuthority_EXECUTION_AUTHORITY_REGISTRY
}
