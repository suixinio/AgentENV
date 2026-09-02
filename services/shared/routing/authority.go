package routing

import "strings"

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
// 🔴 It lives here, and only here: the projection reader and the wake-up
// adapter both derive an authority from the same field, and two copies of a
// three-line rule drift silently — the drift shows up as the gateway refusing
// an exchange the api half would have allowed.
func AuthorityFor(executionID string) Authority {
	if strings.TrimSpace(executionID) == "" {
		return AuthorityUnknown
	}
	return AuthorityRegistry
}
