package routing

import "testing"

func TestNormalizeExecutionID(t *testing.T) {
	cases := map[string]string{
		"":                                       "",
		"   ":                                    "",
		" 0198B7CC-1111-7000-8000-000000000001 ": "0198b7cc-1111-7000-8000-000000000001",
		"0198b7cc-1111-7000-8000-000000000001":   "0198b7cc-1111-7000-8000-000000000001",
	}
	for in, want := range cases {
		if got := NormalizeExecutionID(in); got != want {
			t.Fatalf("NormalizeExecutionID(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestNormalizeExecutionIDPreservesMintOrder is the reason the lower-casing is
// not cosmetic: the arbitration compares these as strings, and in ASCII
// '0'-'9' < 'A'-'F' < 'a'-'f', so one upper-case id ordered against a
// lower-case one puts the older incarnation on top.
func TestNormalizeExecutionIDPreservesMintOrder(t *testing.T) {
	older := "0198b7cc-1111-7000-8000-000000000001"
	newerUpper := "0198B7CC-2222-7000-8000-000000000002"

	if newerUpper > older {
		t.Fatal("precondition failed: the raw upper-case id was expected to sort below the lower-case one")
	}
	if NormalizeExecutionID(newerUpper) <= older {
		t.Fatal("after normalisation the newer incarnation must sort above the older one")
	}
}

func TestAuthorityFor(t *testing.T) {
	if got := AuthorityFor(""); got != AuthorityUnknown {
		t.Fatalf("empty must be unknown, got %v", got)
	}
	if got := AuthorityFor("   "); got != AuthorityUnknown {
		t.Fatalf("blank must be unknown, got %v", got)
	}
	if got := AuthorityFor("0198b7cc-1111-7000-8000-000000000001"); got != AuthorityRegistry {
		t.Fatalf("a named incarnation must be registry, got %v", got)
	}
}
