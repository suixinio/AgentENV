package catalog

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

// The catalog's four statuses, and the four groups the trigger folds them into.
//
// 🔴 Named here so that Go can *check* a value before sending it, never so that
// Go can decide what one means. The table's CHECK constraint is the authority
// and it belongs to whoever migrates the database; a status this build has not
// heard of coming back out of a column reaches the caller intact rather than
// being flattened into something plausible.
const (
	StatusWaiting  = "waiting"
	StatusBuilding = "building"
	StatusReady    = "ready"
	StatusError    = "error"
)

const (
	StatusGroupPending    = "pending"
	StatusGroupInProgress = "in_progress"
	StatusGroupReady      = "ready"
	StatusGroupFailed     = "failed"
)

// The two source kinds. A template and a snapshot are one entity here,
// distinguished by this discriminant, rather than the two tables e2b keeps.
const (
	SourceKindTemplate = "template"
	SourceKindSandbox  = "sandbox"
)

func knownStatus(raw string) bool {
	switch raw {
	case StatusWaiting, StatusBuilding, StatusReady, StatusError:
		return true
	default:
		return false
	}
}

func knownSourceKind(raw string) bool {
	return raw == SourceKindTemplate || raw == SourceKindSandbox
}

// statusGroupFor is the mapping the trigger applies, restated in Go for the one
// job Go has: predicting what a row will carry so a caller can be handed the
// row it just wrote without a second read.
//
// 🔴 Not a second source of truth. Nothing decides anything on the strength of
// this — every predicate that filters on status_group reads the column. A test
// asserts the two agree, which is the only thing keeping this honest.
func statusGroupFor(status string) string {
	switch status {
	case StatusWaiting:
		return StatusGroupPending
	case StatusBuilding:
		return StatusGroupInProgress
	case StatusReady:
		return StatusGroupReady
	default:
		return StatusGroupFailed
	}
}

// requireUUID rejects anything that is not a canonical hyphenated uuid, and
// lower-cases what it accepts.
//
// The check is here rather than left to PostgreSQL's cast so that a malformed
// id fails naming itself instead of taking a whole statement down with a cast
// error that names nothing. It is stricter than PostgreSQL's own parser, which
// also takes braced and unhyphenated forms: the node's ids come from a Uuid
// whose Display is always canonical, so anything else arrived from somewhere
// this build does not recognise.
//
// 🔴 The lower-casing is not cosmetic. Page boundaries are decided by comparing
// ids as text, and in ASCII '0'-'9' < 'A'-'F' < 'a'-'f' — so one upper-case id
// in a cursor sorts on the far side of every real id and silently skips a page.
func requireUUID(field, raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", fmt.Errorf("%w: %s is required", ErrInvalidArgument, field)
	}
	if !isCanonicalUUID(trimmed) {
		return "", fmt.Errorf("%w: %s %q is not a uuid", ErrInvalidArgument, field, raw)
	}
	return strings.ToLower(trimmed), nil
}

func isCanonicalUUID(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i := 0; i < 36; i++ {
		c := s[i]
		switch i {
		case 8, 13, 18, 23:
			if c != '-' {
				return false
			}
		default:
			isHex := (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
			if !isHex {
				return false
			}
		}
	}
	return true
}

// requireJSONObject checks the shape of a blob this process stores and never
// reads.
//
// 🔴 "Valid JSON" is not enough, and `null` is why: it is a legal document,
// JSONB stores it, and the row afterwards fails to decode on the node — where
// the decoder is shared by every read, so one poisoned row stalls that machine
// entirely. Arrays and scalars are refused for the same reason.
//
// What it does not do is look inside. Which fields belong there is the node's
// business, and a Go type asserting an opinion would drop every field this
// build has not heard of, silently.
func requireJSONObject(field string, raw json.RawMessage) error {
	if len(bytes.TrimSpace(raw)) == 0 {
		return fmt.Errorf("%w: %s is required", ErrInvalidArgument, field)
	}
	var probe map[string]json.RawMessage
	if err := json.Unmarshal(raw, &probe); err != nil {
		return fmt.Errorf("%w: %s is not JSON: %v", ErrInvalidArgument, field, err)
	}
	if probe == nil {
		return fmt.Errorf("%w: %s is JSON null, which stores and then fails to decode", ErrInvalidArgument, field)
	}
	return nil
}

// nilIfEmpty turns an absent string into a SQL NULL, so a COALESCE in a
// statement can tell "leave this alone" from "set it to nothing".
func nilIfEmpty(s string) *string {
	trimmed := strings.TrimSpace(s)
	if trimmed == "" {
		return nil
	}
	return &trimmed
}
