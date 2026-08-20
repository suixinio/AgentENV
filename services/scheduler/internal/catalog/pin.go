package catalog

// PinOriginIfUnpublished decides where a snapshot may be started.
//
// 🔴 This is the only place `published` and `origin_node_id` decide anything,
// and keeping it that way is what makes those two columns removable. The
// resolving queries project them and never filter on them; the day a publish
// can no longer fail permanently, the removal is one migration file, two fields
// moved to `reserved` in the proto, and this function together with its callers
// — no WHERE clause anywhere has to be re-read.
//
// The two axes it stands on are not the same axis:
//
//   - `status_group = 'ready'` asks whether the capture finished — whether
//     there is a snapshot that can run at all. A publish that failed answers
//     yes: the bytes are complete and sitting on the node that made them.
//   - `published` asks whether those bytes reached shared storage — whether
//     anybody else can start it. The same snapshot answers no.
//
// So a failed publish is a `ready` snapshot with published=false, not a
// snapshot stuck at `building`. Pressing it into the status axis would tell the
// user their snapshot does not exist, while its origin node can start it
// perfectly well — which is exactly what the paused registry already does when
// it hands a resume back to the node that holds the bytes.
//
// The two answers:
//
//   - published=false: a hard pin. Only the named node may run it, and the pin
//     travels back to the caller so the refusal can say where the snapshot is
//     rather than merely that it is elsewhere.
//   - published=true: allowed anywhere. The name that comes back is a placement
//     *hint* — it may be empty, and it may be wrong. A hint that misses is not
//     an error: the caller places the sandbox wherever it likes and must not
//     send anything back to the node named here on the strength of it.
func PinOriginIfUnpublished(row SnapshotRow, target string) (allowed bool, pin string) {
	if row.Published {
		return true, row.OriginNodeID
	}
	return row.OriginNodeID == target, row.OriginNodeID
}

// PinAliasTarget is PinOriginIfUnpublished for an alias resolution, which
// carries the same two projected values and no more.
//
// Written as a second entry point rather than as a second copy of the rule: it
// forwards, so there is still exactly one place where the decision is made and
// exactly one place to delete.
func PinAliasTarget(target AliasTarget, node string) (allowed bool, pin string) {
	return PinOriginIfUnpublished(SnapshotRow{
		Published:    target.Published,
		OriginNodeID: target.OriginNodeID,
	}, node)
}
