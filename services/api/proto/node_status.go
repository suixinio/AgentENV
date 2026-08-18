package schedulerv1

// This file is hand-written. It lives next to the generated code because a
// method can only be attached to NodeStatus from the package that declares it.
// `make proto` only rewrites the .pb.go files, so this is not clobbered by
// regeneration.

// CanAcceptNewRequests reports whether a node in this status may be given new
// work — a fresh sandbox, a fork, or a resume that would start a VM there.
//
// This answers "may I send this node something new", not "is this node still
// working". A draining node keeps serving the sandboxes it already holds, so
// paths that act on existing sandboxes — routing, pause, kill, keep-alive —
// must not gate on this.
//
// UNSPECIFIED deliberately answers false. It is the zero value, so callers that
// have no snapshot at all must decide for themselves whether a node that has
// never reported is a candidate; they must not get an accidental "yes" from a
// missing field.
func (x NodeStatus) CanAcceptNewRequests() bool {
	switch x {
	case NodeStatus_NODE_STATUS_READY:
		return true
	default:
		return false
	}
}
