package routing

// Location is where an Answer places the sandbox relative to its node.
//
// Every answer this module produces is Bound. The other values keep the
// gateway's sandbox-location series at the label set it has always carried.
type Location int

const (
	LocationUnspecified Location = iota
	LocationBound
	LocationPlaced
	LocationPinned
)

func (l Location) String() string {
	switch l {
	case LocationBound:
		return "bound"
	case LocationPlaced:
		return "placed"
	case LocationPinned:
		return "pinned"
	default:
		return "unspecified"
	}
}

// Authority says who vouches for the incarnation an Answer names.
type Authority int

const (
	// AuthorityUnknown means nobody could name the incarnation. The zero
	// value, so an Answer nobody filled in claims nothing.
	AuthorityUnknown Authority = iota
	// AuthorityRegistry means the incarnation is the one recorded for a
	// running sandbox, and the node is expected to be running exactly it.
	AuthorityRegistry
	// AuthorityPending means the node is about to mint a new incarnation, so
	// any value carried names the previous one.
	AuthorityPending
)

func (a Authority) String() string {
	switch a {
	case AuthorityRegistry:
		return "registry"
	case AuthorityPending:
		return "pending"
	default:
		return "unknown"
	}
}

// Answer is one resolved sandbox route: the node to forward to and the
// incarnation it is expected to be running.
type Answer struct {
	Node        Node
	Location    Location
	ExecutionID string
	Authority   Authority
}
