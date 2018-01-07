package paxi

import "fmt"

type Ballot uint64

// NewBallot generates ballot number in format <n, zone, node>
func NewBallot(n int, id ID) Ballot {
	return Ballot(n<<32 | id.Zone()<<16 | id.Node())
}

func (b Ballot) N() int {
	return int(uint64(b) >> 32)
}

func (b Ballot) ID() ID {
	zone := int(uint32(b) >> 16)
	node := int(uint16(b))
	return NewID(zone, node)
}

func (b *Ballot) Next(id ID) {
	*b = NewBallot(b.N()+1, id)
}

func (b Ballot) String() string {
	return fmt.Sprintf("%d.%s", b.N(), b.ID())
}

// NextBallot generates next ballot number given current ballot bumber and node id
func NextBallot(ballot int, id ID) int {
	n := id.Zone()<<16 | id.Node()
	return (ballot>>32+1)<<32 | n
}

// LeaderID return the node id from ballot number
func LeaderID(ballot int) ID {
	zone := uint32(ballot) >> 16
	node := uint16(ballot)
	return NewID(int(zone), int(node))
}
