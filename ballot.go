package paxi

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/ailidani/paxi/log"
)

// Ballot is ballot number type combines 32 bits of natual number and 32 bits of node id into uint64
type Ballot uint64

// NewBallot generates ballot number in format <n, zone, node>
func NewBallot(n int, id ID) Ballot {
	return Ballot(n<<32 | id.Zone()<<16 | id.Node())
}

func NewBallotFromString(b string) Ballot {
	if strings.Count(b, ".") < 2 {
		log.Warningf("ballot %s does not contain two \".\"\n", b)
		return 0
	}

	s := strings.Split(b, ".")
	n, err := strconv.ParseUint(s[0], 10, 64)
	if err != nil {
		log.Errorf("Failed to convert counter %s to uint64\n", s[0])
	}

	zone, err := strconv.ParseUint(s[1], 10, 64)
	if err != nil {
		log.Errorf("Failed to convert Zone %s to int\n", s[1])
	}

	node, err := strconv.ParseUint(s[2], 10, 64)
	if err != nil {
		log.Errorf("Failed to convert Node %s to int\n", s[2])
	}

	return NewBallot(int(n), NewID(int(zone), int(node)))
}

// N returns first 32 bit of ballot
func (b Ballot) N() int {
	return int(uint64(b) >> 32)
}

// ID return node id as last 32 bits of ballot
func (b Ballot) ID() ID {
	zone := int(uint32(b) >> 16)
	node := int(uint16(b))
	return NewID(zone, node)
}

// Next generates the next ballot number given node id
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
