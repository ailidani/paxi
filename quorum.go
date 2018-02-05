package paxi

import "github.com/ailidani/paxi/log"

type Quorum struct {
	size  int
	acks  map[ID]bool
	zones map[int]int
	nacks map[ID]bool
}

func NewQuorum() *Quorum {
	return &Quorum{
		size:  0,
		acks:  make(map[ID]bool, NumNodes),
		zones: make(map[int]int, NumZones),
		nacks: make(map[ID]bool, NumNodes),
	}
}

func (q *Quorum) ACK(id ID) {
	if !q.acks[id] {
		q.acks[id] = true
		q.size++
		q.zones[id.Zone()]++
	}
}

func (q *Quorum) NACK(id ID) {
	if !q.nacks[id] {
		q.nacks[id] = true
	}
}

func (q *Quorum) ADD() {
	q.size++
}

func (q *Quorum) Size() int {
	return q.size
}

func (q *Quorum) Reset() {
	q.size = 0
	q.acks = make(map[ID]bool, NumNodes)
	q.zones = make(map[int]int, NumZones)
	q.nacks = make(map[ID]bool, NumNodes)
}

func (q *Quorum) Majority() bool {
	return q.size > NumNodes/2
}

// this is not correct
func (q *Quorum) FastQuorum() bool {
	return q.size >= NumNodes-1
}

// this should be fast quorum (from fast paxos)
func (q *Quorum) FastPath() bool {
	return q.size >= NumNodes*3/4
}

func (q *Quorum) AllZones() bool {
	return len(q.zones) == NumZones
}

func (q *Quorum) ZoneMajority() bool {
	for _, n := range q.zones {
		if n > NumLocalNodes/2 {
			return true
		}
	}
	return false
}

func (q *Quorum) GridRow() bool {
	return q.AllZones()
}

func (q *Quorum) GridColumn() bool {
	for _, n := range q.zones {
		if n == NumLocalNodes {
			return true
		}
	}
	return false
}

func (q *Quorum) FGridQ1() bool {
	z := 0
	for _, n := range q.zones {
		if n > NumLocalNodes/2 {
			z++
		}
	}
	return z >= NumZones-F
}

func (q *Quorum) FGridQ2() bool {
	z := 0
	for _, n := range q.zones {
		if n > NumLocalNodes/2 {
			z++
		}
	}
	return z >= F+1
}

func (q *Quorum) Q1() bool {
	switch QuorumType {
	case "majority":
		return q.Majority()
	case "grid":
		return q.GridRow()
	case "fgrid":
		return q.FGridQ1()
	case "group":
		return q.ZoneMajority()
	case "count":
		return q.size >= NumNodes-F
	default:
		log.Errorln("Unknown quorum type")
		return false
	}
}

func (q *Quorum) Q2() bool {
	switch QuorumType {
	case "majority":
		return q.Majority()
	case "grid":
		return q.GridColumn()
	case "fgrid":
		return q.FGridQ2()
	case "group":
		return q.ZoneMajority()
	case "count":
		return q.size > F
	default:
		log.Errorln("Unknown quorum type")
		return false
	}
}
