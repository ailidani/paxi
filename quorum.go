package paxi

import "github.com/ailidani/paxi/log"

// Quorum records each acknowledgement and check for different types of quorum satisfied
type Quorum struct {
	size  int
	acks  map[ID]bool
	zones map[int]int
	nacks map[ID]bool
}

// NewQuorum returns a new Quorum
func NewQuorum() *Quorum {
	return &Quorum{
		size:  0,
		acks:  make(map[ID]bool, Config.NumNodes()),
		zones: make(map[int]int, Config.NumZones()),
		nacks: make(map[ID]bool, Config.NumNodes()),
	}
}

// ACK adds id to quorum ack records
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

// Size returns current ack size
func (q *Quorum) Size() int {
	return q.size
}

// Reset resets the quorum to empty
func (q *Quorum) Reset() {
	q.size = 0
	q.acks = make(map[ID]bool, Config.NumNodes())
	q.zones = make(map[int]int, Config.NumZones())
	q.nacks = make(map[ID]bool, Config.NumNodes())
}

func (q *Quorum) Majority() bool {
	return q.size > Config.NumNodes()/2
}

// this is not correct
func (q *Quorum) FastQuorum() bool {
	return q.size >= Config.NumNodes()-1
}

// this should be fast quorum (from fast paxos)
func (q *Quorum) FastPath() bool {
	return q.size >= Config.NumNodes()*3/4
}

// AllZones returns true if there is at one ack from each zone
func (q *Quorum) AllZones() bool {
	return len(q.zones) == Config.NumZones()
}

func (q *Quorum) ZoneMajority() bool {
	for _, n := range q.zones {
		if n > Config.NumNodes()/Config.NumZones()/2 {
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
		if n == Config.NumNodes()/Config.NumZones() {
			return true
		}
	}
	return false
}

func (q *Quorum) FGridQ1() bool {
	z := 0
	for _, n := range q.zones {
		if n > Config.NumNodes()/Config.NumZones()/2 {
			z++
		}
	}
	return z >= Config.NumZones()-Config.F
}

func (q *Quorum) FGridQ2() bool {
	z := 0
	for _, n := range q.zones {
		if n > Config.NumNodes()/Config.NumZones()/2 {
			z++
		}
	}
	return z >= Config.F+1
}

// Q1 returns true if Config.Quorum type is satisfied
func (q *Quorum) Q1() bool {
	switch Config.Quorum {
	case "majority":
		return q.Majority()
	case "grid":
		return q.GridRow()
	case "fgrid":
		return q.FGridQ1()
	case "group":
		return q.ZoneMajority()
	case "count":
		return q.size >= Config.NumNodes()-Config.F
	default:
		log.Error("Unknown quorum type")
		return false
	}
}

// Q2 returns true if Config.Quorum type is satisfied
func (q *Quorum) Q2() bool {
	switch Config.Quorum {
	case "majority":
		return q.Majority()
	case "grid":
		return q.GridColumn()
	case "fgrid":
		return q.FGridQ2()
	case "group":
		return q.ZoneMajority()
	case "count":
		return q.size > Config.F
	default:
		log.Error("Unknown quorum type")
		return false
	}
}
