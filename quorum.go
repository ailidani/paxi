package paxi

type Quorum struct {
	size  int
	acks  map[ID]bool
	zones map[uint8]int
	nacks map[ID]bool
}

func NewQuorum() *Quorum {
	return &Quorum{
		size:  0,
		acks:  make(map[ID]bool, NumNodes),
		zones: make(map[uint8]int, NumZones),
		nacks: make(map[ID]bool, NumNodes),
	}
}

func (q *Quorum) ACK(id ID) {
	if !q.acks[id] {
		q.acks[id] = true
		q.size++
		q.zones[id.Site()]++
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

func (q *Quorum) Clear() {
	q.size = 0
	q.acks = make(map[ID]bool, NumNodes)
	q.zones = make(map[uint8]int, NumZones)
	q.nacks = make(map[ID]bool, NumNodes)
}

func (q *Quorum) Majority() bool {
	return q.size > NumNodes/2
}

func (q *Quorum) SiteMajority() bool {
	for _, s := range q.zones {
		if s >= NumLocalNodes/2 {
			return true
		}
	}
	return false
}

func (q *Quorum) AllSites() bool {
	return len(q.zones) == NumZones
}

func (q *Quorum) Q1() bool {
	z := 0
	for _, n := range q.zones {
		if n >= NumLocalNodes/2+1 {
			z++
		}
	}
	return z >= NumZones-F
}

func (q *Quorum) Q2() bool {
	z := 0
	for _, n := range q.zones {
		if n >= NumLocalNodes/2+1 {
			z++
		}
	}
	return z >= F+1
}

func (q *Quorum) FastQuorum() bool {
	return q.size >= NumNodes-1
}

func (q *Quorum) FastPath() bool {
	return q.size >= NumNodes*3/4
}
