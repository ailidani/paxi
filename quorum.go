package paxi

type Quorum struct {
	size  int
	acks  map[ID]bool
	sites map[uint8]int
	nacks map[ID]bool
}

func NewQuorum() *Quorum {
	return &Quorum{
		size:  0,
		acks:  make(map[ID]bool, NumNodes),
		sites: make(map[uint8]int, NumSites),
		nacks: make(map[ID]bool, NumNodes),
	}
}

func (q *Quorum) ACK(id ID) {
	if !q.acks[id] {
		q.acks[id] = true
		q.size++
		q.sites[id.Site()]++
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
	q.sites = make(map[uint8]int, NumSites)
	q.nacks = make(map[ID]bool, NumNodes)
}

func (q *Quorum) Majority() bool {
	return q.size > NumNodes/2
}

func (q *Quorum) Q1() bool {
	return len(q.sites) == NumSites && q.size >= Q1Size
}

func (q *Quorum) Q2() bool {
	for _, s := range q.sites {
		if s >= Q2Size {
			return true
		}
	}
	return false
}

func (q *Quorum) FastQuorum() bool {
	return q.size >= NumNodes-1
}

func (q *Quorum) FastPath() bool {
	return q.size >= NumNodes*3/4
}
