package paxi

// Quorum records each acknowledgement and check for different types of quorum satisfied
type Quorum struct {
	size  int
	acks  map[ID]bool
	zones map[int]int
	nacks map[ID]bool
	idColMap map[ID]int
	valueMap map[ID]int
}

// NewQuorum returns a new Quorum
func NewQuorum() *Quorum {
	q := &Quorum{
		size:  0,
		acks:  make(map[ID]bool),
		zones: make(map[int]int),
		idColMap: make(map[ID]int),
	}
	return q
}

func (q *Quorum) SampleACK(id ID, col int) {
	if !q.acks[id] {
		q.acks[id] = true
		q.size++
		q.zones[id.Zone()]++
		q.idColMap[id] = col
	}

}

func(q *Quorum) BenORAck(id ID, value int) {
	if !q.acks[id] {
		q.acks[id] = true
		q.size++
		q.zones[id.Zone()]++
		q.valueMap[id] = value
	}
}

func (q *Quorum) SampleMajority(sampleID int) bool {
	if q.zones[sampleID] > 2 {
		return true
	}
	return false
}

func(q *Quorum) BenORMajority() int {
	zeroCnt := 0
	oneCnt := 0
	for _, value := range q.idColMap{
		if value == 1{
			oneCnt++
		} else if value == 0{
			zeroCnt++
		}
	}
	if zeroCnt >= oneCnt{
		return 0
	}
	return 1
}

func (q *Quorum) SampleMajorityColor(sampleID int) int {
	redCnt := 0
	blueCnt := 0
	for id, col := range q.idColMap {
		if id.Zone() == sampleID {
			if col == 0 {
				redCnt++
			} else if  col == 1 {
				blueCnt++
			}
		}
	}
	if redCnt >= blueCnt {
		return 0
	}
	return 1
}

// ACK adds id to quorum ack records
func (q *Quorum) ACK(id ID) {
	if !q.acks[id] {
		q.acks[id] = true
		q.size++
		q.zones[id.Zone()]++
	}
}

// NACK adds id to quorum nack records
func (q *Quorum) NACK(id ID) {
	if !q.nacks[id] {
		q.nacks[id] = true
	}
}

// ADD increase ack size by one
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
	q.acks = make(map[ID]bool)
	q.zones = make(map[int]int)
	q.nacks = make(map[ID]bool)
	q.idColMap = make(map[ID]int)
}

func (q *Quorum) All() bool {
	return q.size == config.n
}

// Majority quorum satisfied
func (q *Quorum) Majority() bool {
	return q.size > config.n/2
}

// FastQuorum from fast paxos
func (q *Quorum) FastQuorum() bool {
	return q.size >= config.n*3/4
}

// AllZones returns true if there is at one ack from each zone
func (q *Quorum) AllZones() bool {
	return len(q.zones) == config.z
}

// ZoneMajority returns true if majority quorum satisfied in any zone
func (q *Quorum) ZoneMajority() bool {
	for z, n := range q.zones {
		if n > config.npz[z]/2 {
			return true
		}
	}
	return false
}

// GridRow == AllZones
func (q *Quorum) GridRow() bool {
	return q.AllZones()
}

// GridColumn == all nodes in one zone
func (q *Quorum) GridColumn() bool {
	for z, n := range q.zones {
		if n == config.npz[z] {
			return true
		}
	}
	return false
}

// FGridQ1 is flexible grid quorum for phase 1
func (q *Quorum) FGridQ1(Fz int) bool {
	zone := 0
	for z, n := range q.zones {
		if n > config.npz[z]/2 {
			zone++
		}
	}
	return zone >= config.z-Fz
}

// FGridQ2 is flexible grid quorum for phase 2
func (q *Quorum) FGridQ2(Fz int) bool {
	zone := 0
	for z, n := range q.zones {
		if n > config.npz[z]/2 {
			zone++
		}
	}
	return zone >= Fz+1
}

/*
// Q1 returns true if config.Quorum type is satisfied
func (q *Quorum) Q1() bool {
	switch config.Quorum {
	case "majority":
		return q.Majority()
	case "grid":
		return q.GridRow()
	case "fgrid":
		return q.FGridQ1()
	case "group":
		return q.ZoneMajority()
	case "count":
		return q.size >= config.n-config.F
	default:
		log.Error("Unknown quorum type")
		return false
	}
}

// Q2 returns true if config.Quorum type is satisfied
func (q *Quorum) Q2() bool {
	switch config.Quorum {
	case "majority":
		return q.Majority()
	case "grid":
		return q.GridColumn()
	case "fgrid":
		return q.FGridQ2()
	case "group":
		return q.ZoneMajority()
	case "count":
		return q.size > config.F
	default:
		log.Error("Unknown quorum type")
		return false
	}
}
*/
