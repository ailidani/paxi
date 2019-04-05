package epaxos

import "github.com/ailidani/paxi"

type status int8

const (
	NONE status = iota
	PREACCEPTED
	ACCEPTED
	COMMITTED
	EXECUTED
)

type instance struct {
	cmd    paxi.Command
	ballot paxi.Ballot
	status status
	seq    int
	dep    map[paxi.ID]int

	// leader bookkeeping
	request *paxi.Request
	quorum  *paxi.Quorum
	changed bool // seq and dep changed
}

// merge the seq and dep for instance
func (i *instance) merge(seq int, dep map[paxi.ID]int) {
	if seq > i.seq {
		i.seq = seq
		i.changed = true
	}
	for id, d := range dep {
		if d > i.dep[id] {
			i.dep[id] = d
			i.changed = true
		}
	}
}

// copyDep clones dependency list of instance
func (i *instance) copyDep() (dep map[paxi.ID]int) {
	dep = make(map[paxi.ID]int)
	for id, d := range i.dep {
		dep[id] = d
	}
	return dep
}
