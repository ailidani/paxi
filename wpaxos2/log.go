package wpaxos2

import (
	"time"

	. "github.com/ailidani/paxi"
)

// TODO same as instance in wpaxos/paxos
type entry struct {
	ballot    int
	cmds      []Command
	committed bool
	request   *Request
	quorum    *Quorum
	timestamp time.Time
}

type index struct {
	i, j int
}

type log struct {
	grid [][]*entry
	next index
}

func NewLog() *log {
	log := new(log)
	log.grid = make([][]*entry, 0)
	log.next = index{0, 0}
	return log
}
