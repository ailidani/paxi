package wpaxos

import (
	"time"

	"github.com/ailidani/paxi"
)

// TODO same as instance in wpaxos/paxos
type entry struct {
	ballot    int
	cmds      []paxi.Command
	committed bool
	request   *paxi.Request
	quorum    *paxi.Quorum
	timestamp time.Time
}

type index struct {
	i, j int
}

type clog struct {
	grid [][]*entry
	next index
}

func NewLog() *clog {
	clog := new(clog)
	clog.grid = make([][]*entry, 0)
	clog.next = index{0, 0}
	return clog
}
