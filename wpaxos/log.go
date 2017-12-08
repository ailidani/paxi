package wpaxos

import (
	"time"

	. "github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
)

type entry struct {
	ballot    int
	cmds      []Command
	request   Request
	quorum    *Quorum
	timestamp time.Time
}

type index struct {
	i, j int
}

type clog struct {
	committed int
	applied   int
	entries   []*entry

	icommitted index
	iapplied   index
	inext      index
	grid       [][]*entry
}

func NewLog() *clog {
	return &clog{
		committed:  0,
		applied:    0,
		entries:    make([]*entry, 0),
		icommitted: index{0, 0},
		iapplied:   index{0, 0},
		grid:       make([][]*entry, 0),
	}
}

func (c *clog) index() int {
	return len(c.entries) - 1
}

func (c *clog) ballot() int {
	return c.entries[c.index()].ballot
}

func (c *clog) entry(i int) *entry {
	return c.entries[i]
}

func (c *clog) create(b int, r Request) int {
	entry := &entry{
		ballot:    b,
		request:   r,
		quorum:    NewQuorum(),
		timestamp: time.Now(),
	}
	c.entries = append(c.entries, entry)
	return c.index()
}

func (c *clog) append(i int, b int, cmd Command) {
	if i < c.committed {
		log.Warningf("Already committed index %d\n", i)
		return
	}
	// TODO not done

}
