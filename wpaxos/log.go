package wpaxos

import (
	. "paxi"
	"paxi/glog"
	"time"
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

type log struct {
	committed int
	applied   int
	entries   []*entry

	icommitted index
	iapplied   index
	inext      index
	grid       [][]*entry
}

func NewLog() *log {
	return &log{
		committed:  0,
		applied:    0,
		entries:    make([]*entry, 0),
		icommitted: index{0, 0},
		iapplied:   index{0, 0},
		grid:       make([][]*entry, 0),
	}
}

func (l *log) index() int {
	return len(l.entries) - 1
}

func (l *log) ballot() int {
	return l.entries[l.index()].ballot
}

func (l *log) entry(i int) *entry {
	return l.entries[i]
}

func (l *log) create(b int, r Request) int {
	entry := &entry{
		ballot:    b,
		request:   r,
		quorum:    NewQuorum(),
		timestamp: time.Now(),
	}
	l.entries = append(l.entries, entry)
	return l.index()
}

func (l *log) append(i int, b int, cmd Command) {
	if i < l.committed {
		glog.Warningf("Already committed index %d\n", i)
		return
	}
	// TODO not done

}
