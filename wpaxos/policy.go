package wpaxos

import (
	"log"
	"time"

	"github.com/ailidani/paxi"
)

type Policy interface {
	hit(id paxi.ID) paxi.ID
}

func NewPolicy(name string) Policy {
	switch name {
	case "consecutive":
		p := new(consecutive)
		p.n = paxi.GetConfig().Threshold
		return p
	case "majority":
		p := new(majority)
		p.interval = paxi.GetConfig().Threshold
		p.hits = make(map[paxi.ID]int)
		p.time = time.Now()
		return p
	default:
		log.Fatal("unknown policy name ", name)
		return nil
	}
}

type consecutive struct {
	last paxi.ID
	hits int
	n    int
}

func (c *consecutive) hit(id paxi.ID) paxi.ID {
	if id == c.last {
		c.hits++
	} else {
		c.last = id
		c.hits = 1
	}
	if c.hits >= c.n {
		return c.last
	}
	return ""
}

// stat of access history in previous interval time
type majority struct {
	hits     map[paxi.ID]int
	interval int       // in milliseconds
	time     time.Time // last start time
	sum      int       // total hits in current interval
}

// hit record access id and return the
func (m *majority) hit(id paxi.ID) paxi.ID {
	m.hits[id]++
	m.sum++
	var res paxi.ID
	if m.sum > 1 && time.Since(m.time) >= time.Millisecond*time.Duration(m.interval) {
		for id, n := range m.hits {
			if n >= m.sum/2 {
				res = id
			}
		}
		// we reset for every interval
		m.reset()
	}
	return res
}

func (m *majority) reset() {
	for id := range m.hits {
		m.hits[id] = 0
	}
	m.sum = 0
	m.time = time.Now()
}
