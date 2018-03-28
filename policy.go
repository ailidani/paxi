package paxi

import (
	"time"

	"github.com/ailidani/paxi/log"
)

type Policy interface {
	Hit(id ID) ID
}

func NewPolicy() Policy {
	switch config.Policy {
	case "consecutive":
		p := new(consecutive)
		p.n = config.Threshold
		return p
	case "majority":
		p := new(majority)
		p.interval = config.Threshold
		p.hits = make(map[ID]int)
		p.time = time.Now()
		return p
	default:
		log.Fatal("unknown policy name ", config.Policy)
		return nil
	}
}

type consecutive struct {
	last ID
	hits int
	n    int
}

func (c *consecutive) Hit(id ID) ID {
	if id == c.last {
		c.hits++
	} else {
		c.last = id
		c.hits = 1
	}
	if c.hits >= c.n {
		c.hits = 0
		return c.last
	}
	return ""
}

// stat of access history in previous interval time
type majority struct {
	hits     map[ID]int
	interval int       // in milliseconds
	time     time.Time // last start time
	sum      int       // total hits in current interval
}

// hit record access id and return the
func (m *majority) Hit(id ID) ID {
	m.hits[id]++
	m.sum++
	var res ID
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
