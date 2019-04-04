package paxi

import (
	"math"
	"time"

	"github.com/ailidani/paxi/log"
)

// Policy defines a trigger for data access patterns, that can be used in data migration protocols
type Policy interface {
	Hit(id ID) ID
}

// NewPolicy returns the policy by policy name from config
func NewPolicy() Policy {
	switch config.Policy {
	case "":
		fallthrough

	case "null":
		p := new(null)
		return p

	case "consecutive":
		p := new(consecutive)
		p.n = int(config.Threshold)
		return p

	case "majority":
		p := new(majority)
		p.interval = int(config.Threshold)
		p.hits = make(map[ID]int)
		p.time = time.Now()
		return p

	case "ema":
		p := new(ema)
		p.alpha = config.Threshold
		p.epsilon = 0.1
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
	var res ID
	if id == c.last {
		c.hits++
	} else {
		c.last = id
		c.hits = 1
	}
	if c.hits >= c.n {
		res = c.last
		c.last = ""
		c.hits = 0
	}
	return res
}

// stat of access history in previous interval time
type majority struct {
	hits     map[ID]int
	interval int       // in second
	time     time.Time // last start time
	sum      int       // total hits in current interval
}

// hit record access id and return the
func (m *majority) Hit(id ID) ID {
	m.hits[id]++
	m.sum++
	var res ID
	if m.sum > 1 && time.Since(m.time) >= time.Second*time.Duration(m.interval) {
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

// ema is exponential moving average policy
type ema struct {
	alpha   float64
	s       float64
	epsilon float64
	zone    int
}

func (e *ema) Hit(id ID) ID {
	if e.s == 0 {
		e.s = float64(id.Zone())
		return ""
	}

	e.s = e.alpha*float64(id.Zone()) + (1-e.alpha)*e.s
	if math.Abs(e.s-math.Round(e.s)) > e.epsilon {
		// no signifigant changes
		return ""
	}
	z := int(math.Round(e.s))
	if z != e.zone {
		// new zone value
		e.zone = z
		return NewID(e.zone, 1)
	}
	return ""
}

type null struct{}

func (n *null) Hit(id ID) ID {
	return ""
}
