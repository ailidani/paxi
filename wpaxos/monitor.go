package wpaxos

import (
	. "paxi"
	"time"
)

type monitor struct {
	n int

	history map[Key]*record

	t    time.Time
	hits map[Key]map[ID]int
}

type record struct {
	id ID
	n  int
}

func NewMonitor(n int) *monitor {
	return &monitor{
		n:       n,
		history: make(map[Key]*record),
		hits:    make(map[Key]map[ID]int),
		t:       time.Now(),
	}
}

func (m *monitor) h(key Key, id ID) bool {
	r, exists := m.history[key]
	if !exists {
		r = &record{id, 0}
		m.history[key] = r
	}

	if r.id == id {
		r.n++
	} else {
		r.id = id
		r.n = 1
	}

	if r.n >= m.n {
		return true
	}
	return false
}

func (m *monitor) hit(key Key, id ID) ID {
	h, exists := m.hits[key]
	if !exists {
		h = make(map[ID]int)
		h[id] = 0
		m.hits[key] = h
	}

	h[id]++

	if time.Since(m.t) >= time.Millisecond*time.Duration(m.n) {
		m.t = time.Now()
		sum := 0
		for _, y := range h {
			sum += y
		}

		for x, y := range h {
			if y >= sum/2 {
				delete(m.hits, key)
				return x
			}
		}
	}
	return 0
}
