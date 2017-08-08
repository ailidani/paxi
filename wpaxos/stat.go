package wpaxos

import (
	. "paxi"
	"time"
)

type stat struct {
	n    int
	hits map[ID]int
	t    time.Time
}

func NewStat(n int) *stat {
	return &stat{
		n:    n,
		hits: make(map[ID]int),
		t:    time.Now(),
	}
}

func (s *stat) hit(id ID) ID {
	s.hits[id]++
	if time.Since(s.t) >= time.Millisecond*time.Duration(s.n) {
		s.t = time.Now()
		sum := 0
		for _, y := range s.hits {
			sum += y
		}
		for x, y := range s.hits {
			if y >= sum/2 {
				s.hits = make(map[ID]int)
				return x
			}
		}
	}
	return 0
}
