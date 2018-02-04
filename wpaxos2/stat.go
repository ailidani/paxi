package wpaxos2

import (
	"time"

	"github.com/ailidani/paxi"
)

// stat of access history in previous interval time
type stat struct {
	hits     map[paxi.ID]int
	interval int       // in milliseconds
	time     time.Time // last start time
	sum      int       // total hits in current interval
}

func newStat(interval int) *stat {
	return &stat{
		interval: interval,
		hits:     make(map[paxi.ID]int),
		time:     time.Now(),
	}
}

// hit record access id and return the
func (s *stat) hit(id paxi.ID) paxi.ID {
	s.hits[id]++
	s.sum++
	var res paxi.ID
	if time.Since(s.time) >= time.Millisecond*time.Duration(s.interval) {
		for id, n := range s.hits {
			if n >= s.sum/2 {
				res = id
			}
		}
		// we reset for every interval
		s.reset()
	}
	return res
}

func (s *stat) reset() {
	s.hits = make(map[paxi.ID]int)
	s.sum = 0
	s.time = time.Now()
}
