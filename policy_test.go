package paxi

import (
	"math/rand"
	"testing"
)

func TestPolicy(t *testing.T) {
	config.Policy = "ema"
	config.Threshold = 0.1
	p := NewPolicy()

	for i := 0; i < 1000; i++ {
		zone := rand.Intn(3) + 1
		id := p.Hit(NewID(zone, 1))
		if id != "" {
			t.Logf("round %d hit %v", i, id)
		}
	}
}
