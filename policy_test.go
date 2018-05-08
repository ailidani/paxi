package paxi

import (
	"math/rand"
	"testing"
)

func uniformTest(p Policy, t *testing.T) {
	for i := 0; i < 10000; i++ {
		zone := rand.Intn(3) + 1
		id := p.Hit(NewID(zone, 1))
		if id != "" {
			t.Logf("round %d hit %v", i, id)
		}
	}
}

func TestPolicy(t *testing.T) {
	var p Policy

	t.Log("EMA:")
	config.Policy = "ema"
	config.Threshold = 0.5
	p = NewPolicy()
	uniformTest(p, t)

	t.Log("Consecutive")
	config.Policy = "consecutive"
	config.Threshold = 3
	p = NewPolicy()
	uniformTest(p, t)

	t.Log("Majority")
	config.Policy = "majority"
	config.Threshold = 1
	p = NewPolicy()
	uniformTest(p, t)
}
