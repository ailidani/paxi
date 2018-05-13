package paxi

import (
	"math/rand"
	"testing"
)

func uniformTest(p Policy, t *testing.T) {
	sum := 0
	for i := 0; i < 1000; i++ {
		zone := rand.Intn(3) + 1
		// time.Sleep(time.Duration(10) * time.Millisecond)
		id := p.Hit(NewID(zone, 1))
		if id != "" {
			sum++
		}
	}
	t.Logf("sum %d", sum)
}

func TestPolicy(t *testing.T) {
	var p Policy

	t.Log("EMA:")
	config.Policy = "ema"
	for k := 0.1; k >= 0.1; k -= 0.1 {
		config.Threshold = k
		p = NewPolicy()
		uniformTest(p, t)
	}

	t.Log("Consecutive")
	config.Policy = "consecutive"
	for k := 2; k <= 10; k++ {
		config.Threshold = float64(k)
		p = NewPolicy()
		uniformTest(p, t)
	}

	t.Log("Majority")
	config.Policy = "majority"
	for k := 1; k <= 2; k++ {
		config.Threshold = float64(k)
		p = NewPolicy()
		uniformTest(p, t)
	}
}
