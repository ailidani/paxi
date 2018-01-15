package paxi

import "testing"

func TestLinerizabilityChecker(t *testing.T) {
	t.Parallel()

	ops := []operation{
		{100, 0, 0, 100},
		{nil, 100, 25, 75},
		{nil, 0, 30, 60},
	}
	c := newChecker(nil)
	res := c.linearizable(ops)
	if res != true {
		t.Fatal("expected operations to be linearizable")
	}
}
