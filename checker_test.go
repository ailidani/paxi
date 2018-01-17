package paxi

import "testing"

// examples from https://pdos.csail.mit.edu/6.824/papers/fb-consistency.pdf
func TestLinerizabilityChecker(t *testing.T) {
	c := newChecker(nil)

	ops := []operation{
		{100, nil, 0, 100},
		{nil, 100, 25, 75},
		{nil, 0, 30, 60},
	}
	if c.linearizable(ops) != true {
		t.Fatal("expected operations to be linearizable")
	}

	ops = []operation{
		{1, nil, 0, 5},
		{2, nil, 6, 10},
		{nil, 1, 11, 15},
	}
	if c.linearizable(ops) == true {
		t.Fatal("expected operations to NOT be linearizable")
	}

	ops = []operation{
		{1, nil, 0, 5},
		{2, nil, 0, 5},
		{nil, 1, 6, 10},
		{nil, 2, 6, 10},
	}
	if c.linearizable(ops) == true {
		t.Fatal("expected operations to NOT be linearizable")
	}
}
