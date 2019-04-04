package paxi

import "testing"

// examples from https://pdos.csail.mit.edu/6.824/papers/fb-consistency.pdf
func TestLinerizabilityChecker(t *testing.T) {
	c := newChecker()

	// single operation is liearizable
	ops := []*operation{
		&operation{42, nil, 0, 24},
	}
	if len(c.linearizable(ops)) != 0 {
		t.Error("expected operations to be linearizable")
	}

	// concurrent operation is linearizable
	// +--w---+
	//   +---r--+
	ops = []*operation{
		&operation{42, nil, 0, 5},
		&operation{nil, 42, 3, 10},
	}
	if len(c.linearizable(ops)) != 0 {
		t.Error("expected operations to be linearizable")
	}

	// no dependency in graph is linearizable
	ops = []*operation{
		&operation{1, nil, 0, 5},
		&operation{nil, 2, 6, 10},
		&operation{3, nil, 11, 15},
		&operation{nil, 4, 16, 20},
	}
	if len(c.linearizable(ops)) != 0 {
		t.Error("expected operations to be linearizable")
	}

	// concurrent reads are linearizable
	// +-------w100---------+
	//    +--r100--+
	//       +----r0-----+
	ops = []*operation{
		&operation{0, nil, 0, 0},
		&operation{100, nil, 0, 100},
		&operation{nil, 100, 5, 35},
		&operation{nil, 0, 30, 60},
	}
	if len(c.linearizable(ops)) != 0 {
		t.Error("expected operations to be linearizable")
	}

	// non-concurrent reads are not linearizable
	// +---------w100-----------+
	//   +---r100---+  +-r0--+
	ops = []*operation{
		&operation{0, nil, 0, 0},
		&operation{100, nil, 0, 100},
		&operation{nil, 100, 5, 25}, // reads 100, write time is cut to 25
		&operation{nil, 0, 30, 60},  // happens after previous read, but read 0
	}
	if len(c.linearizable(ops)) == 0 {
		t.Error("expected operations to NOT be linearizable")
	}

	// read misses a previous write is not linearizable
	// +--w1--+ +--w2--+ +--r1--+
	ops = []*operation{
		&operation{1, nil, 0, 5},
		&operation{2, nil, 6, 10},
		&operation{nil, 1, 11, 15},
	}
	if len(c.linearizable(ops)) == 0 {
		t.Error("expected operations to NOT be linearizable")
	}

	// cross reads is not linearizable
	// +--w1--+  +--r1--+
	// +--w2--+  +--r2--+
	ops = []*operation{
		&operation{1, nil, 0, 5},
		&operation{2, nil, 0, 5},
		&operation{nil, 1, 6, 10},
		&operation{nil, 2, 6, 10},
	}
	if len(c.linearizable(ops)) == 0 {
		t.Error("expected operations to NOT be linearizable")
	}

	// two amonaly reads
	// +--w1--+ +--w2--+ +--r1--+
	//                     +--r1--+
	ops = []*operation{
		&operation{1, nil, 0, 5},
		&operation{2, nil, 6, 10},
		&operation{nil, 1, 11, 15},
		&operation{nil, 1, 12, 16},
	}
	n := len(c.linearizable(ops))
	if n != 2 {
		t.Errorf("expected two amonaly operations, detected %d", n)
	}

	// test link between two writes
	// +--w1--+ +--r1--+ +--r1--+
	//          +--w2--+

	ops = []*operation{
		&operation{1, nil, 0, 5},
		&operation{nil, 1, 6, 10},
		&operation{2, nil, 7, 10},
		&operation{nil, 1, 11, 15},
	}
	if len(c.linearizable(ops)) == 0 {
		t.Errorf("expected violation")
	}
}

func TestNonUniqueValue(t *testing.T) {
	c := newChecker()

	// cross read same value should be linearizable
	// +--w1--+  +--r1--+
	// +--w1--+  +--r1--+
	ops := []*operation{
		&operation{1, nil, 0, 5},
		&operation{1, nil, 0, 5},
		&operation{nil, 1, 6, 10},
		&operation{nil, 1, 6, 10},
	}

	n := len(c.linearizable(ops))
	if n != 0 {
		t.Errorf("expected no violation, detected %d", n)
	}
}
