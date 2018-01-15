package paxi

import (
	"testing"
)

func TestBallot(t *testing.T) {
	n := 0
	id := NewID(2, 1)
	b := NewBallot(n, id)

	b.Next(id)
	b.Next(id)

	if b.N() != n+2 {
		t.Errorf("Ballot.N() %v != %v", b.N(), n+1)
	}

	if b.ID() != id {
		t.Errorf("Ballot.ID() %v != %v", b.ID(), id)
	}
}
