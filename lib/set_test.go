package lib

import "testing"

func TestSet(t *testing.T) {
	s := NewSet()
	for i := 1; i <= 10; i++ {
		s.Add(i)
	}

	if !s.Has(5) {
		t.Error("missing element")
	}

	s.Remove(5)
	if s.Has(5) {
		t.Error("cannot remove element")
	}
}
