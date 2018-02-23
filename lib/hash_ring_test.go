package lib

import "testing"

func TestHashRing(t *testing.T) {
	ring := new(HashRing)
	a := "a"
	b := "b"
	c := "c"

	ring.Insert(a, []byte(a))

	if ring.Get([]byte(b)) != a {
		t.Error()
	}

	ring.Insert(b, []byte(b))
	if ring.Next(a) != b {
		t.Errorf("%v", ring.Next(a))
	}

	ring.Insert(c, []byte(c))
	if ring.Next(c) != a && ring.Next(c) != b {
		t.Error()
	}
}
