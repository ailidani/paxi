package paxi

import "testing"
import "encoding/gob"

type A struct {
	I int
	S string
	B bool
}

func TestGobCodec(t *testing.T) {
	gob.Register(A{})
	c := NewCodec("gob")
	a := A{1, "a", true}
	buf := c.Encode(a)
	t.Logf("Buffer is %x", buf)
	b := c.Decode(buf)
	t.Logf("Decoded %v", b)
	if a != b {
		t.Error(a, b)
	}
}
