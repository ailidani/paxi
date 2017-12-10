package paxi

import (
	"encoding/gob"
	"errors"
	"testing"
)

var id1 = NewID(1, 1)
var id2 = NewID(1, 2)

var TCPAddress = map[ID]string{
	id1: "tcp://127.0.0.1:1735",
	id2: "tcp://127.0.0.1:1736",
}

var UDPAddress = map[ID]string{
	id1: "udp://127.0.0.1:1735",
	id2: "udp://127.0.0.1:1736",
}

var ChanAddress = map[ID]string{
	id1: "chan://127.0.0.1:1735",
	id2: "chan://127.0.0.1:1736",
}

var MixAddress = map[ID]string{
	id1: "tcp://127.0.0.1:1735",
	id2: "udp://127.0.0.1:1736",
}

type MSG struct {
	I int
	S string
}

func run(address map[ID]string) error {
	gob.Register(MSG{})
	send := &MSG{42, "hello"}
	go func() {
		sock1 := NewSocket(id1, address, "gob")
		defer sock1.Close()
		sock1.Multicast(id1.Zone(), send)
	}()
	sock2 := NewSocket(id2, address, "gob")
	defer sock2.Close()
	recv := sock2.Recv()
	if recv.(MSG) != *send {
		return errors.New("")
	}
	return nil
}

func TestSocket(t *testing.T) {
	var err error
	err = run(TCPAddress)
	if err != nil {
		t.Error()
	}

	err = run(UDPAddress)
	if err != nil {
		t.Error()
	}

	err = run(ChanAddress)
	if err != nil {
		t.Error()
	}

	// err = run(MixAddress)
	// if err != nil {
	// 	t.Error()
	// }
}
