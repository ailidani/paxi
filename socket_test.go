package paxi

import (
	"encoding/gob"
	"errors"
	"testing"
)

var id1 = ID("1.1")
var id2 = ID("1.2")

var Address = map[ID]string{
	id1: "127.0.0.1:1735",
	id2: "127.0.0.1:1736",
}

type MSG struct {
	I int
	S string
}

func run(transport string) error {
	gob.Register(MSG{})
	send := &MSG{42, "hello"}
	go func() {
		sock1 := NewSocket(id1, Address, transport, "gob")
		defer sock1.Close()
		sock1.Multicast(id1.Zone(), send)
	}()
	sock2 := NewSocket(id2, Address, transport, "gob")
	defer sock2.Close()
	recv := sock2.Recv()
	if recv.(MSG) != *send {
		return errors.New("")
	}
	return nil
}

func TestSocket(t *testing.T) {
	var err error
	err = run("chan")
	if err != nil {
		t.Error()
	}

	err = run("udp")
	if err != nil {
		t.Error()
	}

	err = run("tcp")
	if err != nil {
		t.Error()
	}
}
