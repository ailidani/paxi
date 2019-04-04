package paxi

import (
	"encoding/gob"
	"testing"
)

var id1 = ID("1.1")
var id2 = ID("1.2")

var Address = map[ID]string{
	id1: "127.0.0.1:1736",
	id2: "127.0.0.1:1737",
}

type MSG struct {
	I int
	S string
}

func run(transport string, t *testing.T) {
	gob.Register(MSG{})
	var send interface{}
	var recv interface{}

	address := make(map[ID]string)
	if transport != "" {
		address[id1] = transport + "://" + Address[id1]
		address[id2] = transport + "://" + Address[id2]
	} else {
		address[id1] = Address[id1]
		address[id2] = Address[id2]
	}

	send = MSG{42, "hello"}
	go func() {
		sock1 := NewSocket(id1, address)
		defer sock1.Close()
		sock1.Broadcast(send)
	}()
	sock2 := NewSocket(id2, address)
	defer sock2.Close()
	recv = sock2.Recv()
	if send.(MSG) != recv.(MSG) {
		t.Error("expect recv equal to send message")
	}
}

func TestSocket(t *testing.T) {
	run("chan", t)
	run("tcp", t)
	run("udp", t)
}
