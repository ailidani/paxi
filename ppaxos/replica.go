package ppaxos

import (
	"encoding/gob"

	"github.com/ailidani/paxi"
)

type P2a struct {
	paxi.Command
}

func init() {
	gob.Register(P2a{})
}

type Replica struct {
	paxi.Node
}

func NewReplica(config paxi.Config) *Replica {
	r := new(Replica)
	r.Node = paxi.NewNode(config)
	r.Register(paxi.Request{}, r.handleRequest)
	r.Register(P2a{}, r.handle)
	return r
}

// TODO replace this with a consistent hash ring
func index(key paxi.Key) paxi.ID {
	if key < 200 {
		return paxi.ID("1.1")
	} else if key >= 200 && key < 400 {
		return paxi.ID("2.1")
	} else if key >= 400 && key < 600 {
		return paxi.ID("3.1")
	} else if key >= 600 && key < 800 {
		return paxi.ID("4.1")
	} else {
		return paxi.ID("5.1")
	}
}

func (r *Replica) handleRequest(m paxi.Request) {
	key := m.Command.Key

	leader := index(key)
	if leader == r.ID() {
		v := r.Execute(m.Command)
		r.Broadcast(&P2a{
			Command: m.Command,
		})
		m.Reply(paxi.Reply{
			Command: m.Command,
			Value:   v,
		})
	} else {
		go r.Forward(leader, m)
	}
}

func (r *Replica) handle(m P2a) {
	r.Execute(m.Command)
}
