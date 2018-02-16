package wankeeper

import (
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/paxos"
)

type Replica struct {
	paxi.Node
	*paxos.Paxos
	// current levels are 1 and 2
	level int
}

func NewReplica(id paxi.ID) *Replica {
	r := new(Replica)
	r.Node = paxi.NewNode(id)
	r.Paxos = paxos.NewPaxos(r)
	if id.Zone() == 1 {
		r.level = 2
	}
	r.Register(paxi.Request{}, r.handleRequest)
	return r
}

func (r *Replica) handleRequest(msg paxi.Request) {
}

// Broadcast overrides Socket interface in Node
func (r *Replica) Broadcast(msg interface{}) {
	r.Node.Multicast(r.ID().Zone(), msg)
}
