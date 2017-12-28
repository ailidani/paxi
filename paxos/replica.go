package paxos

import "github.com/ailidani/paxi"

type Replica struct {
	paxi.Node
	*Paxos
}

func NewReplica(config paxi.Config) *Replica {
	r := new(Replica)
	r.Node = paxi.NewNode(config)
	r.Paxos = NewPaxos(r)
	r.Register(paxi.Request{}, r.HandleRequest)
	r.Register(P1a{}, r.HandleP1a)
	r.Register(P1b{}, r.HandleP1b)
	r.Register(P2a{}, r.HandleP2a)
	r.Register(P2b{}, r.HandleP2b)
	r.Register(P3{}, r.HandleP3)
	return r
}
