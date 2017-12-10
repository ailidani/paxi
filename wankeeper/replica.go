package wankeeper

import (
	. "github.com/ailidani/paxi"
)

type Replica struct {
	*Node
	leader bool
	master bool
}

func NewReplica(config Config) *Replica {
	r := new(Replica)
	r.Node = NewNode(config)
	if config.ID.Zone() == 1 {
		r.master = true
	}
	r.Register(Request{}, r.handleRequest)
	return r
}

func (r *Replica) handleRequest(msg Request) {
}
