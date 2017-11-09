package wankeeper

import (
	. "paxi"
)

type Replica struct {
	*Node
	leader bool
	master bool
}

func NewReplica(config *Config) *Replica {
	r := new(Replica)
	r.Node = NewNode(config)
	if config.ID.Site() == 1 {
		r.master = true
	}
	r.Register(Request{}, r.handleRequest)
	return r
}

func (r *Replica) handleRequest(msg Request) {
}