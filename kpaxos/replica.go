package kpaxos

import (
	. "paxi"
)

type Replica struct {
	*Node
	paxi map[Key]*paxos
}

func NewReplica(config *Config) *Replica {
	r := new(Replica)
	r.Node = NewNode(config)
	r.paxi = make(map[Key]*paxos)
	r.Register(Request{}, r.handleRequest)
	r.Register(Prepare{}, r.handlePrepare)
	r.Register(Promise{}, r.handlePromise)
	r.Register(Accept{}, r.handleAccept)
	r.Register(Accepted{}, r.handleAccepted)
	r.Register(Commit{}, r.handleCommit)
	return r
}

func index(key Key) ID {
	if key < 200 {
		return NewID(1, 1)
	} else if key >= 200 && key < 400 {
		return NewID(2, 1)
	} else if key >= 400 && key < 600 {
		return NewID(3, 1)
	} else if key >= 600 && key < 800 {
		return NewID(4, 1)
	} else {
		return NewID(5, 1)
	}
}

func (r *Replica) init(key Key) {
	if _, exists := r.paxi[key]; !exists {
		r.paxi[key] = NewPaxos(r.Node, key)
		id := index(key)
		if id == r.ID {
			r.paxi[key].active = true
		}
		r.paxi[key].ballot = NextBallot(1, id)
	}
}

func (r *Replica) handleRequest(msg Request) {
	key := msg.Command.Key
	r.init(key)
	r.paxi[key].handleRequest(msg)
}

func (r *Replica) handlePrepare(msg Prepare) {
	key := msg.Key
	r.init(key)
	r.paxi[key].handlePrepare(msg)
}

func (r *Replica) handlePromise(msg Promise) {
	key := msg.Key
	r.paxi[key].handlePromise(msg)
}

func (r *Replica) handleAccept(msg Accept) {
	key := msg.Key
	r.init(key)
	r.paxi[key].handleAccept(msg)
}

func (r *Replica) handleAccepted(msg Accepted) {
	key := msg.Key
	r.paxi[key].handleAccepted(msg)
}

func (r *Replica) handleCommit(msg Commit) {
	key := msg.Key
	r.init(key)
	r.paxi[key].handleCommit(msg)
}

func (r *Replica) keys() int {
	sum := 0
	for _, paxos := range r.paxi {
		if paxos.active {
			sum++
		}
	}
	return sum
}
