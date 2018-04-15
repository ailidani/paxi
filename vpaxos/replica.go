package vpaxos

import (
	"sync"

	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
)

// master node id
var mid = paxi.ID("2.1")

type Replica struct {
	paxi.Node

	paxos   *gpaxos
	index   map[paxi.Key]int // key -> group id
	leaders map[int]paxi.ID  // group id -> leader id
	policy  map[paxi.Key]paxi.Policy

	master *master
	cond   *sync.Cond
}

func NewReplica(id paxi.ID) *Replica {
	r := &Replica{
		Node:   paxi.NewNode(id),
		index:  make(map[paxi.Key]int),
		policy: make(map[paxi.Key]paxi.Policy),
		cond:   sync.NewCond(new(sync.Mutex)),
	}
	r.paxos = newGPaxos(id.Zone(), r.Node)

	if r.ID() == mid {
		r.master = new(master)
	}

	r.Register(paxi.Request{}, r.handleRequest)
	r.Register(Info{}, r.handleInfo)
	r.Register(Prepare{}, r.handlePrepare)
	r.Register(Promise{}, r.handlePromise)
	r.Register(Accept{}, r.handleAccept)
	r.Register(Accepted{}, r.handleAccepted)
	r.Register(Commit{}, r.handleCommit)

	return r
}

func (r *Replica) init(key paxi.Key) {
	if _, exists := r.index[key]; !exists {
		if r.master == nil {
			r.Send(mid, Query{key, r.ID(), r.paxos.Ballot()})
			r.cond.Wait()
		} else {
			r.master.create(key)
			r.index[key] = r.paxos.gid
		}
	}
}

func (r *Replica) handleRequest(m paxi.Request) {
	log.Debugf("replica %v received %v", r.ID(), m)
	k := m.Command.Key
	r.init(k)
	gid := r.index[k]
	if gid == r.paxos.gid {
		r.paxos.HandleRequest(m)
	} else {
		id := r.leaders[gid]
		r.Forward(id, m)
	}
}

func (r *Replica) handleInfo(m Info) {
	r.index[m.Key] = m.GroupID
	r.leaders[m.GroupID] = m.Ballot.ID()
	r.cond.Signal()
}

func (r *Replica) handlePrepare(m Prepare) {
	r.paxos.HandleP1a(m.P1a)
}

func (r *Replica) handlePromise(m Promise) {
	r.paxos.HandleP1b(m.P1b)
}

func (r *Replica) handleAccept(m Accept) {
	r.paxos.HandleP2a(m.P2a)
}

func (r *Replica) handleAccepted(m Accepted) {
	r.paxos.HandleP2b(m.P2b)
}

func (r *Replica) handleCommit(m Commit) {
	r.paxos.HandleP3(m.P3)
}
