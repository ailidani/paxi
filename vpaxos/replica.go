package vpaxos

import (
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
)

// master node id
var mid = paxi.ID("2.1")

type Replica struct {
	paxi.Node

	paxos  *gpaxos
	index  map[paxi.Key]paxi.Ballot
	policy map[paxi.Key]paxi.Policy

	pending map[paxi.Key][]paxi.Request
	master  *master
}

func NewReplica(id paxi.ID) *Replica {
	r := &Replica{
		Node:    paxi.NewNode(id),
		index:   make(map[paxi.Key]paxi.Ballot),
		policy:  make(map[paxi.Key]paxi.Policy),
		pending: make(map[paxi.Key][]paxi.Request),
	}
	r.paxos = newGPaxos(id.Zone(), r.Node)

	if r.ID() == mid {
		r.master = newMaster(r)
	}

	r.Register(paxi.Request{}, r.handleRequest)
	r.Register(Info{}, r.handleInfo)
	r.Register(Move{}, r.handleMove)
	r.Register(Prepare{}, r.handlePrepare)
	r.Register(Promise{}, r.handlePromise)
	r.Register(Accept{}, r.handleAccept)
	r.Register(Accepted{}, r.handleAccepted)
	r.Register(Commit{}, r.handleCommit)

	return r
}

func (r *Replica) monitor(k paxi.Key, id paxi.ID) {
	_, exist := r.policy[k]
	if !exist {
		r.policy[k] = paxi.NewPolicy()
	}
	to := r.policy[k].Hit(id)
	if to != "" && to.Zone() != r.ID().Zone() {
		r.Send(mid, Move{
			Key:       k,
			From:      r.ID(),
			To:        to,
			OldBallot: r.paxos.Ballot(),
		})
	}
}

func (r *Replica) handleRequest(m paxi.Request) {
	log.Debugf("replica %v received %v", r.ID(), m)
	k := m.Command.Key
	b, exist := r.index[k]
	if !exist {
		if r.master == nil {
			// if unknown key, save request and query to master
			_, ok := r.pending[k]
			if !ok {
				r.pending[k] = make([]paxi.Request, 0)
			}
			r.pending[k] = append(r.pending[k], m)
			r.Send(mid, Query{
				Key: k,
				ID:  r.ID(),
			})
			return
		}
		r.master.create(k)
		b = paxi.NewBallot(1, r.ID())
		r.index[k] = b
	}
	if b.ID().Zone() == r.paxos.gid {
		r.paxos.HandleRequest(m)
		r.monitor(k, m.NodeID)
	} else {
		r.Forward(b.ID(), m)
	}
}

func (r *Replica) handleInfo(m Info) {
	log.Debugf("replica %v received Info %+v", r.ID(), m)
	r.index[m.Key] = m.Ballot
	if len(r.pending[m.Key]) > 0 {
		for _, request := range r.pending[m.Key] {
			r.handleRequest(request)
		}
	}
}

func (r *Replica) handleMove(m Move) {
	log.Debugf("replica %v received Move %+v", r.ID(), m)
	if m.OldBallot == r.index[m.Key] {
		r.index[m.Key] = m.NewBallot
	}
	if r.master != nil {
		r.master.handleMove(m)
	}
}

func (r *Replica) handlePrepare(m Prepare) {
	log.Debugf("replica %v received Prepare %+v", r.ID(), m)
	r.paxos.HandleP1a(m.P1a)
}

func (r *Replica) handlePromise(m Promise) {
	log.Debugf("replica %v received Promise %+v", r.ID(), m)
	r.paxos.HandleP1b(m.P1b)
}

func (r *Replica) handleAccept(m Accept) {
	log.Debugf("replica %v received Accept %+v", r.ID(), m)
	r.paxos.HandleP2a(m.P2a)
}

func (r *Replica) handleAccepted(m Accepted) {
	log.Debugf("replica %v received Accepted %+v", r.ID(), m)
	r.paxos.HandleP2b(m.P2b)
}

func (r *Replica) handleCommit(m Commit) {
	log.Debugf("replica %v received Commit %+v", r.ID(), m)
	r.paxos.HandleP3(m.P3)
}
