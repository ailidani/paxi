package kpaxos

import (
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
	"github.com/ailidani/paxi/paxos"
)

// Replica KPaxos replica with Paxos instance for each key
type Replica struct {
	paxi.Node
	paxi map[paxi.Key]*paxos.Paxos

	key paxi.Key // current working key
}

func NewReplica(id paxi.ID) *Replica {
	r := new(Replica)
	r.Node = paxi.NewNode(id)
	r.paxi = make(map[paxi.Key]*paxos.Paxos)

	r.Register(paxi.Request{}, r.handleRequest)
	r.Register(Prepare{}, r.handlePrepare)
	r.Register(Promise{}, r.handlePromise)
	r.Register(Accept{}, r.handleAccept)
	r.Register(Accepted{}, r.handleAccepted)
	r.Register(Commit{}, r.handleCommit)
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

func (r *Replica) init(key paxi.Key) {
	if _, exists := r.paxi[key]; !exists {
		r.paxi[key] = paxos.NewPaxos(r)
	}
}

func (r *Replica) handleRequest(m paxi.Request) {
	r.key = m.Command.Key
	r.init(r.key)

	leader := index(r.key)
	if leader == r.ID() {
		r.paxi[r.key].HandleRequest(m)
	} else {
		go r.Forward(leader, m)
	}
}

func (r *Replica) handlePrepare(m Prepare) {
	r.key = m.Key
	r.init(r.key)
	r.paxi[r.key].HandleP1a(m.P1a)
}

func (r *Replica) handlePromise(m Promise) {
	r.key = m.Key
	r.paxi[r.key].HandleP1b(m.P1b)
}

func (r *Replica) handleAccept(m Accept) {
	r.key = m.Key
	r.init(r.key)
	r.paxi[r.key].HandleP2a(m.P2a)
}

func (r *Replica) handleAccepted(m Accepted) {
	r.key = m.Key
	r.paxi[r.key].HandleP2b(m.P2b)
}

func (r *Replica) handleCommit(m Commit) {
	log.Debugf("Replica ===[%v]===>>> Replica %s\n", m, r.ID())
	r.key = m.Key
	r.init(r.key)
	r.paxi[r.key].HandleP3(m.P3)
}

func (r *Replica) keys() int {
	sum := 0
	for _, paxos := range r.paxi {
		if paxos.IsLeader() {
			sum++
		}
	}
	return sum
}

// Broadcast overrides Socket interface in Node
func (r *Replica) Broadcast(msg interface{}) {
	switch m := msg.(type) {
	case paxos.P1a:
		r.Node.Broadcast(Prepare{r.key, m})
	case paxos.P2a:
		r.Node.Broadcast(Accept{r.key, m})
	case paxos.P3:
		r.Node.Broadcast(Commit{r.key, m})
	default:
		r.Node.Broadcast(msg)
	}
}

// Send overrides Socket interface in Node
func (r *Replica) Send(to paxi.ID, msg interface{}) {
	switch m := msg.(type) {
	case paxos.P1b:
		r.Node.Send(to, Promise{r.key, m})
	case paxos.P2b:
		r.Node.Send(to, Accepted{r.key, m})
	default:
		r.Node.Send(to, msg)
	}
}
