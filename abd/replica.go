package abd

import (
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
)

type state int

// states of each instance
const (
	GetPhase state = iota
	SetPhase
	Done
)

type entry struct {
	r         *paxi.Request
	state     state
	getQuorum *paxi.Quorum
	setQuorum *paxi.Quorum
	value     paxi.Value
	version   int
}

// Replica implements ABD atomic storage protocol
// Each read and write operation proceed in Get and Set phase
type Replica struct {
	paxi.Node
	cid int

	log     map[int]*entry
	version map[paxi.Key]int
}

// NewReplica generates ABD replica
func NewReplica(id paxi.ID) *Replica {
	r := new(Replica)
	r.Node = paxi.NewNode(id)
	r.log = make(map[int]*entry)
	r.version = make(map[paxi.Key]int)
	r.Register(paxi.Request{}, r.handleRequest)
	r.Register(Get{}, r.handleGet)
	r.Register(GetReply{}, r.handleGetReply)
	r.Register(Set{}, r.handleSet)
	r.Register(SetReply{}, r.handleSetReply)
	return r
}

func (r *Replica) handleRequest(m paxi.Request) {
	log.Debugf("Node %s received Request %v", r.ID(), m)
	k := m.Command.Key
	v := r.Get(k)
	version := r.version[k]
	r.cid++
	// entry save my local verion of value
	r.log[r.cid] = &entry{
		r:         &m,
		state:     GetPhase,
		getQuorum: paxi.NewQuorum(),
		setQuorum: paxi.NewQuorum(),
		value:     v,
		version:   version,
	}
	r.log[r.cid].getQuorum.ACK(r.ID())
	r.Broadcast(Get{
		ID:  r.ID(),
		CID: r.cid,
		Key: k,
	})
}

func (r *Replica) handleGet(m Get) {
	v := r.Node.Get(m.Key)
	r.Send(m.ID, GetReply{
		ID:      r.ID(),
		CID:     m.CID,
		Key:     m.Key,
		Value:   v,
		Version: r.version[m.Key],
	})
}

func (r *Replica) handleSet(m Set) {
	if m.Version > r.version[m.Key] {
		// update local value
		r.Node.Put(m.Key, m.Value)
		r.version[m.Key] = m.Version
	}
	r.Send(m.ID, SetReply{
		ID:  r.ID(),
		CID: m.CID,
		Key: m.Key,
	})
}

func (r *Replica) handleGetReply(m GetReply) {
	e := r.log[m.CID]
	if e.state != GetPhase {
		return
	}
	if m.Version > e.version {
		e.value = m.Value
		e.version = m.Version
		// update local value
		r.Node.Put(m.Key, m.Value)
		r.version[m.Key] = m.Version
	}
	e.getQuorum.ACK(m.ID)
	if e.getQuorum.Majority() {
		e.state = SetPhase // into set phase
		e.setQuorum.ACK(r.ID())
		if e.r.Command.IsRead() {
			r.Broadcast(Set{
				ID:      r.ID(),
				CID:     m.CID,
				Key:     m.Key,
				Value:   e.value,
				Version: e.version,
			})
		} else {
			e.value = e.r.Command.Value
			e.version++
			// write new value to local database first
			r.Node.Put(e.r.Command.Key, e.r.Command.Value)
			r.version[m.Key] = e.version
			r.Broadcast(Set{
				ID:      r.ID(),
				CID:     m.CID,
				Key:     e.r.Command.Key,
				Value:   e.r.Command.Value,
				Version: e.version,
			})
		}
	}
}

func (r *Replica) handleSetReply(m SetReply) {
	e := r.log[m.CID]
	if e.state != SetPhase {
		return
	}
	e.setQuorum.ACK(m.ID)
	if e.setQuorum.Majority() {
		e.state = Done
		if e.r.Command.IsRead() {
			e.r.Reply(paxi.Reply{
				Command: e.r.Command,
				Value:   e.value,
			})
		} else {
			e.r.Reply(paxi.Reply{
				Command: e.r.Command,
			})
		}
	}
}
