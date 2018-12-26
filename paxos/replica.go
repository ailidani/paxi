package paxos

import (
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
	"errors"
)

// Replica for one Paxos instance
type Replica struct {
	paxi.Node
	*Paxos
}

// NewReplica generates new Paxos replica
func NewReplica(id paxi.ID) *Replica {
	r := new(Replica)
	r.Node = paxi.NewNode(id)
	r.Paxos = NewPaxos(r)
	r.Register(paxi.Request{}, r.handleRequest)
	r.Register(P1a{}, r.HandleP1a)
	r.Register(P1b{}, r.HandleP1b)
	r.Register(P2a{}, r.HandleP2a)
	r.Register(P2b{}, r.HandleP2b)
	r.Register(P3{}, r.HandleP3)
	return r
}

func (r *Replica) handleRequest(m paxi.Request) {
	log.Debugf("Replica %s received %v\n", r.ID(), m)
	if paxi.GetConfig().FastRead && m.Command.IsRead() {
		v := r.Node.Execute(m.Command)
		m.Reply(paxi.Reply{
			Command: m.Command,
			Value:   v,
		})
		return
	}
	if m.ReqType == paxi.REQ_PAXOS_QUORUM_READ {
		// do PQR
		log.Debugf("Replica %s starting PQR\n", r.ID())
		if m.BSlot == paxi.NO_BARRIER_SLOT {
			// this is the first PQR request
			if r.Paxos.execute - 1 == r.Paxos.slot {
				// return value if we see the max accepted slot the same as last executed
				r.sendPQRExecSlot(m)
				return
			} else {
				if !r.Paxos.IsInProgress(m.Command.Key){
					// here we are trying to avoid the barrier if Key is not in progress
					r.sendPQRExecSlot(m)
					return
				}
				// otherwise return barrier slot
				r.sendPQRBarrierSlot(m)
				return
			}
		} else {
			barrier_slot := m.BSlot
			if r.Paxos.execute > int(barrier_slot) {
				r.sendPQRExecSlot(m)
				return
			} else {
				r.sendPQRBarrierSlot(m)
				return
			}
		}
		m.Reply(paxi.Reply{
			Command: m.Command,
			Value:   nil,
			Err:     errors.New("PQR command is in wrong format"),
		})
		return
	}

	if paxi.GetConfig().Adaptive {
		if r.Paxos.IsLeader() || r.Paxos.Ballot() == 0 {
			r.Paxos.HandleRequest(m)
		} else {
			go r.Forward(r.Paxos.Leader(), m)
		}
	} else {
		r.Paxos.HandleRequest(m)
	}
}

func (r *Replica) sendPQRBarrierSlot(m paxi.Request) {
	slot := r.Paxos.slot
	log.Debugf("Replica %s sending PQR barrier slot %d\n", r.ID(), slot)
	m.Reply(paxi.Reply{
		Command: m.Command,
		Value:   nil,
		Slot: r.Paxos.slot,
	})
}

func (r *Replica) sendPQRExecSlot(m paxi.Request) {
	slot := r.Paxos.execute - 1
	log.Debugf("Replica %s sending PQR data at slot %d\n", r.ID(), slot)
	data := r.Get(m.Command.Key)
	m.Reply(paxi.Reply{
		Command: m.Command,
		Slot: slot,
		Value:   data,
	})
}