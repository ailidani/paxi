package sdpaxos

import (
	"time"

	"github.com/ailidani/paxi"
)

type entry struct {
	ballot    paxi.Ballot
	command   paxi.Command
	cquorum   *paxi.Quorum
	ccommit   bool
	gslot     uint64
	oquorum   *paxi.Quorum
	ocommit   bool
	request   *paxi.Request
	timestamp time.Time
}

type Replica struct {
	paxi.Node
	log             map[paxi.ID]map[uint64]*entry // received log
	glog            map[uint64]*entry             // global ordered log
	gslot           uint64                        // global slot number
	execute         uint64                        // last executed in global log
	slot            uint64                        // my slot number
	ballot          paxi.Ballot                   // my ballot
	sequencer       paxi.ID
	sequencerBallot paxi.Ballot
}

func NewReplica(id paxi.ID) *Replica {
	r := new(Replica)
	r.Node = paxi.NewNode(id)
	r.log = make(map[paxi.ID]map[uint64]*entry)
	for id := range paxi.GetConfig().Addrs {
		r.log[id] = make(map[uint64]*entry)
	}
	r.glog = make(map[uint64]*entry)
	r.ballot = paxi.NewBallot(1, id)
	r.sequencer = paxi.NewID(1, 1)

	r.Register(paxi.Request{}, r.handleRequest)
	r.Register(CAccept{}, r.handleCAccept)
	r.Register(CAck{}, r.handleCAck)
	r.Register(CCommit{}, r.handleCCommit)
	r.Register(OAccept{}, r.handleOAccept)
	r.Register(OAck{}, r.handleOAck)
	r.Register(OCommit{}, r.handleOCommit)

	return r
}

func (r *Replica) handleRequest(m paxi.Request) {
	_, exist := r.log[r.ID()]
	if !exist {
		r.log[r.ID()] = make(map[uint64]*entry)
	}
	r.log[r.ID()][r.slot] = &entry{
		ballot:  r.ballot,
		command: m.Command,
		cquorum: paxi.NewQuorum(),
		ccommit: false,
		gslot:   0,
		oquorum: paxi.NewQuorum(),
		ocommit: false,
		request: &m,
	}
	r.log[r.ID()][r.slot].cquorum.ACK(r.ID())
	r.Broadcast(CAccept{
		Ballot:  r.ballot,
		From:    r.ID(),
		Command: m.Command,
		Slot:    r.slot,
	})

	if r.sequencer == r.ID() {
		r.Broadcast(OAccept{
			Ballot: r.ballot,
			ID:     r.ID(),
			Slot:   r.slot,
			GSlot:  r.gslot,
			From:   r.ID(),
		})
		r.gslot++
	}

	r.slot++
}

func (r *Replica) handleCAccept(m CAccept) {
	_, exist := r.log[m.From]
	if !exist {
		r.log[m.From] = make(map[uint64]*entry)
	}
	r.log[m.From][m.Slot] = &entry{
		ballot:  m.Ballot,
		command: m.Command,
		ccommit: false,
		gslot:   0,
	}
	r.Send(m.From, CAck{
		Ballot: m.Ballot,
		From:   r.ID(),
		Slot:   m.Slot,
	})

	if r.sequencer == r.ID() {
		r.Broadcast(OAccept{
			Ballot: r.ballot,
			ID:     m.From,
			Slot:   m.Slot,
			GSlot:  r.gslot,
			From:   r.ID(),
		})
		r.gslot++
	}
}

func (r *Replica) handleCAck(m CAck) {
	if m.Ballot != r.ballot {
		return
	}
	entry := r.log[r.ID()][m.Slot]
	if entry.ccommit {
		return
	}
	entry.cquorum.ACK(m.From)
	if entry.cquorum.Majority() {
		entry.ccommit = true
		r.Broadcast(CCommit{
			Ballot:  r.ballot,
			From:    r.ID(),
			Slot:    m.Slot,
			Command: entry.command,
		})
		if entry.ocommit {
			r.glog[entry.gslot] = entry
			go r.exec()
		}
	}
}

func (r *Replica) handleCCommit(m CCommit) {
	_, exist := r.log[m.From]
	if !exist {
		r.log[m.From] = make(map[uint64]*entry)
	}
	e, exist := r.log[m.From][m.Slot]
	if !exist {
		r.log[m.From][m.Slot] = &entry{
			ballot:  m.Ballot,
			command: m.Command,
			ccommit: true,
		}
		return
	}
	e.ccommit = true

	if e.ocommit {
		go r.exec()
	}
}

func (r *Replica) handleOAccept(m OAccept) {
	if m.From == r.sequencer {
		e, exist := r.log[m.ID][m.Slot]
		if !exist {
			r.log[m.ID][m.Slot] = &entry{
				ccommit: false,
				ocommit: false,
				gslot:   m.GSlot,
			}
			e = r.log[m.ID][m.Slot]
		} else if e.ocommit {
			return
		}

		e.gslot = m.GSlot
		//r.glog[m.GSlot] = entry
		if m.ID == r.ID() {
			e.oquorum.ACK(r.ID())
		} else {
			r.Send(m.ID, OAck{
				Ballot: r.ballot,
				ID:     m.ID,
				Slot:   m.Slot,
				GSlot:  m.GSlot,
				From:   r.ID(),
			})
		}
	}
}

func (r *Replica) handleOAck(m OAck) {
	if m.ID == r.ID() {
		e, exist := r.log[m.ID][m.Slot]
		if !exist || e.ocommit {
			return
		}
		e.oquorum.ACK(m.From)
		if e.oquorum.Majority() {
			e.ocommit = true
			r.glog[m.GSlot] = e
			r.Broadcast(OCommit{
				Ballot: r.ballot,
				Slot:   m.Slot,
				GSlot:  m.GSlot,
				From:   r.ID(),
			})

			if e.ccommit {
				go r.exec()
			}
		}
	}
}

func (r *Replica) handleOCommit(m OCommit) {
	e, exist := r.log[m.From][m.Slot]
	if !exist {
		r.log[m.From][m.Slot] = &entry{
			ballot:  m.Ballot,
			ocommit: true,
			gslot:   m.GSlot,
		}
	}
	e.ocommit = true
	e.gslot = m.GSlot
	r.glog[m.GSlot] = e

	if e.ccommit {
		go r.exec()
	}
}

func (r *Replica) exec() {
	for {
		e, ok := r.glog[r.execute]
		if !ok || !e.ccommit || !e.ocommit {
			break
		}

		value := r.Node.Execute(e.command)
		if e.request != nil {
			reply := paxi.Reply{
				Command:    e.command,
				Value:      value,
				Properties: make(map[string]string),
			}
			e.request.Reply(reply)
			e.request = nil
		}
		// TODO clean up the log periodically
		//delete(r.glog, r.execute)
		r.execute++
	}
}
