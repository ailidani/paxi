package vpaxos

import (
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
)

// entry in log
type entry struct {
	ballot  paxi.Ballot
	command paxi.Command
	commit  bool
	request *paxi.Request
	quorum  *paxi.Quorum
}

type vpaxos struct {
	paxi.Node

	log     map[int]*entry
	ballot  paxi.Ballot
	slot    int
	execute int
}

func new(node paxi.Node) *vpaxos {
	v := &vpaxos{
		Node: node,
		log:  make(map[int]*entry),
		// ballot: paxi.NewBallot(1, paxi.NewID(node.ID().Zone(), 1)),
		slot: -1,
	}

	v.Register(P2a{}, v.handleP2a)
	v.Register(P2b{}, v.handleP2b)
	v.Register(P3{}, v.handleP3)

	return v
}

func (v *vpaxos) handleRequest(m paxi.Request) {
	if v.ballot.ID() == v.ID() {
		v.slot++
		v.log[v.slot] = &entry{
			ballot:  v.ballot,
			command: m.Command,
			request: &m,
			quorum:  paxi.NewQuorum(),
		}
		v.log[v.slot].quorum.ACK(v.ID())
		v.Multicast(v.ID().Zone(), P2a{
			Ballot:  v.ballot,
			Slot:    v.slot,
			Command: m.Command,
		})
	} else {
		v.Forward(v.ballot.ID(), m)
	}
}

func (v *vpaxos) handleP2a(m P2a) {
	log.Debugf("replica %v received %v", v.ID(), m)
	if m.Ballot >= v.ballot {
		v.ballot = m.Ballot
		v.slot = paxi.Max(v.slot, m.Slot)
		v.log[m.Slot] = &entry{
			ballot:  m.Ballot,
			command: m.Command,
			commit:  false,
		}
	}

	v.Send(m.Ballot.ID(), P2b{
		Ballot: v.ballot,
		Slot:   m.Slot,
		ID:     v.ID(),
	})
}

func (v *vpaxos) handleP2b(m P2b) {
	log.Debugf("replica %v received %v", v.ID(), m)
	if m.Ballot < v.log[m.Slot].ballot || v.log[m.Slot].commit {
		return
	}

	if m.Ballot > v.ballot {
		v.ballot = m.Ballot
	}

	if m.Ballot.ID() == v.ID() && m.Ballot == v.log[m.Slot].ballot {
		v.log[m.Slot].quorum.ACK(m.ID)
		if v.log[m.Slot].quorum.ZoneMajority() {
			v.log[m.Slot].commit = true
			v.Multicast(v.ID().Zone(), P3{
				Ballot:  m.Ballot,
				Slot:    m.Slot,
				Command: v.log[m.Slot].command,
			})

			v.exec()
		}
	}
}

func (v *vpaxos) handleP3(m P3) {
	log.Debugf("replica %v received Commit %+v", v.ID(), m)
	v.slot = paxi.Max(v.slot, m.Slot)
	e, exist := v.log[m.Slot]
	if !exist {
		v.log[m.Slot] = &entry{}
		e = v.log[m.Slot]
	}

	e.command = m.Command
	e.commit = true

	v.exec()
}

func (v *vpaxos) exec() {
	for {
		e, ok := v.log[v.execute]
		if !ok || !e.commit {
			break
		}
		value := v.Execute(e.command)
		if e.request != nil {
			e.request.Reply(paxi.Reply{
				Command: e.command,
				Value:   value,
			})
			e.request = nil
		}
		v.execute++
	}
}
