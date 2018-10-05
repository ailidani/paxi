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

type paxos struct {
	paxi.Node

	log     map[int]*entry
	ballot  paxi.Ballot
	slot    int
	execute int
	active  bool

	pending []paxi.Request // pending during phase-1
	quorum  *paxi.Quorum
}

func newPaxos(node paxi.Node) *paxos {
	v := &paxos{
		Node: node,
		log:  make(map[int]*entry),
		// ballot: paxi.NewBallot(1, paxi.NewID(node.ID().Zone(), 1)),
		slot:    -1,
		pending: make([]paxi.Request, 0),
		quorum:  paxi.NewQuorum(),
	}

	v.Register(P1b{}, v.handleP1b)
	v.Register(P2a{}, v.handleP2a)
	v.Register(P2b{}, v.handleP2b)
	v.Register(P3{}, v.handleP3)

	return v
}

func (p *paxos) handleRequest(m paxi.Request) {
	if !p.active {
		p.pending = append(p.pending, m)
		return
	}

	if p.ballot.ID() != p.ID() {
		p.Forward(p.ballot.ID(), m)
	}

	p.slot++
	p.log[p.slot] = &entry{
		ballot:  p.ballot,
		command: m.Command,
		request: &m,
		quorum:  paxi.NewQuorum(),
	}
	p.log[p.slot].quorum.ACK(p.ID())
	p.MulticastZone(p.ID().Zone(), P2a{
		Ballot:  p.ballot,
		Slot:    p.slot,
		Command: m.Command,
	})
}

func (p *paxos) handleP1b(m P1b) {
	log.Debugf("replica %v received %v", p.ID(), m)
	if p.active || m.Ballot < p.ballot {
		return
	}

	p.quorum.ACK(m.ID)
	if p.quorum.ZoneMajority() {
		p.quorum.Reset()
		p.active = true
		for _, r := range p.pending {
			p.handleRequest(r)
		}
		p.pending = nil
	}
}

func (p *paxos) handleP2a(m P2a) {
	log.Debugf("replica %v received %v", p.ID(), m)
	if m.Ballot >= p.ballot {
		p.ballot = m.Ballot
		p.slot = paxi.Max(p.slot, m.Slot)
		p.log[m.Slot] = &entry{
			ballot:  m.Ballot,
			command: m.Command,
			commit:  false,
		}
	}

	p.Send(m.Ballot.ID(), P2b{
		Ballot: p.ballot,
		Slot:   m.Slot,
		ID:     p.ID(),
	})
}

func (p *paxos) handleP2b(m P2b) {
	log.Debugf("replica %v received %v", p.ID(), m)
	if m.Ballot < p.log[m.Slot].ballot || p.log[m.Slot].commit {
		return
	}

	if m.Ballot > p.ballot {
		p.ballot = m.Ballot
	}

	if m.Ballot.ID() == p.ID() && m.Ballot == p.log[m.Slot].ballot {
		p.log[m.Slot].quorum.ACK(m.ID)
		if p.log[m.Slot].quorum.ZoneMajority() {
			p.log[m.Slot].commit = true
			p.MulticastZone(p.ID().Zone(), P3{
				Ballot:  m.Ballot,
				Slot:    m.Slot,
				Command: p.log[m.Slot].command,
			})

			p.exec()
		}
	}
}

func (p *paxos) handleP3(m P3) {
	log.Debugf("replica %v received %v", p.ID(), m)
	p.slot = paxi.Max(p.slot, m.Slot)
	e, exist := p.log[m.Slot]
	if !exist {
		p.log[m.Slot] = &entry{}
		e = p.log[m.Slot]
	}

	e.command = m.Command
	e.commit = true

	p.exec()
}

func (p *paxos) exec() {
	for {
		e, ok := p.log[p.execute]
		if !ok || !e.commit {
			break
		}
		value := p.Execute(e.command)
		if e.request != nil {
			e.request.Reply(paxi.Reply{
				Command: e.command,
				Value:   value,
			})
			e.request = nil
		}
		p.execute++
	}
}
