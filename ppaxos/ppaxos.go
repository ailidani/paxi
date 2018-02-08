package ppaxos

import (
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
)

type entry struct {
	ballot  paxi.Ballot
	command paxi.Command
	request *paxi.Request
}

type PPaxos struct {
	paxi.Node

	key    paxi.Key
	log    map[int]*entry
	active bool
	ballot paxi.Ballot
	slot   int

	quorum   *paxi.Quorum    // phase 1 quorum
	requests []*paxi.Request // phase 1 pending requests
}

func NewPPaxos(node paxi.Node, key paxi.Key) *PPaxos {
	p := &PPaxos{
		Node:     node,
		key:      key,
		log:      make(map[int]*entry, node.Config().BufferSize),
		quorum:   paxi.NewQuorum(),
		requests: make([]*paxi.Request, 0),
	}
	p.log[0] = &entry{}
	return p
}

// IsLeader indecates if this node is current leader
func (p *PPaxos) IsLeader() bool {
	return p.active || p.ballot.ID() == p.ID()
}

// Leader returns leader id of the current ballot
func (p *PPaxos) Leader() paxi.ID {
	return p.ballot.ID()
}

// Ballot returns current ballot
func (p *PPaxos) Ballot() paxi.Ballot {
	return p.ballot
}

func (p *PPaxos) handleRequest(r paxi.Request) {
	if !p.active {
		p.requests = append(p.requests, &r)
		if p.ballot.ID() != p.ID() {
			p.p1a()
		}
	} else {
		p.p2a(&r)
	}
}

func (p *PPaxos) p1a() {
	if p.active {
		return
	}
	p.ballot.Next(p.ID())
	p.quorum.Reset()
	p.quorum.ACK(p.ID())
	m := P1a{
		Key:    p.key,
		Ballot: p.ballot,
	}
	p.Broadcast(&m)
}

func (p *PPaxos) p2a(r *paxi.Request) {
	p.slot++
	p.log[p.slot] = &entry{
		ballot:  p.ballot,
		command: r.Command,
		request: r,
	}
	value := p.Execute(r.Command)
	r.Reply(paxi.Reply{
		Command: r.Command,
		Value:   value,
	})
	m := &P2a{
		Key:     p.key,
		Ballot:  p.ballot,
		Slot:    p.slot,
		Command: r.Command,
	}
	p.Broadcast(&m)
}

func (p *PPaxos) HandleP1a(m P1a) {
	if m.Ballot > p.ballot {
		p.ballot = m.Ballot
		p.active = false
		if len(p.requests) > 0 {
			defer p.p1a()
		}
	}

	p.Send(m.Ballot.ID(), &P1b{
		Key:    p.key,
		Ballot: p.ballot,
		ID:     p.ID(),
		Slot:   p.slot,
	})
}

func (p *PPaxos) HandleP1b(m P1b) {
	if m.Ballot < p.ballot || p.active {
		return
	}

	p.slot = paxi.Max(p.slot, m.Slot)

	if m.Ballot > p.ballot {
		p.ballot = m.Ballot
		p.active = false
		//p.p1a()
	}

	if m.Ballot.ID() == p.ID() && m.Ballot == p.ballot {
		p.quorum.ACK(m.ID)
		if p.quorum.Q1() {
			p.active = true
			for _, r := range p.requests {
				p.p2a(r)
			}
			p.requests = make([]*paxi.Request, 0)
		}
	}
}

func (p *PPaxos) HandleP2a(m P2a) {
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.Ballot.ID(), m, p.ID())
	if m.Ballot >= p.ballot {
		p.ballot = m.Ballot
		p.active = false
		p.slot = paxi.Max(p.slot, m.Slot)
		if e, exists := p.log[m.Slot]; exists {
			if m.Ballot > e.ballot {
				e.command = m.Command
				e.ballot = m.Ballot
			}
		} else {
			p.log[m.Slot] = &entry{
				ballot:  m.Ballot,
				command: m.Command,
			}
		}
	}

	p.Send(m.Ballot.ID(), &P2b{
		Key:    p.key,
		Ballot: p.ballot,
		Slot:   m.Slot,
		ID:     p.ID(),
	})

}

func (p *PPaxos) HandleP2b(m P2b) {
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.ID, m, p.ID())
	// TODO check for slot ballot instead??
	if m.Ballot < p.ballot {
		return
	}

	p.slot = paxi.Max(p.slot, m.Slot)

	if m.Ballot > p.ballot {
		p.ballot = m.Ballot
		p.active = false
	}
}
