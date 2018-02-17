package ppaxos

import (
	"github.com/ailidani/paxi"
)

type entry struct {
	ballot  paxi.Ballot
	command paxi.Command
	request *paxi.Request
}

// PPaxos instance
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

// NewPPaxos generates new PPaxos instance
func NewPPaxos(node paxi.Node, key paxi.Key) *PPaxos {
	p := &PPaxos{
		Node:     node,
		key:      key,
		log:      make(map[int]*entry, paxi.GetConfig().BufferSize),
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
	p.Broadcast(P1a{
		Key:    p.key,
		Ballot: p.ballot,
	})
}

func (p *PPaxos) p2a(r *paxi.Request) {
	p.slot++
	p.log[p.slot] = &entry{
		ballot:  p.ballot,
		command: r.Command,
		request: r,
	}
	p.Broadcast(P2a{
		Key:     r.Command.Key,
		Ballot:  p.ballot,
		Slot:    p.slot,
		Command: r.Command,
	})
	r.Reply(paxi.Reply{
		Command: r.Command,
		Value:   p.Execute(r.Command),
	})
}

// HandleP1a handles P1a message
func (p *PPaxos) HandleP1a(m P1a) {
	if m.Ballot > p.ballot {
		p.ballot = m.Ballot
		p.active = false
		if len(p.requests) > 0 {
			defer p.p1a()
		}
	}

	p.Send(m.Ballot.ID(), P1b{
		Key:    m.Key,
		Ballot: p.ballot,
		ID:     p.ID(),
		Slot:   p.slot,
		Value:  p.Node.Get(m.Key),
	})
}

// HandleP1b handles P1b message
func (p *PPaxos) HandleP1b(m P1b) {
	if m.Slot > p.slot {
		p.slot = m.Slot
		p.Node.Put(m.Key, m.Value)
	}

	if m.Ballot < p.ballot || p.active {
		return
	}

	if m.Ballot > p.ballot {
		p.ballot = m.Ballot
		p.active = false
		p.p1a()
	}

	if m.Ballot == p.ballot && m.Ballot.ID() == p.ID() {
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

// HandleP2a handles P2a message
func (p *PPaxos) HandleP2a(m P2a) {
	if m.Ballot >= p.ballot {
		p.ballot = m.Ballot
		p.active = false
		if m.Slot > p.slot {
			p.slot = m.Slot
			p.Execute(m.Command)
		}
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

	p.Send(m.Ballot.ID(), P2b{
		Key:    p.key,
		Ballot: p.ballot,
		ID:     p.ID(),
		Slot:   m.Slot,
		Value:  p.Get(p.key),
	})
}

// HandleP2b handles P2b message
func (p *PPaxos) HandleP2b(m P2b) {
	if m.Slot > p.slot {
		p.slot = m.Slot
		p.Node.Put(m.Key, m.Value)
	}

	// TODO check for slot ballot instead??
	if m.Ballot < p.ballot {
		return
	}

	if m.Ballot > p.ballot {
		p.ballot = m.Ballot
		p.active = false
	}
}
