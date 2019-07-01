package hpaxos

import (
	"time"

	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
)

// entry in log
type entry struct {
	ballot    paxi.Ballot
	command   paxi.Command
	commit    bool
	request   *paxi.Request
	quorum1   *paxi.Quorum
	quorum2   *paxi.Quorum
	timestamp time.Time
}

// Paxos instance
type Paxos struct {
	paxi.Node

	config []paxi.ID

	log     map[int]*entry // log ordered by slot
	execute int            // next execute slot number
	slot    int            // highest slot number

	Q1              func(*paxi.Quorum) bool
	Q2              func(*paxi.Quorum) bool
	ReplyWhenCommit bool
}

// NewPaxos creates new paxos instance
func NewPaxos(n paxi.Node, options ...func(*Paxos)) *Paxos {
	p := &Paxos{
		Node:            n,
		log:             make(map[int]*entry, paxi.GetConfig().BufferSize),
		slot:            -1,
		Q1:              func(q *paxi.Quorum) bool { return q.Size() == 1 },
		Q2:              func(q *paxi.Quorum) bool { return q.All() },
		ReplyWhenCommit: false,
	}

	for _, opt := range options {
		opt(p)
	}

	return p
}

func (p *Paxos) IsLeader(slot int) bool {
	return p.log[slot].ballot.ID() == p.ID()
}

func (p *Paxos) Leader(slot int) paxi.ID {
	return p.log[slot].ballot.ID()
}

func (p *Paxos) HandleRequest(r paxi.Request) {
	p.P1a(r)
}

func (p *Paxos) P1a(r paxi.Request) {
	p.slot++
	p.log[p.slot] = &entry{
		ballot:  paxi.NewBallot(0, p.ID()),
		command: r.Command,
		commit:  false,
		request: &r,
		quorum1: paxi.NewQuorum(),
		quorum2: paxi.NewQuorum(),
	}
	p.log[p.slot].quorum1.ACK(p.ID())

	if p.Q1(p.log[p.slot].quorum1) {
		p.P2a(p.slot)
	}
}

func (p *Paxos) P2a(s int) {
	e := p.log[s]
	if !p.Q1(e.quorum1) {
		log.Warning("Starting P2a without reaching Q1")
		return
	}

	e.quorum2.ACK(p.ID())
	p.Broadcast(P2a{
		Ballot:  e.ballot,
		Slot:    s,
		Command: e.command,
	})
}

func (p *Paxos) HandleP1a(m P1a) {
	p.slot = paxi.Max(p.slot, m.Slot)

	e, ok := p.log[m.Slot]
	if !ok {
		e = &entry{
			ballot: m.Ballot,
			commit: false,
		}
		p.log[m.Slot] = e
	}

	// old message
	if m.Ballot < e.ballot || e.commit {
		return
	}

	// new leader for slot
	if m.Ballot > e.ballot {
		e.ballot = m.Ballot
		e.quorum1.Reset()
		e.quorum2.Reset()
	}

	p.Send(m.Ballot.ID(), P1b{
		Ballot:  e.ballot,
		ID:      p.ID(),
		Command: e.command,
	})
}

func (p *Paxos) retry(s int, b paxi.Ballot) {
	e, ok := p.log[s]
	if !ok {
		log.Fatalf("Retry with unknown slot %d", s)
	}

	log.Infof("Replica %v Retrying slot %d", p.ID(), s)

	if !e.commit && b > e.ballot {
		e.ballot = b
		e.quorum1 = paxi.NewQuorum()
		e.quorum2 = paxi.NewQuorum()
		if e.request != nil {
			p.P1a(*e.request)
			e.request = nil
		}
	}
}

func (p *Paxos) HandleP1b(m P1b) {
	p.slot = paxi.Max(p.slot, m.Slot)

	e, ok := p.log[m.Slot]
	if !ok {
		log.Fatalf("Receive P1b message with unknown slot %d", m.Slot)
	}

	// old message
	if m.Ballot < e.ballot || e.commit {
		return
	}

	// reject message
	if m.Ballot > e.ballot {
		p.retry(m.Slot, m.Ballot)
		e.command = m.Command
	}

	// ack message
	if m.Ballot == e.ballot && m.Ballot.ID() == p.ID() {
		e.quorum1.ACK(m.ID)
		if p.Q1(e.quorum1) {
			p.P2a(m.Slot)
		}
	}
}

func (p *Paxos) HandleP2a(m P2a) {
	p.slot = paxi.Max(p.slot, m.Slot)

	e, ok := p.log[m.Slot]
	if !ok {
		e = &entry{
			ballot:  m.Ballot,
			command: m.Command,
			commit:  false,
		}
		p.log[m.Slot] = e
	}

	// old message
	if m.Ballot < e.ballot || e.commit {
		return
	}

	// new leader for slot
	if m.Ballot >= e.ballot {
		if e.request != nil && !e.command.Equal(m.Command) {
			p.retry(m.Slot, m.Ballot)
		}
		e.ballot = m.Ballot
		e.command = m.Command
	}

	p.Send(m.Ballot.ID(), P2b{
		Ballot: e.ballot,
		Slot:   m.Slot,
		ID:     p.ID(),
	})
}

func (p *Paxos) HandleP2b(m P2b) {
	p.slot = paxi.Max(p.slot, m.Slot)

	e, ok := p.log[m.Slot]
	// old message
	if !ok || m.Ballot < e.ballot || e.commit {
		return
	}

	// reject message
	if m.Ballot > e.ballot {
		p.retry(m.Slot, m.Ballot)
	}

	// ack message
	if m.Ballot == e.ballot && m.Ballot.ID() == e.ballot.ID() {
		e.quorum2.ACK(m.ID)
		if p.Q2(e.quorum2) {
			e.commit = true
			p.Broadcast(P3{
				Ballot:  m.Ballot,
				Slot:    m.Slot,
				Command: e.command,
			})

			if p.ReplyWhenCommit && e.request != nil {
				e.request.Reply(paxi.Reply{
					Command:   e.command,
					Timestamp: e.request.Timestamp,
				})
			} else {
				p.exec()
			}
		}
	}
}

func (p *Paxos) HandleP3(m P3) {
	p.slot = paxi.Max(p.slot, m.Slot)

	e, exist := p.log[m.Slot]
	if exist {
		if !e.command.Equal(m.Command) && e.request != nil {
			p.retry(m.Slot, m.Ballot)
		}
	} else {
		e = &entry{}
		p.log[m.Slot] = e
	}
	e.ballot = m.Ballot
	e.command = m.Command
	e.commit = true

	if p.ReplyWhenCommit && e.request != nil {
		e.request.Reply(paxi.Reply{
			Command:   e.request.Command,
			Timestamp: e.request.Timestamp,
		})
	} else {
		p.exec()
	}
}

func (p *Paxos) exec() {
	for {
		e, ok := p.log[p.execute]
		if !ok || !e.commit {
			break
		}

		value := p.Execute(e.command)
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
		delete(p.log, p.execute)
		p.execute++
	}
}
