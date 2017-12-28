package paxos

import (
	"time"

	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
)

type entry struct {
	ballot    int
	cmd       paxi.Command
	commit    bool
	request   *paxi.Request
	quorum    *paxi.Quorum
	timestamp time.Time
}

// Paxos instance
type Paxos struct {
	paxi.Node

	leader  bool           // am i leader for this ballot
	ballot  int            // ballot number of <n, id>
	slot    int            // max slot number
	log     map[int]*entry // entire log ordered by slot
	execute int            // next execute slot number

	quorum   *paxi.Quorum   // quorum for phase 1
	requests []paxi.Request // pending requests during phase 1
}

// NewPaxos creates new paxos instance
func NewPaxos(n paxi.Node) *Paxos {
	p := new(Paxos)
	p.Node = n
	p.log = make(map[int]*entry, paxi.BUFFER_SIZE)
	p.log[0] = &entry{}
	p.execute = 1
	p.quorum = paxi.NewQuorum()
	p.requests = make([]paxi.Request, 0)
	return p
}

func (p *Paxos) HandleRequest(r paxi.Request) {
	log.Debugf("Replica %s received %v\n", p.ID(), r)
	if !p.leader {
		p.requests = append(p.requests, r)
		if paxi.LeaderID(p.ballot) != p.ID() {
			p.P1a()
		}
	} else {
		p.P2a(r)
	}
}

// P1a starts phase 1 prepare
func (p *Paxos) P1a() {
	p.ballot = paxi.NextBallot(p.ballot, p.ID())
	p.quorum.ACK(p.ID())
	p.Broadcast(&P1a{Ballot: p.ballot})
}

// P2a starts phase 2 accept
func (p *Paxos) P2a(r paxi.Request) {
	p.slot++
	p.log[p.slot] = &entry{
		ballot:    p.ballot,
		cmd:       r.Command,
		request:   &r,
		quorum:    paxi.NewQuorum(),
		timestamp: time.Now(),
	}
	p.log[p.slot].quorum.ACK(p.ID())
	p.Broadcast(&P2a{
		Ballot:  p.ballot,
		Slot:    p.slot,
		Command: r.Command,
	})
}

func (p *Paxos) HandleP1a(m P1a) {
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", paxi.LeaderID(m.Ballot), m, p.ID())
	if m.Ballot > p.ballot {
		p.ballot = m.Ballot
		p.leader = false
	}
	p.Send(paxi.LeaderID(m.Ballot), &P1b{
		Ballot:  m.Ballot,
		ID:      p.ID(),
		Slot:    p.slot,
		Command: p.log[p.slot].cmd,
	})
}

// HandleP1b handles p1b message
func (p *Paxos) HandleP1b(m P1b) {
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.ID, m, p.ID())
	if !p.leader && m.Ballot == p.ballot {
		// TODO update command
		p.slot = paxi.Max(p.slot, m.Slot)
		p.quorum.ACK(m.ID)
		if p.quorum.Majority() {
			p.leader = true
			for _, req := range p.requests {
				p.P2a(req)
			}
			p.requests = make([]paxi.Request, 0)
		}
	}
}

func (p *Paxos) HandleP2a(m P2a) {
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", paxi.LeaderID(m.Ballot), m, p.ID())
	if m.Ballot >= p.ballot {
		p.ballot = m.Ballot
		p.leader = false
		p.slot = paxi.Max(p.slot, m.Slot)

		p.log[m.Slot] = &entry{
			ballot: m.Ballot,
			cmd:    m.Command,
		}
	}

	p.Send(paxi.LeaderID(m.Ballot), &P2b{
		Ballot: p.ballot,
		Slot:   m.Slot,
		ID:     p.ID(),
	})
}

func (p *Paxos) HandleP2b(m P2b) {
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.ID, m, p.ID())
	if !p.log[m.Slot].commit && m.Ballot == p.ballot {
		entry := p.log[m.Slot]
		entry.quorum.ACK(m.ID)
		if entry.quorum.Majority() {
			entry.commit = true
			p.Broadcast(&P3{
				Ballot:  p.ballot,
				Slot:    m.Slot,
				Command: entry.cmd,
			})

			// TODO reply when commit??
			p.exec()
		}
	}

	if m.Ballot > p.ballot {
		p.ballot = m.Ballot
		p.leader = false
		// TODO retry the entry
	}
}

func (p *Paxos) HandleP3(m P3) {
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", paxi.LeaderID(m.Ballot), m, p.ID())
	// is this needed?
	if m.Ballot > p.ballot {
		p.ballot = m.Ballot
	}

	p.slot = paxi.Max(p.slot, m.Slot)

	if p.log[m.Slot] == nil {
		p.log[m.Slot] = &entry{
			cmd:    m.Command,
			ballot: m.Ballot,
			commit: true,
		}
	} else {
		p.log[m.Slot].commit = true
	}

	p.exec()
}

func (p *Paxos) exec() {
	for {
		i, ok := p.log[p.execute]
		if !ok || !i.commit {
			break
		}

		log.Debugf("Replica %s execute cmd=%v in slot %d ", p.ID(), i.cmd, p.execute)
		value, err := p.Execute(i.cmd)
		p.execute++

		if i.request != nil {
			i.request.Command.Value = value
			i.request.Reply(paxi.Reply{
				ClientID:  i.request.ClientID,
				CommandID: i.request.CommandID,
				Command:   i.request.Command,
				Err:       err,
			})
		}
	}
}
