package kpaxos

import (
	"math/rand"
	. "paxi"
	"paxi/glog"
	"time"
)

type instance struct {
	ballot    int
	command   Command
	committed bool
	request   *Request
	quorum    *Quorum
	timestamp time.Time
}

type paxos struct {
	*Node

	key      Key
	active   bool
	ballot   int
	quorum   *Quorum   // quorum for phase 1
	requests []Request // pending requests in phase 1
	sleeping bool

	cmds   map[int]*instance
	slot   int
	commit int
}

func NewPaxos(node *Node, key Key) *paxos {
	return &paxos{
		Node:     node,
		key:      key,
		active:   false,
		ballot:   0,
		requests: make([]Request, 0),
		cmds:     make(map[int]*instance),
		slot:     0,
		commit:   0,
	}
}

// phase 1
func (p *paxos) prepare() {
	if p.active == false {
		p.NextBallot()
		p.quorum = NewQuorum()
		p.quorum.ACK(p.ID)
		p.Broadcast(&Prepare{
			Key:    p.key,
			Ballot: p.ballot,
		})
		glog.V(2).Infof("Replica %v sent %v", p.ID, Prepare{p.key, p.ballot})
	}
}

// phase 2
func (p *paxos) accept(msg Request) {
	p.slot++
	p.cmds[p.slot] = &instance{
		ballot:    p.ballot,
		command:   msg.Command,
		committed: false,
		request:   &msg,
		quorum:    NewQuorum(),
		timestamp: time.Now(),
	}
	p.cmds[p.slot].quorum.ACK(p.ID)
	p.Multicast(&Accept{
		Key:     p.key,
		Ballot:  p.ballot,
		Slot:    p.slot,
		Command: msg.Command,
	})
}

func (p *paxos) handleRequest(msg Request) {
	if p.active {
		p.accept(msg)
	} else {
		// p.Forward(LeaderID(p.ballot), msg)
		p.ReplyChan <- Reply{
			OK:        false,
			CommandID: msg.CommandID,
			LeaderID:  LeaderID(p.ballot),
			ClientID:  msg.ClientID,
			Command:   msg.Command,
			Timestamp: msg.Timestamp,
		}
	}
}

func (p *paxos) handlePrepare(msg Prepare) {
	if msg.Ballot > p.ballot {
		p.ballot = msg.Ballot
		p.active = false
		if len(p.requests) > 0 && !p.sleeping {
			// p.prepare()
			p.sleeping = true
			go func() {
				time.Sleep(time.Millisecond * time.Duration(rand.Intn(100)+p.BackOff))
				p.prepare()
				p.sleeping = false
			}()
		}
	}

	p.Send(LeaderID(msg.Ballot), &Promise{
		Key:     p.key,
		ID:      p.ID,
		Ballot:  p.ballot,
		PreSlot: p.slot,
	})
}

func (p *paxos) handlePromise(msg Promise) {
	if msg.Ballot < p.ballot || p.active {
		glog.V(2).Infof("Replica %s Ignoring old Promise msg %v\n", p.ID, msg)
		return
	}

	// TODO update commands too not just slot
	if msg.PreSlot > p.slot {
		p.slot = msg.PreSlot
	}

	if msg.Ballot == p.ballot && LeaderID(msg.Ballot) == p.ID {
		p.quorum.ACK(msg.ID)
		if p.quorum.Q1() {
			p.active = true
			for _, req := range p.requests {
				p.accept(req)
			}
			p.requests = make([]Request, 0)
		}
	} else {
		p.ballot = msg.Ballot
		p.active = false
		// p.prepare()
		if !p.sleeping {
			p.sleeping = true
			go func() {
				time.Sleep(time.Millisecond * time.Duration(rand.Intn(100)+p.BackOff))
				p.prepare()
				p.sleeping = false
			}()
		}

	}
}

func (p *paxos) handleAccept(msg Accept) {
	if msg.Ballot >= p.ballot {
		p.ballot = msg.Ballot
		p.active = false
		p.slot = Max(p.slot, msg.Slot)

		ins, exists := p.cmds[msg.Slot]
		if exists && ins.request != nil {
			p.RequestChan <- *p.cmds[msg.Slot].request
		}
		p.cmds[msg.Slot] = &instance{
			ballot:    msg.Ballot,
			command:   msg.Command,
			committed: false,
		}
	}

	p.Send(LeaderID(msg.Ballot), &Accepted{
		Key:    p.key,
		ID:     p.ID,
		Ballot: p.ballot,
		Slot:   msg.Slot,
	})
}

func (p *paxos) handleAccepted(msg Accepted) {
	ins := p.cmds[msg.Slot]
	if ins == nil {
		glog.Warningf("Unknown Accepted msg %v\n", msg)
		return
	}

	if ins.committed || msg.Ballot < ins.ballot {
		glog.V(2).Infof("Ignore old Accepted msg %v\n", msg)
		return
	}

	if msg.Ballot == ins.ballot {
		ins.quorum.ACK(msg.ID)

		if ins.quorum.Q2() {
			ins.committed = true
			for p.cmds[p.commit+1] != nil && p.cmds[p.commit+1].committed {
				p.commit++
			}
			p.Broadcast(&Commit{
				Key:     p.key,
				Ballot:  ins.ballot,
				Slot:    msg.Slot,
				Command: ins.command,
			})
			p.ReplyChan <- Reply{
				OK:        true,
				CommandID: ins.request.CommandID,
				LeaderID:  p.ID,
				ClientID:  ins.request.ClientID,
				Command:   ins.request.Command,
				Timestamp: ins.request.Timestamp,
			}
		}
	} else {
		glog.Warningf("Replica %s put cmd %v in slot=%d back to queue.\n", p.ID, ins.request.Command, msg.Slot)
		p.RequestChan <- *ins.request
		delete(p.cmds, msg.Slot)
	}

	if msg.Ballot > p.ballot {
		p.ballot = msg.Ballot
		p.active = false
	}
}

func (p *paxos) handleCommit(msg Commit) {
	if msg.Ballot > p.ballot {
		p.ballot = msg.Ballot
	}
	if msg.Slot > p.slot {
		p.slot = msg.Slot
	}

	if p.cmds[msg.Slot] == nil {
		p.cmds[msg.Slot] = &instance{
			command:   msg.Command,
			ballot:    msg.Ballot,
			committed: true,
		}
	} else {
		p.cmds[msg.Slot].committed = true
	}

	for p.cmds[p.commit+1] != nil && p.cmds[p.commit+1].committed {
		p.commit++
	}
}

func (p *paxos) NextBallot() {
	p.ballot = NextBallot(p.ballot, p.ID)
}
