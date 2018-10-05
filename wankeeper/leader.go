package wankeeper

import (
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
)

type leader struct {
	*Replica

	zone     int
	masterID paxi.ID
	master   *master
	active   bool
	quorum   *paxi.Quorum // quorum for leader election
}

func newLeader(r *Replica) *leader {
	l := &leader{
		Replica:  r,
		zone:     r.ID().Zone(),
		masterID: mid,
		active:   false,
		quorum:   paxi.NewQuorum(),
	}
	l.quorum.ACK(l.ID())
	if l.masterID == l.ID() {
		log.Debugf("leader %s create master", l.ID())
		l.master = newMaster(l)
	}
	l.Register(Vote{}, l.handleVote)
	l.Register(Ack{}, l.handleAck)
	l.Register(Revoke{}, l.handleRevoke)
	l.Register(Token{}, l.handleToken)
	return l
}

func (l *leader) forward(r *paxi.Request) {
	go l.Forward(l.masterID, *r)
}

func (l *leader) lead(requests ...*paxi.Request) {
	for _, r := range requests {
		key := r.Command.Key
		l.init(key)
		if l.tokens.contains(key) {
			l.slot[key]++
			l.log[key][l.slot[key]] = &entry{
				cmd:    r.Command,
				req:    r,
				quorum: paxi.NewQuorum(),
			}
			l.log[key][l.slot[key]].quorum.ACK(l.ID())
			l.MulticastZone(l.zone, Proposal{
				Ballot:  l.ballot,
				Slot:    l.slot[key],
				Command: r.Command,
			})
		} else if l.master == nil {
			l.forward(r)
		} else {
			l.master.lead(r)
		}
	}
}

func (l *leader) handleVote(m Vote) {
	if m.Ballot < l.ballot || l.active {
		return
	}

	if m.Ballot > l.ballot {
		// step down to follower
		l.active = false
		l.quorum.Reset()
		l.ballot = m.Ballot
		return
	}

	if m.Ballot.ID() == l.ID() {
		l.quorum.ACK(m.ID)
		if l.quorum.ZoneMajority() {
			l.active = true
			l.MulticastZone(l.ID().Zone(), NewLeader{l.ballot})
			l.Send(l.masterID, NewLeader{l.ballot})
			l.lead(l.requests...)
		}
	}
}

func (l *leader) handleAck(m Ack) {
	if m.Ballot < l.ballot {
		return
	}

	if m.Ballot > l.ballot {
		l.ballot = m.Ballot
		l.active = false
		return
	}

	e := l.log[m.Key][m.Slot]
	if e.commit {
		return
	}

	log.Debugf("leader %s received Ack %v", l.ID(), m)

	e.quorum.ACK(m.ID)
	if e.quorum.ZoneMajority() {
		e.commit = true
		c := Commit{
			Ballot:  l.ballot,
			Slot:    m.Slot,
			Command: e.cmd,
		}
		l.Replica.exec(m.Key)
		l.MulticastZone(l.zone, c)
		if l.master == nil {
			l.Send(l.masterID, c)
		} else {
			l.master.handleCommit(c)
		}
	}
}

func (l *leader) handleRevoke(m Revoke) {
	log.Debugf("leader %s received revoke %v", l.ID(), m.Key)
	if l.tokens.contains(m.Key) {
		l.tokens.set(m.Key, l.masterID)
		l.Send(l.masterID, Token{m.Key})
	} else {
		log.Errorf("leader %v does not have token %v", l.ID(), m.Key)
	}
}

func (l *leader) handleToken(m Token) {
	log.Debugf("leader %s receives token %v", l.ID(), m.Key)
	l.tokens.set(m.Key, l.ID())

	// master
	if l.master != nil {
		l.master.handleToken(m)
	}
}

func (l *leader) handleCommit(m Commit) {
	l.MulticastZone(l.zone, m)

	// master
	if l.master != nil {
		l.master.handleCommit(m)
	}
}
