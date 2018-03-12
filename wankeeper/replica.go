package wankeeper

import (
	"github.com/ailidani/paxi"
)

type entry struct {
	cmd    paxi.Command
	commit bool
	req    *paxi.Request
	quorum *paxi.Quorum
}

type Replica struct {
	paxi.Node

	log       map[paxi.Key]map[int]*entry
	slot      int
	committed int
	executed  int
	tokens    *tokens

	master  paxi.ID
	leaders []paxi.ID

	leader   bool
	ballot   paxi.Ballot     // ballot for local group
	quorum   *paxi.Quorum    // quorum for leader election
	requests []*paxi.Request // pending requests
}

func NewReplica(id paxi.ID) *Replica {
	r := &Replica{
		Node:     paxi.NewNode(id),
		log:      make(map[paxi.Key]map[int]*entry),
		tokens:   newTokens(id),
		master:   paxi.ID("2.3"), // TODO no hardcode
		quorum:   paxi.NewQuorum(),
		requests: make([]*paxi.Request, 0),
	}
	r.Register(paxi.Request{}, r.handleRequest)
	r.Register(NewLeader{}, r.handleNewLeader)
	r.Register(Vote{}, r.handleVote)
	r.Register(Proposal{}, r.handleProposal)
	r.Register(Ack{}, r.handleAck)
	r.Register(Commit{}, r.handleCommit)
	r.Register(Revoke{}, r.handleRevoke)
	r.Register(Token{}, r.handleToken)
	return r
}

func (r *Replica) init(key paxi.Key) {
	if _, exist := r.log[key]; !exist {
		r.log[key] = make(map[int]*entry)
	}
}

func (r *Replica) lead(m ...*paxi.Request) {
	for _, request := range m {
		key := request.Command.Key
		r.init(key)
		if r.tokens.contains(key) {
			r.slot++
			// had token start bcast
			r.log[key][r.slot] = &entry{
				cmd:    request.Command,
				req:    request,
				quorum: paxi.NewQuorum(),
			}
			r.Multicast(r.ID().Zone(), Proposal{
				Ballot:  r.ballot,
				Slot:    len(r.log) - 1,
				Command: request.Command,
			})
		} else if r.ID() != r.master {
			// no token level 1
			go r.Forward(r.master, *request)
		} else {
			// no token level 2
			id := r.tokens.get(key)
			r.Send(id, Revoke{key})
			// add to pending request
			r.requests = append(r.requests, request)
		}
	}
}

func (r *Replica) handleRequest(m paxi.Request) {
	if r.ballot == 0 { // start leader election
		r.ballot = paxi.NewBallot(1, r.ID())
		r.Broadcast(NewLeader{r.ballot})
		r.requests = append(r.requests, &m)
	} else if r.leader {
		// is leader of local group
		r.lead(&m)
	} else if r.ballot.ID() == r.ID() {
		// in leader election phase
		r.requests = append(r.requests, &m)
	} else {
		// follower
		go r.Forward(r.ballot.ID(), m)
	}
}

func (r *Replica) handleNewLeader(m NewLeader) {
	if m.Ballot > r.ballot {
		r.ballot = m.Ballot
		r.leader = false
		r.quorum.Reset()
	}
	r.Send(m.Ballot.ID(), Vote{
		Ballot: r.ballot,
		ID:     r.ID(),
	})
}

func (r *Replica) handleVote(m Vote) {
	if m.Ballot < r.ballot || r.leader {
		return
	}

	if m.Ballot > r.ballot {
		// step down to follower
		r.ballot = m.Ballot
		r.leader = false
		r.quorum.Reset()
	} else if m.Ballot.ID() == r.ID() {
		r.quorum.ACK(m.ID)
		if r.quorum.ZoneMajority() {
			r.leader = true
			r.lead(r.requests...)
		}
	}
}

func (r *Replica) handleProposal(m Proposal) {
	if m.Ballot < r.ballot {
		return
	}

	if m.Ballot > r.ballot {
		r.ballot = m.Ballot
		r.leader = false
	}

	key := m.Command.Key
	r.init(key)
	r.log[key][m.Slot] = &entry{
		cmd: m.Command,
	}
	r.Send(m.Ballot.ID(), Ack{
		Ballot: m.Ballot,
		ID:     r.ID(),
		Slot:   m.Slot,
		Key:    key,
	})
}

func (r *Replica) handleAck(m Ack) {
	if m.Ballot < r.ballot {
		return
	}

	if m.Ballot > r.ballot {
		r.ballot = m.Ballot
		r.leader = false
		return
	}

	key := m.Key

	e := r.log[key][m.Slot]
	if e.commit {
		return
	}

	e.quorum.ACK(m.ID)
	if e.quorum.ZoneMajority() {
		e.commit = true
		c := Commit{
			Ballot:  r.ballot,
			Slot:    m.Slot,
			Command: e.cmd,
		}
		r.Multicast(r.ID().Zone(), c)
		if r.ID() != r.master {
			r.Send(r.master, c)
		} else {
			// TODO broadcast to leaders only?
			r.Broadcast(c)
		}
	}
}

func (r *Replica) handleCommit(m Commit) {
	key := m.Command.Key
	r.init(key)
	if !r.leader {
		// follower
		e := r.log[key][m.Slot]
		if e == nil {
			r.log[key][m.Slot] = &entry{
				cmd:    m.Command,
				commit: true,
			}
			e = r.log[key][m.Slot]
		}
		e.commit = true
		r.tokens.set(m.Command.Key, m.Ballot.ID())
	} else if r.ID() == r.master {
		// master
	} else {
		// leader
	}
}

func (r *Replica) handleRevoke(m Revoke) {

}

func (r *Replica) handleToken(m Token) {

}

// Broadcast overrides Socket interface in Node
func (r *Replica) Broadcast(msg interface{}) {
	r.Node.Multicast(r.ID().Zone(), msg)
}
