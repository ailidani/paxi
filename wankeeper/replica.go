package wankeeper

import (
	"github.com/ailidani/paxi"
)

type entry struct {
	cmd    paxi.Command
	req    *paxi.Request
	quorum *paxi.Quorum
}

type Replica struct {
	paxi.Node

	log    []*entry
	tokens *tokens

	master paxi.ID

	leader   bool
	ballot   paxi.Ballot     // ballot for local group
	level    int             // current levels are 1 and 2
	quorum   *paxi.Quorum    // quorum for leader election
	requests []*paxi.Request // pending requests
}

func NewReplica(id paxi.ID) *Replica {
	r := &Replica{
		Node:     paxi.NewNode(id),
		log:      make([]*entry, 0),
		tokens:   newTokens(),
		master:   paxi.ID("1.3"),
		level:    1,
		quorum:   paxi.NewQuorum(),
		requests: make([]*paxi.Request, 0),
	}
	if id.Zone() == 1 {
		r.level = 2
	}
	r.Register(paxi.Request{}, r.handleRequest)
	r.Register(NewLeader{}, r.handleNewLeader)
	r.Register(Vote{}, r.handleVote)
	return r
}

func (r *Replica) lead(m ...*paxi.Request) {
	for _, request := range m {
		if r.tokens.contains(request.Command.Key) {
			r.log = append(r.log, &entry{
				cmd:    request.Command,
				req:    request,
				quorum: paxi.NewQuorum(),
			})
			r.Multicast(r.ID().Zone(), Accept{
				Ballot:  r.ballot,
				Slot:    len(r.log) - 1,
				Command: request.Command,
			})
		} else if r.level == 1 {
			go r.Forward(r.master, *request)
		} else {

		}
	}
}

func (r *Replica) handleRequest(m paxi.Request) {
	if r.ballot == 0 { // start leader election
		r.ballot = paxi.NewBallot(1, r.ID())
		r.Broadcast(NewLeader{r.ballot})
		r.requests = append(r.requests, &m)
	} else if r.leader { // is leader of local group
		r.lead(&m)
		r.log = append(r.log, &entry{
			cmd:    m.Command,
			req:    &m,
			quorum: paxi.NewQuorum(),
		})

		r.Broadcast(Accept{
			Ballot:  r.ballot,
			Slot:    len(r.log),
			Command: m.Command,
		})
	} else if r.ballot.ID() == r.ID() { // in leader election phase
		r.requests = append(r.requests, &m)
	} else { // follower
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

func (r *Replica) handleAccept(m Accept) {

}

// Broadcast overrides Socket interface in Node
func (r *Replica) Broadcast(msg interface{}) {
	r.Node.Multicast(r.ID().Zone(), msg)
}
