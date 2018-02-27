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

	leader   bool
	ballot   paxi.Ballot     // ballot for local group
	level    int             // current levels are 1 and 2
	requests []*paxi.Request // pending requests
}

func NewReplica(id paxi.ID) *Replica {
	r := &Replica{
		Node:     paxi.NewNode(id),
		log:      make([]*entry, 0),
		tokens:   newTokens(),
		level:    1,
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

func (r *Replica) handleRequest(m paxi.Request) {
	if r.ballot == 0 {
		r.ballot = paxi.NewBallot(1, r.ID())
		r.Broadcast(NewLeader{r.ballot})
		r.requests = append(r.requests, &m)
	} else if r.leader {
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
	}
}

func (r *Replica) handleNewLeader(m NewLeader) {
}

func (r *Replica) handleVote(m Vote) {
}

func (r *Replica) handleAccept(m Accept) {

}

// Broadcast overrides Socket interface in Node
func (r *Replica) Broadcast(msg interface{}) {
	r.Node.Multicast(r.ID().Zone(), msg)
}
