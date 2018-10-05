package wankeeper

import (
	"flag"

	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
)

// master node id
var mid = paxi.ID(*flag.String("wankeeper_mid", "2.1", "Master ID in format of Zone.Node"))

type entry struct {
	cmd    paxi.Command
	commit bool
	req    *paxi.Request
	quorum *paxi.Quorum
}

type Replica struct {
	paxi.Node

	log      map[paxi.Key]map[int]*entry
	slot     map[paxi.Key]int
	executed map[paxi.Key]int
	tokens   *tokens

	leader   *leader
	ballot   paxi.Ballot     // ballot for local group
	requests []*paxi.Request // pending requests
}

// NewReplica creates new replica object for wankeeper
func NewReplica(id paxi.ID) *Replica {
	log.Debug("creating replica for wankeeper")
	r := &Replica{
		Node:     paxi.NewNode(id),
		log:      make(map[paxi.Key]map[int]*entry),
		slot:     make(map[paxi.Key]int),
		executed: make(map[paxi.Key]int),
		tokens:   newTokens(id),
		requests: make([]*paxi.Request, 0),
	}
	r.Register(paxi.Request{}, r.handleRequest)
	r.Register(NewLeader{}, r.handleNewLeader)
	r.Register(NewBallot{}, r.handleNewBallot)
	r.Register(Proposal{}, r.handleProposal)
	r.Register(Commit{}, r.handleCommit)

	return r
}

// Run overrides Node.Run() function to start leader election
func (r *Replica) Run() {
	defer r.Node.Run()
	min := r.ID()
	for id := range paxi.GetConfig().Addrs {
		if id.Zone() == min.Zone() && id.Node() < min.Node() {
			min = id
		}
	}
	r.ballot.Next(min)
	if min != r.ID() {
		log.Debugf("replica %s voted for %s", r.ID(), min)
		r.Send(min, Vote{
			Ballot: r.ballot,
			ID:     r.ID(),
		})
	} else {
		log.Debugf("replica %s create new leader", r.ID())
		r.leader = newLeader(r)
	}
	// r.ballot.Next(r.ID())
	// r.MulticastZone(r.ID().Zone(), NewBallot{r.ballot})
}

func (r *Replica) handleNewLeader(m NewLeader) {
	if m.Ballot.ID().Zone() != r.ID().Zone() {
		if r.leader != nil && r.leader.master != nil {
			r.leader.master.handleNewLeader(m)
		}
		return
	}
	if m.Ballot > r.ballot {
		r.ballot = m.Ballot
		r.leader = nil
	}
}

func (r *Replica) handleNewBallot(m NewBallot) {
	if m.Ballot > r.ballot {
		r.ballot = m.Ballot
		r.leader = nil
	}
	r.Send(m.Ballot.ID(), Vote{
		Ballot: r.ballot,
		ID:     r.ID(),
	})
}

func (r *Replica) init(key paxi.Key) {
	if _, exist := r.log[key]; !exist {
		r.log[key] = make(map[int]*entry)
		r.executed[key] = 1
	}
}

func (r *Replica) handleRequest(m paxi.Request) {
	log.Debugf("replica %s received %v", r.ID(), m)
	if r.ballot == 0 {
		// wait leader election
		r.requests = append(r.requests, &m)
	} else if r.leader != nil {
		// is leader of local group
		if r.leader.active {
			r.leader.lead(&m)
		} else {
			r.requests = append(r.requests, &m)
		}
	} else {
		// follower
		go r.Forward(r.ballot.ID(), m)
	}
}

func (r *Replica) handleProposal(m Proposal) {
	if m.Ballot < r.ballot {
		return
	}

	log.Debugf("replica %s received %v", r.ID(), m)

	if m.Ballot > r.ballot {
		r.ballot = m.Ballot
		r.leader = nil
	}

	key := m.Command.Key
	r.init(key)
	r.slot[key] = paxi.Max(r.slot[key], m.Slot)
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

func (r *Replica) handleCommit(m Commit) {
	log.Debugf("replica %s received %v", r.ID(), m)
	key := m.Command.Key
	r.init(key)
	r.slot[key] = paxi.Max(r.slot[key], m.Slot)
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
	// r.tokens.set(m.Command.Key, m.Ballot.ID())

	r.exec(key)

	// leader
	if r.leader != nil {
		r.leader.handleCommit(m)
	}
}

func (r *Replica) exec(key paxi.Key) {
	for {
		e, exist := r.log[key][r.executed[key]]
		if !exist || !e.commit {
			break
		}

		log.Debugf("replica %s execute [s=%d, cmd=%v]", r.ID(), r.executed[key], e.cmd)
		value := r.Execute(e.cmd)
		r.executed[key]++

		if e.req != nil {
			e.req.Reply(paxi.Reply{
				Command: e.cmd,
				Value:   value,
			})
			e.req = nil
		}
	}
}

// Broadcast overrides Socket interface in Node
// func (r *Replica) Broadcast(msg interface{}) {
// 	r.Node.Multicast(r.ID().Zone(), msg)
// }
