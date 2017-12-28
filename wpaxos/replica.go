package wpaxos

import (
	. "github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
)

type Replica struct {
	Node
	paxi map[Key]*paxos
}

func NewReplica(config Config) *Replica {
	r := new(Replica)
	r.Node = NewNode(config)
	r.paxi = make(map[Key]*paxos)

	r.Register(Request{}, r.handleRequest)
	r.Register(Prepare{}, r.handlePrepare)
	r.Register(Promise{}, r.handlePromise)
	r.Register(Accept{}, r.handleAccept)
	r.Register(Accepted{}, r.handleAccepted)
	r.Register(Commit{}, r.handleCommit)
	r.Register(LeaderChange{}, r.handleLeaderChange)
	return r
}

func (r *Replica) init(key Key) {
	if _, exists := r.paxi[key]; !exists {
		r.paxi[key] = NewPaxos(r, key)
	}
}

func (r *Replica) handleRequest(msg Request) {
	log.Debugf("Replica %s received %v\n", r.ID, msg)
	key := msg.Command.Key
	r.init(key)
	r.paxi[key].handleRequest(msg)
}

func (r *Replica) handlePrepare(msg Prepare) {
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", LeaderID(msg.Ballot), msg, r.ID)
	key := msg.Key
	r.init(key)
	r.paxi[key].handlePrepare(msg)
}

func (r *Replica) handlePromise(msg Promise) {
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", msg.ID, msg, r.ID)
	key := msg.Key
	r.paxi[key].handlePromise(msg)
	log.Debugf("Number of keys: %d", r.keys())
}

func (r *Replica) handleAccept(msg Accept) {
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", LeaderID(msg.Ballot), msg, r.ID)
	key := msg.Key
	r.init(key)
	r.paxi[key].handleAccept(msg)
}

func (r *Replica) handleAccepted(msg Accepted) {
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", msg.ID, msg, r.ID)
	key := msg.Key
	r.paxi[key].handleAccepted(msg)
}

func (r *Replica) handleCommit(msg Commit) {
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", LeaderID(msg.Ballot), msg, r.ID)
	key := msg.Key
	r.init(key)
	r.paxi[key].handleCommit(msg)
}

func (r *Replica) handleLeaderChange(msg LeaderChange) {
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", msg.From, msg, r.ID)
	key := msg.Key
	r.paxi[key].handleLeaderChange(msg)
}

func (r *Replica) keys() int {
	sum := 0
	for _, paxos := range r.paxi {
		if paxos.active {
			sum++
		}
	}
	return sum
}
