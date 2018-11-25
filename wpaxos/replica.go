package wpaxos

import (
	"flag"

	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
)

var adaptive = flag.Bool("adaptive", true, "stable leader, if true paxos forward request to current leader")
var fz = flag.Int("fz", 0, "f_z fault tolerent zones")

// Replica is WPaxos replica node
type Replica struct {
	paxi.Node
	paxi map[paxi.Key]*kpaxos
}

// NewReplica create new Replica instance
func NewReplica(id paxi.ID) *Replica {
	r := new(Replica)
	r.Node = paxi.NewNode(id)
	r.paxi = make(map[paxi.Key]*kpaxos)

	r.Register(paxi.Request{}, r.handleRequest)
	r.Register(paxi.Transaction{}, r.handleTransaction)
	r.Register(Prepare{}, r.handlePrepare)
	r.Register(Promise{}, r.handlePromise)
	r.Register(Accept{}, r.handleAccept)
	r.Register(Accepted{}, r.handleAccepted)
	r.Register(Commit{}, r.handleCommit)
	r.Register(LeaderChange{}, r.handleLeaderChange)
	return r
}

func (r *Replica) init(key paxi.Key) {
	if _, exists := r.paxi[key]; !exists {
		r.paxi[key] = newKPaxos(key, r.Node)
	}
}

func (r *Replica) handleRequest(m paxi.Request) {
	log.Debugf("Replica %s received %v\n", r.ID(), m)
	key := m.Command.Key
	r.init(key)

	p := r.paxi[key]
	if *adaptive {
		if p.IsLeader() || p.Ballot() == 0 {
			p.HandleRequest(m)
			to := p.Hit(m.NodeID)
			if to != "" && to.Zone() != r.ID().Zone() {
				p.Send(to, LeaderChange{
					Key:    key,
					To:     to,
					From:   r.ID(),
					Ballot: p.Ballot(),
				})
			}
		} else {
			go r.Forward(p.Leader(), m)
		}
	} else {
		p.HandleRequest(m)
	}
}

func (r *Replica) handleTransaction(m paxi.Transaction) {
	// TODO
}

func (r *Replica) handlePrepare(m Prepare) {
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.Ballot.ID(), m, r.ID())
	r.init(m.Key)
	r.paxi[m.Key].HandleP1a(m.P1a)
}

func (r *Replica) handlePromise(m Promise) {
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.ID, m, r.ID())
	r.paxi[m.Key].HandleP1b(m.P1b)
	// log.Debugf("Number of keys: %d", r.keys())
}

func (r *Replica) handleAccept(m Accept) {
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.Ballot.ID(), m, r.ID())
	r.init(m.Key)
	r.paxi[m.Key].HandleP2a(m.P2a)
}

func (r *Replica) handleAccepted(m Accepted) {
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.ID, m, r.ID())
	r.paxi[m.Key].HandleP2b(m.P2b)
}

func (r *Replica) handleCommit(m Commit) {
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.Ballot.ID(), m, r.ID())
	r.init(m.Key)
	r.paxi[m.Key].HandleP3(m.P3)
}

func (r *Replica) handleLeaderChange(m LeaderChange) {
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.From, m, r.ID())
	p := r.paxi[m.Key]
	if m.Ballot == p.Ballot() && m.To == r.ID() {
		// log.Debugf("Replica %s : change leader of key %d\n", r.ID(), m.Key)
		p.P1a()
	}
}

func (r *Replica) keys() int {
	sum := 0
	for _, p := range r.paxi {
		if p.IsLeader() {
			sum++
		}
	}
	return sum
}
