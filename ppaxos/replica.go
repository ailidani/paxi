package ppaxos

import (
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
)

type Replica struct {
	paxi.Node
	paxi  map[paxi.Key]*PPaxos
	stats map[paxi.Key]*stat
}

func NewReplica(config paxi.Config) *Replica {
	r := new(Replica)
	r.Node = paxi.NewNode(config)
	r.paxi = make(map[paxi.Key]*PPaxos)
	r.stats = make(map[paxi.Key]*stat)

	r.Register(paxi.Request{}, r.handleRequest)
	r.Register(P1a{}, r.handleP1a)
	r.Register(P1b{}, r.handleP1b)
	r.Register(P2a{}, r.handleP2a)
	r.Register(P2b{}, r.hanldeP2b)
	r.Register(LeaderChange{}, r.handleLeaderChange)
	return r
}

func (r *Replica) init(key paxi.Key) {
	if _, exists := r.paxi[key]; !exists {
		r.paxi[key] = NewPPaxos(r, key)
		r.stats[key] = newStat(r.Config().Interval)
	}
}

func (r *Replica) handleRequest(m paxi.Request) {
	log.Debugf("Replica %s received %v\n", r.ID(), m)
	key := m.Command.Key
	r.init(key)

	p := r.paxi[key]
	if p.Config().Adaptive {
		if p.IsLeader() || p.Ballot() == 0 {
			p.handleRequest(m)
			to := r.stats[key].hit(m.Command.ClientID)
			if to != "" && to.Zone() != r.ID().Zone() {
				p.Send(to, &LeaderChange{
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
		p.handleRequest(m)
	}
}

func (r *Replica) handleP1a(m P1a) {
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.Ballot.ID(), m, r.ID())
	r.init(m.Key)
	r.paxi[m.Key].HandleP1a(m)
}

func (r *Replica) handleP1b(m P1b) {
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.ID, m, r.ID())
	r.paxi[m.Key].HandleP1b(m)
}

func (r *Replica) handleP2a(m P2a) {
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.Ballot.ID(), m, r.ID())
	r.init(m.Key)
	r.paxi[m.Key].HandleP2a(m)
}

func (r *Replica) hanldeP2b(m P2b) {
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.ID, m, r.ID())
	r.paxi[m.Key].HandleP2b(m)
}

func (r *Replica) handleLeaderChange(m LeaderChange) {
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.From, m, r.ID())
	if m.To == r.ID() {
		r.paxi[m.Key].p1a()
	}
}
