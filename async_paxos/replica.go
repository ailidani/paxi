package async_paxos

import (
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
)

type Replica struct {
	paxi.Node
	paxi  map[paxi.Key]*Paxos
	stats map[paxi.Key]*stat

	key paxi.Key // current working key
}

func NewReplica(config paxi.Config) *Replica {
	r := new(Replica)
	r.Node = paxi.NewNode(config)
	r.paxi = make(map[paxi.Key]*Paxos)
	r.stats = make(map[paxi.Key]*stat)

	r.Register(paxi.Request{}, r.handleRequest)
	r.Register(Prepare{}, r.handlePrepare)
	r.Register(Promise{}, r.handlePromise)
	r.Register(Accept{}, r.handleAccept)
	r.Register(Accepted{}, r.handleAccepted)
	r.Register(LeaderChange{}, r.handleLeaderChange)
	return r
}

func (r *Replica) init(key paxi.Key) {
	if _, exists := r.paxi[key]; !exists {
		r.paxi[key] = NewPaxos(r)
		r.stats[key] = newStat(r.Config().Interval)
	}
}

func (r *Replica) handleRequest(m paxi.Request) {
	log.Debugf("Replica %s received %v\n", r.ID(), m)
	r.key = m.Command.Key
	r.init(r.key)

	p := r.paxi[r.key]
	if p.Config().Adaptive {
		if p.IsLeader() || p.Ballot() == 0 {
			p.HandleRequest(m)
			to := r.stats[r.key].hit(m.Command.ClientID)
			if p.Config().Adaptive && to != "" && to.Zone() != r.ID().Zone() {
				p.Send(to, &LeaderChange{
					Key:    r.key,
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

func (r *Replica) handlePrepare(m Prepare) {
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.Ballot.ID(), m, r.ID())
	r.key = m.Key
	r.init(r.key)
	r.paxi[r.key].HandleP1a(m.P1a)
}

func (r *Replica) handlePromise(m Promise) {
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.ID, m, r.ID())
	r.key = m.Key
	r.paxi[r.key].HandleP1b(m.P1b)
	// log.Debugf("Number of keys: %d", r.keys())
}

func (r *Replica) handleAccept(m Accept) {
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.Ballot.ID(), m, r.ID())
	r.key = m.Key
	r.init(r.key)
	r.paxi[r.key].HandleP2a(m.P2a)
}

func (r *Replica) handleAccepted(m Accepted) {
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.ID, m, r.ID())
	r.key = m.Key
	r.paxi[r.key].HandleP2b(m.P2b)
}

func (r *Replica) handleLeaderChange(m LeaderChange) {
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.From, m, r.ID())
	r.key = m.Key
	if m.To == r.ID() {
		log.Debugf("Replica %s : change leader of key %d\n", r.ID(), r.key)
		r.paxi[r.key].P1a()
	}
}

// Broadcast overrides Socket interface in Node
func (r *Replica) Broadcast(msg interface{}) {
	switch m := msg.(type) {
	case *P1a:
		r.Node.Broadcast(&Prepare{r.key, *m})
	case *P2a:
		r.Node.Broadcast(&Accept{r.key, *m})
	default:
		r.Node.Broadcast(msg)
	}
}

// Send overrides Socket interface in Node
func (r *Replica) Send(to paxi.ID, msg interface{}) {
	switch m := msg.(type) {
	case *P1b:
		r.Node.Send(to, &Promise{r.key, *m})
	case *P2b:
		r.Node.Send(to, &Accepted{r.key, *m})
	default:
		r.Node.Send(to, msg)
	}
}
