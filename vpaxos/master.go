package vpaxos

import (
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
)

type nodes []paxi.ID

type master struct {
	*Replica
	ballots map[int]paxi.Ballot
	keys    map[paxi.Key]int
}

func newMaster(r *Replica) *master {
	m := &master{
		Replica: r,
		ballots: make(map[int]paxi.Ballot),
		keys:    make(map[paxi.Key]int),
	}
	m.Node.Register(Query{}, m.handleQuery)
	m.Node.Register(Move{}, m.handleMove)
	return m
}

func (m *master) query(k paxi.Key, id paxi.ID) paxi.Ballot {
	z, ok := m.keys[k]
	if !ok {
		z = id.Zone()
		m.keys[k] = z
		_, ok := m.ballots[z]
		if !ok {
			m.ballots[z] = paxi.NewBallot(1, id)
		}
	}
	return m.ballots[z]
}

func (m *master) handleQuery(q Query) {
	log.Debugf("master %v received %v ", m.ID(), q)
	b := m.query(q.Key, q.ID)
	// m.Node.Broadcast(Info{
	// 	Key:    q.Key,
	// 	Ballot: b,
	// })
	m.Node.Send(q.ID, Info{
		Key:    q.Key,
		Ballot: b,
	})
}

func (m *master) handleMove(v Move) {
	log.Debugf("master %v received %v ", m.ID(), v)
	old := m.ballots[m.keys[v.Key]]
	z := v.To.Zone()
	m.keys[v.Key] = z
	b := m.ballots[z]
	b.Next(v.To)
	m.ballots[z] = b
	m.Node.Broadcast(Info{
		Key:       v.Key,
		Ballot:    b,
		OldBallot: old,
	})
	// update local replica
	m.Replica.index[v.Key] = b
	if b.ID() == m.ID() {
		m.Replica.paxos.ballot = b
	}
}
