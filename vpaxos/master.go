package vpaxos

import (
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
)

type nodes []paxi.ID

type master struct {
	*Replica
	keys map[paxi.Key]paxi.Ballot
}

func newMaster(r *Replica) *master {
	m := &master{
		Replica: r,
		keys:    make(map[paxi.Key]paxi.Ballot),
	}
	m.Node.Register(Query{}, m.handleQuery)
	return m
}

func (m *master) query(k paxi.Key, id paxi.ID) paxi.Ballot {
	b, ok := m.keys[k]
	if !ok {
		b = paxi.NewBallot(1, id)
		m.keys[k] = b
	}
	return b
}

func (m *master) handleQuery(q Query) {
	log.Debugf("master %v received Query %+v ", m.ID(), q)
	b := m.query(q.Key, q.ID)
	m.Node.Broadcast(Info{
		Key:    q.Key,
		Ballot: b,
	})
	// m.Node.Send(q.ID, Info{
	// 	Key:    q.Key,
	// 	Ballot: b,
	// })
}

func (m *master) handleMove(v Move) {
	log.Debugf("master %v received Move %+v ", m.ID(), v)
	b := m.keys[v.Key]
	if b.ID() == v.From && b == v.OldBallot {
		b.Next(v.To)
		m.keys[v.Key] = b
		v.NewBallot = b
		m.Node.Broadcast(v)
		// update local replica
		m.Replica.index[v.Key] = b
	}
}
