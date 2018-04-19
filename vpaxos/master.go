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

func (m *master) create(k paxi.Key) {
	b := paxi.NewBallot(1, m.Node.ID())
	m.keys[k] = b
}

func (m *master) handleQuery(q Query) {
	log.Debugf("master %v received Query %+v ", m.ID(), q)
	b, ok := m.keys[q.Key]
	if !ok {
		b = paxi.NewBallot(1, q.ID)
		m.keys[q.Key] = b
	}
	m.Node.Send(q.ID, Info{
		Key:    q.Key,
		Ballot: m.keys[q.Key],
	})
}

func (m *master) handleMove(v Move) {
	log.Debugf("master %v received Move %+v ", m.ID(), v)
	b := m.keys[v.Key]
	if b.ID() == v.From && b == v.OldBallot {
		b.Next(v.To)
		m.keys[v.Key] = b
		v.NewBallot = b
		m.Node.Broadcast(v)
	}
}
