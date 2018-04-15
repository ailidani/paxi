package vpaxos

import "github.com/ailidani/paxi"

type nodes []paxi.ID

type master struct {
	paxi.Node
	keys    map[paxi.Key]int    // key -> gid
	ballots map[int]paxi.Ballot // gid -> ballot
	configs map[int]nodes
}

func newMaster(node paxi.Node) *master {
	m := &master{
		Node:    node,
		keys:    make(map[paxi.Key]int),
		ballots: make(map[int]paxi.Ballot),
		configs: make(map[int]nodes),
	}
	m.Node.Register(Query{}, m.handleQuery)
	return m
}

func (m *master) create(k paxi.Key) {
	gid := m.Node.ID().Zone()
	m.keys[k] = gid
}

func (m *master) handleQuery(q Query) {
	gid, ok := m.keys[q.Key]
	if !ok {
		gid = q.ID.Zone()
		m.keys[q.Key] = gid
		m.ballots[gid] = q.Ballot
	}
	m.Node.Send(q.ID, Info{
		Key:     q.Key,
		GroupID: m.keys[q.Key],
		Ballot:  m.ballots[gid],
	})
}
