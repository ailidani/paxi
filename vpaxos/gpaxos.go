package vpaxos

import (
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/paxos"
)

type gpaxos struct {
	paxi.Node
	gid int
	*paxos.Paxos
}

func newGPaxos(gid int, node paxi.Node) *gpaxos {
	g := &gpaxos{
		Node: node,
		gid:  gid,
	}
	g.Paxos = paxos.NewPaxos(g)
	return g
}

func (g *gpaxos) Broadcast(m interface{}) {
	switch m := m.(type) {
	case paxos.P1a:
		g.Node.Multicast(g.gid, Prepare{g.gid, m})
	case paxos.P2a:
		g.Node.Multicast(g.gid, Accept{g.gid, m})
	case paxos.P3:
		g.Node.Multicast(g.gid, Commit{g.gid, m})
	default:
		g.Node.Multicast(g.gid, m)
	}
}

func (g *gpaxos) Send(to paxi.ID, m interface{}) {
	switch m := m.(type) {
	case paxos.P1b:
		g.Node.Send(to, Promise{g.gid, m})
	case paxos.P2b:
		g.Node.Send(to, Accepted{g.gid, m})
	default:
		g.Node.Send(to, m)
	}
}
