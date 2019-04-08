package paxos

import (
	"flag"
	"strconv"
	"time"

	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
)

var stable = flag.Bool("stable", true, "stable leader, if true paxos forward request to current leader")
var ReadQuorum = flag.Bool("read_quorum", false, "read from quorum of replicas")
var ReadLeader = flag.Bool("read_leader", false, "read from leader of current ballot")

// Replica for one Paxos instance
type Replica struct {
	paxi.Node
	*Paxos
}

// NewReplica generates new Paxos replica
func NewReplica(id paxi.ID) *Replica {
	r := new(Replica)
	r.Node = paxi.NewNode(id)
	r.Paxos = NewPaxos(r)
	r.Register(paxi.Request{}, r.handleRequest)
	r.Register(P1a{}, r.HandleP1a)
	r.Register(P1b{}, r.HandleP1b)
	r.Register(P2a{}, r.HandleP2a)
	r.Register(P2b{}, r.HandleP2b)
	r.Register(P3{}, r.HandleP3)
	return r
}

func (r *Replica) handleRequest(m paxi.Request) {
	log.Debugf("Replica %s received %v\n", r.ID(), m)

	if m.Command.IsRead() {
		if *ReadQuorum || (*ReadLeader && r.Paxos.IsLeader()) {
			v := r.Execute(m.Command)
			reply := paxi.Reply{
				Command:    m.Command,
				Value:      v,
				Properties: make(map[string]string),
				Timestamp:  time.Now().Unix(),
			}
			reply.Properties["slot"] = strconv.Itoa(r.Paxos.slot)
			reply.Properties["ballot"] = r.Paxos.ballot.String()
			reply.Properties["execute"] = strconv.Itoa(r.Paxos.execute)
			m.Reply(reply)
			return
		}
	}

	if *stable {
		if r.Paxos.IsLeader() || r.Paxos.Ballot() == 0 {
			r.Paxos.HandleRequest(m)
		} else {
			go r.Forward(r.Paxos.Leader(), m)
		}
	} else {
		r.Paxos.HandleRequest(m)
	}
}

