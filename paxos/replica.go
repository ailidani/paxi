package paxos

import (
	"flag"
	"strconv"
	"time"

	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
)

var ephemeralLeader = flag.Bool("ephemeral_leader", false, "stable leader, if true paxos forward request to current leader")
var readQuorum = flag.Bool("read_quorum", false, "read from quorum of replicas")
var readLeader = flag.Bool("read_leader", false, "read from leader of current ballot")

const (
	HTTPHeaderSlot    = "Slot"
	HTTPHeaderBallot  = "Ballot"
	HTTPHeaderExecute = "Execute"
)

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

	if m.Command.IsRead() && (*readQuorum || (*readLeader && r.Paxos.IsLeader())) {
		v, s := r.read(m)
		reply := paxi.Reply{
			Command:    m.Command,
			Value:      v,
			Properties: make(map[string]string),
			Timestamp:  time.Now().Unix(),
		}
		reply.Properties[HTTPHeaderSlot] = strconv.Itoa(s)
		reply.Properties[HTTPHeaderBallot] = r.Paxos.ballot.String()
		reply.Properties[HTTPHeaderExecute] = strconv.Itoa(r.Paxos.execute - 1)
		m.Reply(reply)
		return
	}

	if *ephemeralLeader {
		r.Paxos.HandleRequest(m)
		return
	}

	if r.Paxos.IsLeader() || r.Paxos.Ballot() == 0 {
		r.Paxos.HandleRequest(m)
	} else {
		go r.Forward(r.Paxos.Leader(), m)
	}
}

func (r *Replica) read(m paxi.Request) (paxi.Value, int) {
	// TODO
	// (1) last slot is read?
	// (2) entry in log over writen
	// (3) value is not overwriten command

	// is in progress
	for i := r.Paxos.execute; i <= r.Paxos.slot; i++ {
		entry, exist := r.Paxos.log[i]
		if exist && entry.command.Key == m.Command.Key {
			return entry.command.Value, r.Paxos.slot
		}
	}

	// not in progress key
	return r.Node.Execute(m.Command), 0
}
