package wankeeper

import (
	"encoding/gob"
	. "paxi"
	"paxi/glog"
)

type Replica struct {
	*Node
	leader bool
	master bool
}

func NewReplica(config *Config) *Replica {
	gob.Register(NewLeader{})
	gob.Register(Vote{})
	gob.Register(Accept{})
	gob.Register(Accepted{})
	gob.Register(Commit{})

	master := false

	if config.ID.Site() == 1 {
		master = true
	}

	return &Replica{
		Node:   NewNode(config),
		master: master,
	}
}

func (r *Replica) Run() {
	go r.messageLoop()
	r.Node.Run()
}

func (r *Replica) messageLoop() {
	for {
		select {
		case msg := <-r.RequestChan:
			glog.V(2).Infof("Replica %s received %v\n", r.ID, msg)
			r.handleRequest(msg)

		case msg := <-r.MessageChan:
			switch msg := msg.(type) {
			case NewLeader:

			case Vote:

			case Accept:

			case Accepted:

			case Commit:

			}
		}
	}
}

func (r *Replica) handleRequest(msg Request) {

}
