package cosmos

import (
	"encoding/gob"
	. "paxi"
	"paxi/glog"
)

type Replica struct {
	*Node
}

func NewReplica(config *Config) *Replica {
	gob.Register(CMD{})
	gob.Register(ACK{})

	return &Replica{
		Node: NewNode(config),
	}
}

// Run start running replica
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
			r.dispatch(msg)
		}
	}
}

func (r *Replica) handleRequest(msg Request) {

}

func (r *Replica) dispatch(msg Message) {
	switch msg := msg.(type) {
	case CMD:
	
	case ACK:

	}
}
