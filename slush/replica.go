package slush

import (
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
	"time"
)
// Replica for Slush
type Replica struct {
	paxi.Node
	*Slush
}

// NewReplica generates new Slush replica
func NewReplica(id paxi.ID) *Replica {
	r := new(Replica)
	r.Node = paxi.NewNode(id)
	r.Slush = NewSlush(r)
	r.Register(paxi.Request{}, r.handleRequest)
	r.Register(Msg1{}, r.HandleMsg1)
	return r
}

func (r *Replica) handleRequest(m paxi.Request) {
	log.Infof("Enter HandleRequest")
	log.Debugf("Replica %s received %v\n", r.ID(), m)

	if m.Command.IsRead() /*&& *read != ""*/ {
		reply := paxi.Reply{
			Command:    m.Command,
			Value:      m.Command.Value,
			Properties: make(map[string]string),
			Timestamp:  time.Now().Unix(),
		}
		m.Reply(reply)
		return
	}
	//r.Slush.SetQuerying(true)
	r.Slush.HandleRequest(m)
	log.Infof("Exit HandleRequest")
}
