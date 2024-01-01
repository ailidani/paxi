package snowball

import (
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
	"time"
)
// Replica for Snowball
type Replica struct {
	paxi.Node
	*Snowball
}

// NewReplica generates new Snowball replica
func NewReplica(id paxi.ID) *Replica {
	r := new(Replica)
	r.Node = paxi.NewNode(id)
	r.Snowball = NewSnowball(r)
	r.Register(paxi.Request{}, r.handleRequest)
	r.Register(Msg1{}, r.HandleMsg1)
	r.Register(Msg2{}, r.HandleMsg2)
	r.Register(Msg3{}, r.HandleMsg3)
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
	//r.Snowball.SetQuerying(true)
	r.Snowball.HandleRequest(m)
	log.Infof("Exit HandleRequest")
}
