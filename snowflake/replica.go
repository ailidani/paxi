package snowflake

import (
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
	"time"
)
// Replica for Snowflake
type Replica struct {
	paxi.Node
	*Snowflake
}

// NewReplica generates new Snowflake replica
func NewReplica(id paxi.ID) *Replica {
	r := new(Replica)
	r.Node = paxi.NewNode(id)
	r.Snowflake = NewSnowflake(r)
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
	//r.Snowflake.SetQuerying(true)
	r.Snowflake.HandleRequest(m)
	log.Infof("Exit HandleRequest")
}
