package benor

import (
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
)

// required for implementing slots
const (
	HTTPHeaderSlot       = "Slot"
	HTTPHeaderBallot     = "Ballot"
	HTTPHeaderExecute    = "Execute"
)


// Replica for Benor
type Replica struct {
	paxi.Node
	*Benor
}

// New Replica generates a new replica for Benor
func NewReplica(id paxi.ID) *Replica{
	r := new(Replica)
	r.Node = paxi.NewNode(id)
	r.Benor = NewBenor(r)
	r.Register(paxi.Request{}, r.handleRequest)

	r.Register(Msg1{}, r.HandleMsg1)
	r.Register(Msg2{}, r.HandleMsg2)
	r.Register(Msg3{}, r.HandleMsg3)
	return r
}

func(r *Replica) handleRequest(m paxi.Request){
	log.Infof("Enter HandleRequest")
	log.Debugf("Replica %s received %v\n", r.ID(), m)

	//if m.Command.IsRead() /*&& *read != ""*/ {
	//	reply := paxi.Reply{
	//		Command:    m.Command,
	//		Value:      m.Command.Value,
	//		Properties: make(map[string]string),
	//		Timestamp:  time.Now().Unix(),
	//	}
	//	reply.Properties[HTTPHeaderSlot] = strconv.Itoa(r.Benor.slot)
	//
	//	m.Reply(reply)
	//	return
	//}

	r.Benor.HandleRequest(m)
	log.Infof("Exit Replica handleRequest")
}