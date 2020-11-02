package binaryconsensus

import (
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
)

type Replica struct {
	paxi.Node
	*Binary
}

//function NewReplica creates a new Replica Instance
func NewReplica(id paxi.ID) *Replica{
	r := new(Replica)
	r.Node = paxi.NewNode(id)
	r.Binary = NewBinary(r)
	r.Register(paxi.Request{}, r.HandleRequest)

	r.Register(Msg1{}, r.HandleMsg1)
	r.Register(Msg2{}, r.HandleMsg2)
	r.Register(Msg3{}, r.HandleMsg3)
	r.Register(Msg4{}, r.HandleMsg4)

	return r

}


func(r *Replica) handleRequest(m paxi.Request){
	log.Infof("Enter HandleRequest")
	log.Debugf("Replica %s received %v\n", r.ID(), m)

	r.Binary.HandleRequest(m)
	log.Infof("Exit Replica handleRequest")
}