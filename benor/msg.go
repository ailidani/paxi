package benor

import (
	"encoding/gob"
	"fmt"
	"github.com/ailidani/paxi"
)

func init(){
	gob.Register(Msg1{})
	gob.Register(Msg2{})
	gob.Register(Msg3{})
}

type Msg1 struct {
	// msg for broadcasting p1v value by the replica that receives client request
	ID paxi.ID
	p1v int // phase 1 value - input, can be 0,1
	slot int // added slots
	request paxi.Request	// client request structure
	//round int
}

type Msg2 struct {
	// msg for broadcasting to other replicas when one replica receives msg1 from the client serving replica
	ID paxi.ID
	p1v int // phase 2 value, can be 0,1,-1
	slot int	// slots
}

type Msg3 struct {
	ID paxi.ID
	p2v int
	slot int
}

func(m Msg1) String() string{
	return fmt.Sprint("Msg 1= ID: ", m.ID,"and p1v: ", m.p1v)
}

func(m Msg2) String() string{
	return fmt.Sprint("Msg2: ID: p", m.ID,"and p2v: ", m.p1v)
}

func(m Msg3) String() string{
	return fmt.Sprint("Msg3: ID: ", m.ID)
}