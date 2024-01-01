package binaryconsensus

import (
	"encoding/gob"
	"fmt"
	"github.com/ailidani/paxi"
)

func init(){
	gob.Register(Msg1{})
	gob.Register(Msg2{})
	gob.Register(Msg3{})
	gob.Register(Msg4{})
}

type Msg1 struct {
	ID paxi.ID
	p1v int

}

type Msg2 struct {
	ID paxi.ID
	p1v int
}

type Msg3 struct {
	ID paxi.ID
	p2v int
}

type Msg4 struct {
	ID paxi.ID
	decision bool
}

func(m Msg1) String() string{
	return fmt.Sprint("Msg 1= ID: ", m.ID,"and p1v: ", m.p1v)
}

func(m Msg2) String() string{
	return fmt.Sprint("Msg2: ID: p", m.ID,"and p2v: ", m.p1v)
}


