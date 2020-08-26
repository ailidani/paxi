package benor

import(
	"encoding/gob"
	"fmt"

	"github.com/ailidani/paxi"
)

func init(){
	gob.Register(Msg1{})
	gob.Register(Msg2{})
}

type Msg1 struct {
	ID paxi.ID
	p1v int // phase 1 value - input, can be 0,1
	//round int
}

type Msg2 struct {
	ID paxi.ID
	p2v int // phase 2 value, can be 0,1,-1
}

func(m Msg1) String() string{
	return fmt.Sprint("Msg 1= ID: ", m.ID,"and p1v: ", m.p1v)
}

func(m Msg2) String() string{
	return fmt.Sprint("Msg2: ID: p", m.ID,"and p2v: ", m.p2v)
}

