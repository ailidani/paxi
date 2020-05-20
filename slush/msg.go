package slush

import (
	"encoding/gob"
	"fmt"

	"github.com/ailidani/paxi"
)

func init() {
	gob.Register(Msg1{})
	gob.Register(Msg2{})
	gob.Register(Msg3{})
}

type Msg1 struct {
	ID paxi.ID // Node ID
	Col int // Node Color
}

type Msg2 struct {
	ID paxi.ID
	Col int
}

/* Reset Message */
type Msg3 struct {
	ID paxi.ID
}

func (m Msg1) String() string {
	return fmt.Sprint("Msg1 {id=#{m.ID} col=#{m.Col}")
}

func (m Msg2) String() string {
	return fmt.Sprint("Msg2 {id=#{m.ID} col=#{m.Col}")
}