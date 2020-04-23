package slush

import (
	"encoding/gob"
	"fmt"

	"Slush/paxi"
)

func init() {
	gob.Register(Msg1{})
}

type Msg1 struct {
	ID paxi.ID // Node ID
	Col int // Node Color
}

func (m Msg1) String() string {
	return fmt.Sprint("Msg1 {id=#{m.ID} col=#{m.Col}")
}

