package chain

import (
	"encoding/gob"
	"fmt"

	"github.com/ailidani/paxi"
)

func init() {
	gob.Register(Accept{})
	gob.Register(Ack{})
}

type Accept struct {
	Ballot  paxi.Ballot
	Command paxi.Command
	LSN     uint64
	From    paxi.ID
}

func (m Accept) String() string {
	return fmt.Sprintf("Accept {b=%v cmd=%v lsn=%d from=%s}", m.Ballot, m.Command, m.LSN, m.From)
}

type Ack struct {
	Ballot paxi.Ballot
	LSN    uint64
	From   paxi.ID
}

func (m Ack) String() string {
	return fmt.Sprintf("Ack {b=%v lsn=%d from=%s}", m.Ballot, m.LSN, m.From)
}
