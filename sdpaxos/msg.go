package sdpaxos

import (
	"encoding/gob"
	"fmt"

	"github.com/ailidani/paxi"
)

func init() {
	gob.Register(CAccept{})
	gob.Register(CAck{})
	gob.Register(CCommit{})
	gob.Register(OAccept{})
	gob.Register(OAck{})
	gob.Register(OCommit{})
}

type CAccept struct {
	Ballot  paxi.Ballot
	Command paxi.Command
	Slot    uint64
	From    paxi.ID
}

func (m CAccept) String() string {
	return fmt.Sprintf("CAccept {b=%v cmd=%v s=%d from=%s}", m.Ballot, m.Command, m.Slot, m.From)
}

type CAck struct {
	Ballot paxi.Ballot
	Slot   uint64
	From   paxi.ID
}

func (m CAck) String() string {
	return fmt.Sprintf("CAck {b=%v s=%d from=%v}", m.Ballot, m.Slot, m.From)
}

type CCommit struct {
	Ballot  paxi.Ballot
	Command paxi.Command
	Slot    uint64
	From    paxi.ID
}

func (m CCommit) String() string {
	return fmt.Sprintf("CCommit {b=%v cmd=%v s=%d from=%s}", m.Ballot, m.Command, m.Slot, m.From)
}

type OAccept struct {
	Ballot paxi.Ballot
	ID     paxi.ID
	Slot   uint64
	GSlot  uint64
	From   paxi.ID
}

func (m OAccept) String() string {
	return fmt.Sprintf("OAccept {b=%v id=%v s=%d gs=%d from=%v}", m.Ballot, m.ID, m.Slot, m.GSlot, m.From)
}

type OAck struct {
	Ballot paxi.Ballot
	ID     paxi.ID
	Slot   uint64
	GSlot  uint64
	From   paxi.ID
}

func (m OAck) String() string {
	return fmt.Sprintf("OAck {b=%v id=%v s=%d gs=%d from=%v}", m.Ballot, m.ID, m.Slot, m.GSlot, m.From)
}

type OCommit struct {
	Ballot paxi.Ballot
	Slot   uint64
	GSlot  uint64
	From   paxi.ID
}

func (m OCommit) String() string {
	return fmt.Sprintf("OCommit {b=%v s=%d gs=%d from=%v}", m.Ballot, m.Slot, m.GSlot, m.From)
}
