package epaxos

import (
	"encoding/gob"
	"fmt"

	"github.com/ailidani/paxi"
)

func init() {
	gob.Register(PreAccept{})
	gob.Register(PreAcceptReply{})
	gob.Register(Accept{})
	gob.Register(AcceptReply{})
	gob.Register(Commit{})
}

type PreAccept struct {
	Ballot  paxi.Ballot
	Replica paxi.ID
	Slot    int
	Command paxi.Command
	Seq     int
	Dep     map[paxi.ID]int
}

func (m PreAccept) String() string {
	return fmt.Sprintf("PreAccept {bal=%d id=%s s=%d cmd=%v seq=%d dep=%v}", m.Ballot, m.Replica, m.Slot, m.Command, m.Seq, m.Dep)
}

type PreAcceptReply struct {
	Ballot    paxi.Ballot
	Replica   paxi.ID
	Slot      int
	Seq       int
	Dep       map[paxi.ID]int
	Committed map[paxi.ID]int
}

func (m PreAcceptReply) String() string {
	return fmt.Sprintf("PreAcceptReply {bal=%d id=%s s=%d seq=%d dep=%v c=%v}", m.Ballot, m.Replica, m.Slot, m.Seq, m.Dep, m.Committed)
}

type Accept struct {
	Ballot  paxi.Ballot
	Replica paxi.ID
	Slot    int
	Seq     int
	Dep     map[paxi.ID]int
}

type AcceptReply struct {
	Ballot  paxi.Ballot
	Replica paxi.ID
	Slot    int
}

type Commit struct {
	Ballot  paxi.Ballot
	Replica paxi.ID
	Slot    int
	Command paxi.Command
	Seq     int
	Dep     map[paxi.ID]int
}
