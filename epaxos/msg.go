package epaxos

import (
	"encoding/gob"
	"fmt"

	"github.com/ailidani/paxi"
)

func init() {
	gob.Register(Prepare{})
	gob.Register(PrepareReply{})
	gob.Register(PreAccept{})
	gob.Register(PreAcceptReply{})
	gob.Register(PreAcceptOK{})
	gob.Register(Accept{})
	gob.Register(AcceptReply{})
	gob.Register(Commit{})
	gob.Register(CommitShort{})
	gob.Register(TryPreAccept{})
	gob.Register(TryPreAcceptReply{})
}

type Prepare struct {
	Ballot   paxi.Ballot
	Replica  paxi.ID
	Instance int
}

func (p Prepare) String() string {
	return fmt.Sprintf("Prepare {b=%v, s=%d}", p.Ballot, p.Instance)
}

type PrepareReply struct {
	AcceptorId paxi.ID
	Replica    paxi.ID
	Instance   int
	OK         bool
	Ballot     paxi.Ballot
	Status     status
	Command    paxi.Command
	Seq        int
	Deps       map[paxi.ID]int
}

type PreAccept struct {
	Ballot   paxi.Ballot
	Replica  paxi.ID
	Instance int
	Command  paxi.Command
	Seq      int
	Deps     map[paxi.ID]int
}

func (p PreAccept) String() string {
	return fmt.Sprintf("PreAccept {b=%v, ins=%d, cmd=%v}", p.Ballot, p.Instance, p.Command)
}

type PreAcceptReply struct {
	Replica       paxi.ID
	Instance      int
	OK            bool
	Ballot        paxi.Ballot
	Seq           int
	Deps          map[paxi.ID]int
	CommittedDeps map[paxi.ID]int
}

type PreAcceptOK struct {
	Replica  paxi.ID
	Instance int
}

func (p PreAcceptOK) String() string {
	return fmt.Sprintf("PreAcceptOK {id=%v, ins=%d}", p.Replica, p.Instance)
}

type Accept struct {
	Ballot   paxi.Ballot
	Replica  paxi.ID
	Instance int
	Count    int
	Seq      int
	Deps     map[paxi.ID]int
}

type AcceptReply struct {
	Replica  paxi.ID
	Instance int
	OK       bool
	Ballot   paxi.Ballot
}

func (a AcceptReply) String() string {
	return fmt.Sprintf("AcceptReply {id=%v, ins=%d, ok=%t, bal=%d}", a.Replica, a.Instance, a.OK, a.Ballot)
}

type Commit struct {
	LeaderId paxi.ID
	Replica  paxi.ID
	Instance int
	Command  paxi.Command
	Seq      int
	Deps     map[paxi.ID]int
}

type CommitShort struct {
	LeaderId paxi.ID
	Replica  paxi.ID
	Instance int
	Count    int
	Seq      int
	Deps     map[paxi.ID]int
}

type TryPreAccept struct {
	LeaderId paxi.ID
	Replica  paxi.ID
	Instance int
	Ballot   int
	Command  paxi.Command
	Seq      int
	Deps     map[paxi.ID]int
}

type TryPreAcceptReply struct {
	AcceptorId       paxi.ID
	Replica          paxi.ID
	Instance         int
	OK               bool
	Ballot           paxi.Ballot
	ConflictReplica  paxi.ID
	ConflictInstance int
	ConflictStatus   int8
}

type status int8

const (
	NONE status = iota
	PREACCEPTED
	PREACCEPTED_EQ
	ACCEPTED
	COMMITTED
	EXECUTED
)
