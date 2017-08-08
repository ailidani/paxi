package epaxos

import (
	"fmt"
	. "paxi"
)

type Prepare struct {
	LeaderId ID
	Replica  ID
	Instance int
	Ballot   int
}

func (p Prepare) String() string {
	return fmt.Sprintf("Prepare {lid=%v, bal=%d, ins=%d}", p.LeaderId, p.Ballot, p.Instance)
}

type PrepareReply struct {
	AcceptorId ID
	Replica    ID
	Instance   int
	OK         bool
	Ballot     int
	Status     int8
	Command    []Command
	Seq        int
	Deps       map[ID]int
}

type PreAccept struct {
	LeaderId ID
	Replica  ID
	Instance int
	Ballot   int
	Command  []Command
	Seq      int
	Deps     map[ID]int
}

func (p PreAccept) String() string {
	return fmt.Sprintf("PreAccept {lid=%v, bal=%d, ins=%d, cmd=%v}", p.LeaderId, p.Ballot, p.Instance, p.Command[0])
}

type PreAcceptReply struct {
	Replica       ID
	Instance      int
	OK            bool
	Ballot        int
	Seq           int
	Deps          map[ID]int
	CommittedDeps map[ID]int
}

func (p PreAcceptReply) String() string {
	return fmt.Sprintf("PreAcceptReply {id=%v, ins=%d, ok=%t, bal=%d}", p.Replica, p.Instance, p.OK, p.Ballot)
}

type PreAcceptOK struct {
	Replica  ID
	Instance int
}

func (p PreAcceptOK) String() string {
	return fmt.Sprintf("PreAcceptOK {id=%v, ins=%d}", p.Replica, p.Instance)
}

type Accept struct {
	LeaderId ID
	Replica  ID
	Instance int
	Ballot   int
	Count    int
	Seq      int
	Deps     map[ID]int
}

func (a Accept) String() string {
	return fmt.Sprintf("Accept {lid=%v, id=%v, ins=%d, bal=%d}", a.LeaderId, a.Replica, a.Instance, a.Ballot)
}

type AcceptReply struct {
	Replica  ID
	Instance int
	OK       bool
	Ballot   int
}

func (a AcceptReply) String() string {
	return fmt.Sprintf("AcceptReply {id=%v, ins=%d, ok=%t, bal=%d}", a.Replica, a.Instance, a.OK, a.Ballot)
}

type Commit struct {
	LeaderId ID
	Replica  ID
	Instance int
	Command  []Command
	Seq      int
	Deps     map[ID]int
}

type CommitShort struct {
	LeaderId ID
	Replica  ID
	Instance int
	Count    int
	Seq      int
	Deps     map[ID]int
}

type TryPreAccept struct {
	LeaderId ID
	Replica  ID
	Instance int
	Ballot   int
	Command  []Command
	Seq      int
	Deps     map[ID]int
}

type TryPreAcceptReply struct {
	AcceptorId       ID
	Replica          ID
	Instance         int
	OK               bool
	Ballot           int
	ConflictReplica  ID
	ConflictInstance int
	ConflictStatus   int8
}

const (
	NONE int8 = iota
	PREACCEPTED
	PREACCEPTED_EQ
	ACCEPTED
	COMMITTED
	EXECUTED
)
