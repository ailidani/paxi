package vpaxos

import (
	"encoding/gob"
	"fmt"

	"github.com/ailidani/paxi"
)

func init() {
	gob.Register(P2a{})
	gob.Register(P2b{})
	gob.Register(P3{})
	gob.Register(Query{})
	gob.Register(Info{})
	gob.Register(Move{})
}

/**********************
 *   Paxos Messages   *
 **********************/

// P2a accept message
type P2a struct {
	Ballot  paxi.Ballot
	Slot    int
	Command paxi.Command
}

func (m P2a) String() string {
	return fmt.Sprintf("P2a {b=%v, s=%d, c=%v}", m.Ballot, m.Slot, m.Command)
}

// P2b accepted message
type P2b struct {
	Ballot paxi.Ballot
	ID     paxi.ID // from node id
	Slot   int
}

func (m P2b) String() string {
	return fmt.Sprintf("P2b {b=%v, id=%s, s=%d}", m.Ballot, m.ID, m.Slot)
}

// P3 commit message
type P3 struct {
	Ballot  paxi.Ballot
	Slot    int
	Command paxi.Command
}

func (m P3) String() string {
	return fmt.Sprintf("P3 {b=%v s=%d cmd=%v}", m.Ballot, m.Slot, m.Command)
}

/***********************
 *   Master Messages   *
 ***********************/

// Query message request the current info on key from master
type Query struct {
	Key paxi.Key
	ID  paxi.ID
}

func (m Query) String() string {
	return fmt.Sprintf("Query {key=%d id=%v}", m.Key, m.ID)
}

// Info is reply message for both query and Move message
type Info struct {
	Key    paxi.Key
	Ballot paxi.Ballot
}

func (m Info) String() string {
	return fmt.Sprintf("Info {key=%d ballot=%v}", m.Key, m.Ballot)
}

// Move message suggest master to move an object
type Move struct {
	Key  paxi.Key
	From paxi.ID
	To   paxi.ID
}

func (m Move) String() string {
	return fmt.Sprintf("Move {key=%d from=%v to=%v}", m.Key, m.From, m.To)
}
