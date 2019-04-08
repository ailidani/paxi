package paxi

import (
	"encoding/gob"
	"fmt"
	"github.com/ailidani/paxi/log"
)

func init() {
	gob.Register(Request{})
	gob.Register(Reply{})
	gob.Register(PaxiBatchMsg{})
	gob.Register(Read{})
	gob.Register(ReadReply{})
	gob.Register(Transaction{})
	gob.Register(TransactionReply{})
	gob.Register(Register{})
	gob.Register(Config{})
}

/***************************
 * Client-Replica Messages *
 ***************************/

// Request is client reqeust with http response channel
type Request struct {
	Command    Command
	Properties map[string]string
	Timestamp  int64
	NodeID     ID         // forward by node
	c          chan Reply // reply channel created by request receiver
}

// Reply replies to current client session
func (r *Request) Reply(reply Reply) {
	r.c <- reply
}

func (r Request) String() string {
	return fmt.Sprintf("Request {cmd=%v nid=%v}", r.Command, r.NodeID)
}

// Reply includes all info that might replies to back the client for the coresponding reqeust
type Reply struct {
	Command    Command
	Value      Value
	Properties map[string]string
	Timestamp  int64
	Err        error
}

func (r Reply) String() string {
	return fmt.Sprintf("Reply {cmd=%v value=%v}", r.Command, r.Value)
}

type PaxiBatchMsg struct {
	Messages  []interface{}
	Timestamp int64
}

func (m PaxiBatchMsg) String() string {
	return fmt.Sprintf("PaxiBatchMsg {|msg|=%d time=%v}", len(m.Messages), m.Timestamp)
}

func (m* PaxiBatchMsg) AddToBatch(msg interface{}) {
	m.Messages = append(m.Messages, msg)
	log.Debugf("Added to Batch %v", m)
}

// Read can be used as a special request that directly read the value of key without go through replication protocol in Replica
type Read struct {
	CommandID int
	Key       Key
}

func (r Read) String() string {
	return fmt.Sprintf("Read {cid=%d, key=%d}", r.CommandID, r.Key)
}

// ReadReply cid and value of reading key
type ReadReply struct {
	CommandID int
	Value     Value
}

func (r ReadReply) String() string {
	return fmt.Sprintf("ReadReply {cid=%d, val=%v}", r.CommandID, r.Value)
}

// Transaction contains arbitrary number of commands in one request
// TODO read-only or write-only transactions
type Transaction struct {
	Commands  []Command
	Timestamp int64

	c chan TransactionReply
}

// Reply replies to current client session
func (t *Transaction) Reply(r TransactionReply) {
	t.c <- r
}

func (t Transaction) String() string {
	return fmt.Sprintf("Transaction {cmds=%v}", t.Commands)
}

// TransactionReply is the result of transaction struct
type TransactionReply struct {
	OK        bool
	Commands  []Command
	Timestamp int64
	Err       error
}

/**************************
 *     Config Related     *
 **************************/

// Register message type is used to regitster self (node or client) with master node
type Register struct {
	Client bool
	ID     ID
	Addr   string
}
