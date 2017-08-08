package paxi

import "fmt"

/**************************
 *    Interface Related   *
 **************************/

type Message interface{}

/***************************
 * Client-Replica Messages *
 ***************************/

type CommandID int

type Request struct {
	CommandID CommandID
	Command   Command
	ClientID  ID
	Timestamp int64
}

func (p Request) String() string {
	return fmt.Sprintf("Request {cid=%d, cmd=%v, id=%s}", p.CommandID, p.Command, p.ClientID)
}

type Reply struct {
	OK        bool
	CommandID CommandID
	LeaderID  ID
	ClientID  ID
	Command   Command
	Timestamp int64
}

func (r Reply) String() string {
	return fmt.Sprintf("Reply {ok=%t, cid=%d, lid=%s, id=%v, cmd=%v}", r.OK, r.CommandID, r.LeaderID, r.ClientID, r.Command)
}

type Read struct {
	CommandID CommandID
	Key       Key
}

func (r Read) String() string {
	return fmt.Sprintf("Read {cid=%d, key=%d}", r.CommandID, r.Key)
}

type ReadReply struct {
	CommandID CommandID
	Value     Value
}

func (r ReadReply) String() string {
	return fmt.Sprintf("ReadReply {cid=%d, val=%v}", r.CommandID, r.Value)
}

type Transaction struct {
	CommandId CommandID
	Commands  []Command
	ClientID  ID
	Timestamp int64
}

func (t Transaction) String() string {
	return fmt.Sprintf("Transaction {cid=%d, cmds=%v, id=%s", t.CommandId, t.Commands, t.ClientID)
}

type TransactionReply struct {
	OK        bool
	CommandID CommandID
	LeaderID  ID
	ClientID  ID
	Commands  []Command
	Timestamp int64
}

/**************************
 *     Config Related     *
 **************************/

type EndpointType uint8

const (
	CLIENT EndpointType = iota
	NODE
)

type Register struct {
	EndpointType EndpointType
	ID           ID
	Addr         string
}
