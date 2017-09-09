package paxi

import (
	"encoding/json"
	"fmt"
	"net/http"
)

/***************************
 * Client-Replica Messages *
 ***************************/

type CommandID int

type Request struct {
	CommandID CommandID
	Commands  []Command
	ClientID  ID
	Timestamp int64

	w http.ResponseWriter `json:"-"`
}

func (r *Request) Reply(rep Reply) {
	b, _ := json.Marshal(rep)
	r.w.Write(b)
}

func (r Request) String() string {
	return fmt.Sprintf("Request {cid=%d, cmd=%v, id=%s}", r.CommandID, r.Commands, r.ClientID)
}

type Reply struct {
	OK        bool
	CommandID CommandID
	LeaderID  ID
	ClientID  ID
	Commands  []Command
	Timestamp int64
}

func (r Reply) String() string {
	return fmt.Sprintf("Reply {ok=%t, cid=%d, lid=%s, id=%v, cmd=%v}", r.OK, r.CommandID, r.LeaderID, r.ClientID, r.Commands)
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

type Register struct {
	Client bool
	ID     ID
	Addr   string
}
