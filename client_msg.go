package paxi

import (
	"encoding/gob"
	"fmt"
	"net/http"
	"paxi/glog"
)

func init() {
	gob.Register(Request{})
	gob.Register(Reply{})
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

// CommandID identifies commands from each client, can be any integer type.
type CommandID uint64

// Request client reqeust with http response write
type Request struct {
	ClientID  ID
	CommandID CommandID
	Command   Command
	Timestamp int64

	w http.ResponseWriter
}

// Reply replies to the current request
func (r *Request) Reply(reply Reply) {
	if reply.Err != nil {
		http.Error(r.w, reply.Err.Error(), http.StatusInternalServerError)
		return
	}
	r.w.Header().Set("ok", fmt.Sprintf("%v", reply.OK))
	r.w.Header().Set("id", reply.ClientID.String())
	r.w.Header().Set("cid", fmt.Sprintf("%v", reply.CommandID))
	r.w.Header().Set("timestamp", fmt.Sprintf("%v", reply.Timestamp))
	_, err := r.w.Write(reply.Command.Value)
	if err != nil {
		glog.Errorln(err)
	}
}

func (r Request) String() string {
	return fmt.Sprintf("Request {id=%s, cid=%d, cmd=%v}", r.ClientID, r.CommandID, r.Command)
}

// Reply includes all info that might replies to back the client for the coresponding reqeust
type Reply struct {
	ClientID  ID
	CommandID CommandID
	OK        bool
	LeaderID  ID
	Command   Command
	Timestamp int64
	Err       error
}

func (r Reply) String() string {
	return fmt.Sprintf("Reply {ok=%t, cid=%d, lid=%s, id=%v, cmd=%v}", r.OK, r.CommandID, r.LeaderID, r.ClientID, r.Command)
}

// Read can be used as a special request that directly read the value of key without go through replication protocol in Replica
type Read struct {
	CommandID CommandID
	Key       Key
}

func (r Read) String() string {
	return fmt.Sprintf("Read {cid=%d, key=%d}", r.CommandID, r.Key)
}

// ReadReply cid and value of reading key
type ReadReply struct {
	CommandID CommandID
	Value     Value
}

func (r ReadReply) String() string {
	return fmt.Sprintf("ReadReply {cid=%d, val=%v}", r.CommandID, r.Value)
}

// Transaction contains arbitrary number of commands in one request
type Transaction struct {
	CommandID CommandID
	Commands  []Command
	ClientID  ID
	Timestamp int64
}

func (t Transaction) String() string {
	return fmt.Sprintf("Transaction {id=%s, cid=%d, cmds=%v", t.ClientID, t.CommandID, t.Commands)
}

// TransactionReply is the result of transaction struct
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

// Register message type is used to regitster self (node or client) with master node
type Register struct {
	Client bool
	ID     ID
	Addr   string
}
