package binaryconsensus
import(
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
	"math/rand"

	"time"
)

// Log entry struct
type entry struct {
	ballot paxi.Ballot
	command paxi.Command
	commit bool
	request *paxi.Request
	quorum *paxi.Quorum
	timestamp time.Time
}

// Binary instance
type Binary struct{
	paxi.Node

	log           map[int]*entry // log ordered by slot
	slot          int // slot number for the requests
	p1v           int
	p2v           int
	decision      bool
	decidedValue int
	Q1        *paxi.Quorum     	// phase 1 value quorum
	Q2        *paxi.Quorum		// phase 2 value quorum
	rounds        int
	request       *paxi.Request
	leader paxi.ID

}

func NewBinary(n paxi.Node, options ...func(*Binary)) *Binary{
	bi := &Binary{
		Node:     n,
		log:      make(map[int]*entry, paxi.GetConfig().BufferSize),
		slot:     -1,
		p1v:      1,
		p2v:      -1,
		decision: false,
		Q1:   	paxi.NewQuorum(),
		Q2: 	paxi.NewQuorum(),
		rounds:   4,

	}

	for _, opt := range options{
		opt(bi)
	}

	//if n.ID().Node() % 2 == 0{
	//	bi.p1v = 1
	//}

	return bi
}

// HandleRequest handles the request received from the client
// Request received will then start working on sending the Msg1
// Msg1 will contain phase 1 value
func(bi *Binary) HandleRequest(r paxi.Request){
	log.Infof("Enter HandleRequest binary.go")
	bi.request = &r
	log.Infof("Received from client: %v, key: %v, value: %v", bi.request.Command.ClientID,
		bi.request.Command.Key, bi.request.Command.Value)

	bi.Msg1()

	log.Infof("Exit HandleRequest binary.go")
}

// only the replica that receives client request will enter this
func (bi *Binary) Msg1(){
	// I broadcast the message to other nodes
	log.Infof("Enter Msg1")
	bi.slot++
	bi.log[bi.slot] = &entry {
		request: bi.request,
		quorum: paxi.NewQuorum(),
		timestamp: time.Now(),
	}

	log.Infof("Broadcast Msg1: ID: %v, p1v: %v", bi.ID(), bi.p1v)
	bi.Broadcast(Msg1{
		ID:  bi.ID(),
		p1v: bi.p1v,
	})

	log.Infof("Exit Msg1")
}

// all follower replicas will receive the message from the "leader"
func(bi *Binary) HandleMsg1(m Msg1){

	log.Infof("Enter HandleMsg1")
	bi.leader = m.ID
	log.Infof("received ID: %v p1v: %v", m.ID, m.p1v)
	bi.Q1.BenORAck(m.ID, m.p1v)
	log.Infof("I follower broadcast the ID and p1v to all")

	// Msg2 sent
	bi.Broadcast(Msg2{
		ID:  bi.ID(),
		p1v: bi.p1v,
	})

	log.Infof("Exit HandleMsg1")
}

// all replicas will do this procedure
func(bi *Binary) HandleMsg2(m Msg2){
	log.Infof("Enter HandleMsg2")

	log.Infof("received ID: %v p1v: %v", m.ID, m.p1v)
	log.Infof("ACKing the message received and adding to quorum 1")

	if !(m.ID == bi.leader){
		bi.Q1.BenORAck(m.ID, m.p1v)
	}
	// check for majority in the arrived quorums
	// based on the majority set p2v as 0 or 1
	// no majority means -1


	if bi.Q1.BenORMajority() == 0{
		bi.p2v = 0
	} else if bi.Q1.BenORMajority() == 1 {
		bi.p2v = 1
	} else {
		bi.p2v = -1
	}

	// value decided
	// ready to start phase 2

	log.Infof("Broadcasting my p2v I decided upon: p2v: %v", bi.p2v)
	bi.Broadcast(Msg3{
		ID:  bi.ID(),
		p2v: bi.p2v,
	})

	log.Infof("Exit HandleMsg2")
}

// all replicas will do this procedure for phase 2 value
func(bi *Binary) HandleMsg3(m Msg3){
	log.Infof("Enter HandleMsg3")
	log.Infof("received ID: %v p1v: %v", m.ID, m.p2v)
	log.Infof("ACKing the message received and adding to quorum 2")

	// ack the incoming message in a new quorum
	bi.Q2.BenORAck(m.ID, m.p2v)

	// again check for majority in the messages received
	// if 0 set 0, if 1 set 1
	// if none repeat the procedure for next round
	// this time set p1v to a value 0 or 1 if atleast one of those is present
	// else random value between 0 or 1
	if bi.Q2.BenORMajority() == 0 {
		bi.decidedValue = 0
		bi.decision = true
		log.Infof("decision reached")
		log.Infof("Sending msg to leader to send reply back to client")
		bi.Send(bi.leader, Msg4{
			ID:       bi.ID(),
			decision: bi.decision,
		})

	} else if bi.Q2.BenORMajority() == 1 {
		bi.decidedValue = 1
		bi.decision = true
		log.Infof("decision reached")
		log.Infof("Sending msg to leader to send reply back to client")
		bi.Send(bi.leader, Msg4{
			ID:       bi.ID(),
			decision: bi.decision,
		})

	} else {
		if bi.Q2.BenOrAtLeastOneValue() == 0 {
			bi.p1v = 0
		} else if bi.Q2.BenOrAtLeastOneValue() == 1 {
			bi.p1v = 1
		} else {
			// randomize
			rand.Seed(time.Now().UnixNano())
			randomNumber := rand.Intn(100)
			if randomNumber > 50 {
				bi.p1v = 1
			} else {
				bi.p2v = 0
			}

		}
	}

	log.Infof("Exit HandleMsg3")
}

// only the leader node will do this process
func(bi *Binary) HandleMsg4(m Msg4){

	log.Infof("Enter HandleMsg4")
	log.Infof("Sending reply back to client")
	bi.request.Reply(paxi.Reply{
		Command:   bi.request.Command,
		Value:     bi.request.Command.Value,
		Timestamp: bi.request.Timestamp,
	})

	log.Infof("Exit HandleMsg4")
}