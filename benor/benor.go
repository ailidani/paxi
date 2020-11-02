package benor

import(
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
	"math/rand"
	"strconv"
	"time"
)

// Log entry struct
type entry struct {
	ballot paxi.Ballot
	command paxi.Command
	commit bool
	request *paxi.Request
	quorum1 *paxi.Quorum
	quorum2 *paxi.Quorum
	timestamp time.Time
}

// Benor instance
type Benor struct{
	paxi.Node
	ballot 		  paxi.Ballot
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
	msg1Broadcast bool
	msg2Broadcast bool
	msg3Broadcast bool
	isClientRequest bool
	currentRound int
	MAXROUNDS int
	execute int

	ReplyWhenCommit bool
	//sentMsg1 [5]bool
}

func NewBenor(n paxi.Node, options ...func(*Benor)) *Benor{
	b := &Benor{
		Node:     n,
		log:      make(map[int]*entry, paxi.GetConfig().BufferSize),
		slot:     -1,
		p1v:      0,
		p2v:      -1,
		decision: false,
		decidedValue: -1,
		Q1:   paxi.NewQuorum(),
		Q2:   paxi.NewQuorum(),
		//rounds:   4,
		msg1Broadcast: false,
		msg2Broadcast: false,
		msg3Broadcast: false,
		isClientRequest: false,
		currentRound: 0,
		MAXROUNDS: 5,
		//request:  nil,
	}

	for _, opt := range options{
		opt(b)
	}
	//for i:= 0; i < b.rounds ; i++ {
	//	b.sentMsg1[i] = false
	//}

	//if n.ID().Node() % 2 == 1{
	//	b.p1v = 1
	//}
	/*
		1.1 - 1
		1.2 - 0
		1.3 - 1
		1.4 - 0
		1.5 - 1
	*/
	return b
}

// SetP1v sets the phase1 value of the node
func (b *Benor) SetP1v(phase1value int){
	b.p1v = phase1value
}
//SetP2v sets phase 2 value
func (b *Benor) SetP2v(phase2value int){
	b.p2v = phase2value
}

// GetP1v returns the phase 1 value of the node
func (b *Benor) GetP1v()int{
	return b.p1v
}

// GetP2v returns the phase 2 value of the node
func (b *Benor) GetP2v()int{
	return b.p2v
}


func (b *Benor) isDecision() bool {
	return b.decision
}

func (b *Benor) setDecision() {
	log.Infof("Accepted value %v:", b.GetP2v())
	b.decision = true
}

// HandleRequest handles the request received from the client
// Request received will then start working on sending the Msg1
// Msg1 will contain phase 1 value
func(b *Benor) HandleRequest(r paxi.Request){
	// increments the slot number. since at first it's -1
	b.slot++
	b.ballot.Next(b.ID())				// assigns the request a sequence number here
	// creating the log entry for that slot
	b.log[b.slot]= &entry{
		ballot:    b.ballot,
		command:   r.Command,
		commit:    false,
		request:   &r,
		quorum1:    b.Q1,
		quorum2:	b.Q2,
		timestamp: time.Time{},
	}
	log.Infof("Enter HandleRequest benor.go")
	//b.request = &r
	log.Infof("Received from Client!")
	//log.Infof("Received from client: key: %v, value: %v",
	//	b.request.Command.Key, b.request.Command.Value)
	// I received request from client
	// so I'm the assumed "leader"
	b.isClientRequest = true
	b.Msg1(r)

}

// Msg1 will handle broadcasting of phase 1 value
// only client serving replica can enter this
func(b *Benor) Msg1(req paxi.Request){
	log.Infof("Enter Msg1")
	//b.slot++
	//b.log[b.slot] = &entry {
	//	request: b.request,
	//	quorum: paxi.NewQuorum(),
	//	timestamp: time.Now(),
	//}
	b.Broadcast(Msg1{ID: b.ID(), p1v: b.p1v, slot: b.slot, request: req})
	//for i := 0; i < b.rounds; i++ {
	//	log.Infof("For round: %v", i)
	//	b.Broadcast(Msg1{ID: b.ID(), p1v: b.p1v, round: i})
	//	b.sentMsg1[i] = true
	//}
	log.Infof("Exit Msg1")
}

// all other replicas that receive from leader replica
// will receive msg 1 and broadcast to all other replicas
func(b *Benor) HandleMsg1(m Msg1) {
	//if b.sentMsg1[m.round] == false {
	//	b.Broadcast(Msg1{ID:b.ID(), p1v: b.p1v, round: m.round})
	//}

	log.Infof("Enter Handle Msg 1")
	b.log[m.slot] = &entry{
		command:   m.request.Command,
		commit:    false,
		request:   &m.request,
		quorum1:    b.Q1,
		quorum2: b.Q2,
		timestamp: time.Time{},
	}

	// replica acks the leader msg to the quorum
	b.log[m.slot].quorum1.SampleACK(m.ID, m.p1v)
	// contains p1v value of nodes
	b.Broadcast(Msg2{ID: b.ID(), p1v: b.p1v, slot:m.slot})
	log.Infof("Exit HandleMsg1")
}

func(b *Benor) HandleMsg2(m Msg2) {
	//if b.sentMsg1[m.round] == false {
	//	b.Broadcast(Msg1{ID:b.ID(), p1v: b.p1v, round: m.round})
	//}

	// leader replica can enter here too
	// agreeing on p2v based on p1v
	log.Infof("Enter Handle Msg 2")
	e, k := b.log[m.slot]
	if !k {
		log.Infof("not ok Handle Msg2")
		b.log[m.slot] = &entry{}
	}
	e.quorum1.BenORAck(m.ID, m.p1v)
	if e.quorum1.Majority() {
		log.Infof("Entered the Phase 1 Majority: HandleMsg2")
		if e.quorum1.BenORMajority() == 0 {
			log.Infof("Decided phase 2 value as 0")
			b.p2v = 0
		} else if e.quorum1.BenORMajority() == 1 {
			log.Infof("Decided phase 2 value as 1")
			b.p2v = 1
		} else {
			log.Infof("Decided phase 2 value as -1")
			b.p2v = -1
		}
		// value decided for p2v
		b.Broadcast(Msg3{ID: b.ID(), p2v: b.p2v, slot: m.slot})
	}
	log.Infof("Exit Handle Msg 2")
}

// this function decides the majority for phase 2 value
func(b *Benor) HandleMsg3(m Msg3) {
	log.Infof("Enter Handle Msg3")
	e, ok := b.log[m.slot]

	if !ok{
		log.Infof("not ok: log not found HandleMsg3")
		b.log[m.slot] = &entry{}
	}

	e.quorum2.BenORAck(m.ID, m.p2v)
	log.Infof("SampleACK HandleMsg3")

	if e.quorum2.Majority() {
		if e.quorum2.BenORMajority() == 0 {
			log.Infof("HandleMsg3 final value decided = 0")
			b.decidedValue = 0
			b.decision = true
			b.log[m.slot].commit = true
			b.exec()

		} else if e.quorum2.BenORMajority() == 1 {
			log.Infof("HandleMsg3 final value decided = 1")
			b.decidedValue = 1
			b.decision = true
			b.log[m.slot].commit = true
			b.exec()
		} else {
			// no majority
			log.Infof("Handle Msg 3 no majority reached")
			if e.quorum2.BenOrAtLeastOneValue() == 0 {
				log.Infof("p1v decided as 0 for next round")
				b.p1v = 0
			} else if e.quorum2.BenOrAtLeastOneValue() == 1 {
				log.Infof("p1v decided as 1 for next round")
				b.p1v = 1
			} else {
				log.Infof("Random number decided!")
				rand.Seed(time.Now().UnixNano())
				randomNumber := rand.Intn(100)
				if randomNumber > 50 {
					b.p1v = 1
				} else {
					b.p2v = 0
				}
				//b.Broadcast(Msg2{
				//	ID:   b.ID(),
				//	p1v:  b.p1v,
				//	slot: m.slot,
				//})
			}

		}
	}
}

func (b *Benor) exec() {
	log.Infof("Entered exec block")
	for {
		log.Infof("Inside the execute block for loop")
		e, ok := b.log[b.execute]
		if !ok || !e.commit {
			break
		}

		value := b.Execute(e.command)
		if e.request != nil {
			log.Infof("Sending Reply back to the client!")
			reply := paxi.Reply{
				Command:    e.command,
				Value:      value,
				Properties: make(map[string]string),
			}
			reply.Properties[HTTPHeaderSlot] = strconv.Itoa(b.execute)
			reply.Properties[HTTPHeaderBallot] = e.ballot.String()
			reply.Properties[HTTPHeaderExecute] = strconv.Itoa(b.execute)
			e.request.Reply(reply)
			e.request = nil
		}
		// TODO clean up the log periodically
		delete(b.log, b.execute)
		b.execute++

	}
	log.Infof("Exit exec block!")
}


//	log.Infof("Msg 1 received is %v", m.String())
//	b.Q1.SampleACK(m.ID, m.p1v)
//	log.Infof("Quorum size: %v", b.Q1.Size())
//	// once message received from the "main" replica, should broadcast to all other replica too
//	// This is the all to all communication
//	if !b.msg1Broadcast{
//		b.Broadcast(Msg1{ID: b.ID(), p1v: b.p1v})
//		b.msg1Broadcast = true		// no repeat of broadcast msg
//		log.Infof("I %v sent too all", b.ID())
//	} else{
//
//		// phase 1 msg
//		// wait for replies from N-F nodes
//		// of these check for majority from 0 or 1
//		// set these values as phase 2 values and then again broadcast
//		// checking majority of 0 or 1
//		if b.Q1.BenORMajority() == 0{
//			// p1v majority of 0 reached
//			// set p2v as 0
//			log.Infof("Majority of p1v = 0 received")
//			b.p2v = 0
//		} else if b.Q1.BenORMajority() == 1 {
//			// p1v majority of 1 reached
//			// set p2v as 1
//			log.Infof("Majority of p1v = 1 received")
//			b.p2v = 1
//		} else {
//			// no majority received
//			// set p2v as -1
//			log.Infof("Majority of p1v not reached thus p2v = -1")
//			b.p2v = -1
//		}
//		log.Infof("Exit Handle Msg 1")
//		if !b.msg2Broadcast{
//			b.Msg2()
//		}
//	}
//}


// Msg2 takes care of broadcasting the phase2 value
//func(b *Benor) Msg2(){
//	log.Infof("Enter Msg2")
//	if !b.msg2Broadcast{
//		b.Broadcast(Msg2{ID: b.ID(), p2v: b.p2v})
//		b.msg2Broadcast = true
//	}
//	log.Infof("Exit Msg2")
//}
//
//func(b *Benor) HandleMsg2ToBeHandlesInFuture(m Msg2){
//	log.Infof("Enter Handle Msg 2")
//
//	// HAPPY PATH
//	// wait for n-f msgs from all nodes
//	// of these check if majority between 0 or 1
//	// set that value as final/accepted/decided 0 or 1
//	// done!
//	log.Infof("Msg 2 received is: %v", m.String())
//	b.Q2.SampleACK(m.ID, b.p2v)
//	//log.Infof("Current State: p1v: %v, p2v: %v", b.p1v, b.p2v)
//	log.Infof("Second ACK working!")
//	if b.Q2.Size() > 5/2 {
//		// majority replies
//		if b.Q2.BenORMajority() == 0{
//			log.Infof("Majority of 0 reached")
//			log.Infof("Decided on 0")
//			b.acceptedValue = 0
//			b.decision = true
//		} else if b.Q2.BenORMajority() == 1{
//			log.Infof("Majority of 1 reached")
//			log.Infof("decided on 1")
//			b.acceptedValue = 1
//			b.decision = true
//		}
//		// else condition would be to restart the process
//		// "jolt" the system by randomly choose 1 or 0 for p1v
//		// after that again start from the first i.e send p1v msg
//		// decide majority for p2v
//		// send p2v
//		// decide majority for acceptedValue
//		// process complete
//		// this allows for rounds
//	}
//	// send reply back to client
//	//if b.ID() == b.request.NodeID {
//	//	if b.decision{
//	//		b.request.Reply(paxi.Reply{
//	//			Value: b.request.Command.Value,
//	//			Command: b.request.Command,
//	//			Timestamp: b.request.Timestamp,
//	//		})
//	//	}
//	//}
//
//	if b.currentRound < b.MAXROUNDS {
//		// simulates the rounds in algorithms
//		b.Send(m.ID, Msg3{ID: b.ID(), currentRound: b.currentRound})
//	} else if b.currentRound == b.MAXROUNDS {
//		// Only the replica that received request from the client
//		// can enter this block
//		// pls work
//		if b.isClientRequest {
//			if b.decision{
//				log.Infof("Preparing for next round")
//				log.Infof("Sending Reply back to client!")
//				b.request.Reply(paxi.Reply{
//					Value: b.request.Command.Value,
//					Command: b.request.Command,
//					Timestamp: b.request.Timestamp,
//				})
//			}
//		}
//	}
//
//	log.Infof("Exit Handle Msg 2")
//}
//
//
//// Msg3 will take care of moving the rounds ahead in a lockstep manner
//// once all the rounds are complete only then can the client
//// expect a reply to the request back
//func(b *Benor) Msg3(){
//	log.Infof("Enter Msg3")
//	log.Infof("I'm the standin leader here")
//	log.Infof("i'll broadcast the msg 3 and proceed to the next round")
//	b.Broadcast(Msg3{
//		ID:           b.ID(),
//		currentRound: b.currentRound,
//	})
//}
//
//
//
//func(b *Benor) HandleMsg3(m Msg3){
//	log.Infof("Enter HandleMsg3")
//	if b.isClientRequest {
//		log.Infof("only I can make others to move on to next round") // I AM THE LAW
//		b.Msg3()
//	}
//	if b.currentRound == m.currentRound {
//		b.currentRound++
//		b.msg1Broadcast = !b.msg1Broadcast
//		b.msg2Broadcast = !b.msg2Broadcast
//	}
//	log.Testf("currentRound:%v, msg1broadcast:%v, msg2broadcast:%v", b.currentRound, b.msg1Broadcast, b.msg2Broadcast)
//	log.Infof("Exit HandleMsg3")
//
//	if b.isClientRequest {
//		log.Infof("Starting next round!")
//		b.Msg1()
//	}
//}