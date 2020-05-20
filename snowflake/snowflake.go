package snowflake

import (
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
	"time"
)

// Log entry
type entry struct {
	ballot paxi.Ballot
	command paxi.Command
	commit bool
	request *paxi.Request
	quorum *paxi.Quorum
	timestamp time.Time
}

// Slush instance
type Snowflake struct{
	paxi.Node

	log map[int]*entry // log ordered by slot
	slot int // slot number for the requests
	Col int
	IsQuerying bool // True for the querying replica
	accept bool // Final decision variable
	quorum *paxi.Quorum
	quorum_ *paxi.Quorum
	noOfSamples int // K value
	request *paxi.Request
	majorityInSamples int
	samples [5]int
	convictionCnt int
	maj bool
	Beta int // Threshold of conviction count to accept a value
}

// NewSlush creates new slush instance
func NewSnowflake(n paxi.Node, options ...func(*Snowflake)) *Snowflake {
	s := &Snowflake{
		Node:	n,
		log: make(map[int]*entry, paxi.GetConfig().BufferSize),
		slot: -1,
		Col: -1,
		IsQuerying:false,
		accept:false,
		quorum:paxi.NewQuorum(),
		quorum_: paxi.NewQuorum(),
		noOfSamples: 3,
		majorityInSamples: 0,
		convictionCnt: 0,
		maj: false,
		Beta: 2,
	}

	for _, opt := range options {
		opt(s)
	}

	/* Initializes the color for the node
	   0 - Red, 1 - Blue, -1 - Uncolored
	*/
	if n.ID().Node() %	2 == 0 {
		s.Col = 1
	}

	for i := 0; i < 5; i++ {
		s.samples[i] = 0
	}
	return s
}

// Set Majority
func (s *Snowflake) SetMajority(majority bool) {
	s.maj = majority
}

// Get Majority
func (s *Snowflake) GetMajority() bool {
	return s.maj
}

// SetQuerying sets current slush instance as the querying node
func (s *Snowflake) SetQuerying(querying bool) {
	s.IsQuerying = querying
}

// IsQuerying indicates if this node is a querying node
func (s *Snowflake) isQuerying() bool {
	return s.IsQuerying
}

// SetColor sets the color of the node
func (s *Snowflake) SetColor(color int){
	s.Col = color
}

// GetColor returns the current color of the node
func (s *Snowflake) GetColor()int{
	return s.Col
}

func (s *Snowflake) isAccepted() bool {
	return s.accept
}

func (s *Snowflake) setAccepted() {
	log.Infof("Accepted color %v:", s.GetColor())
	s.accept = true
}

/**
 * HandleRequest handles the request from the client
 * and starts the gossip protocol here
 */
func (s* Snowflake) HandleRequest(r paxi.Request) {
	log.Infof("Enter HandleRequest Snowflake.go")
	s.request = &r
	log.Infof("Received from client: %v, key: %v, value: %v", s.request.Command.ClientID,
		s.request.Command.Key, s.request.Command.Value)
	if s.isAccepted() == true {
		s.accept = false
	}
	s.SetQuerying(true)
	s.SetColor(0) /* Mapping color to the client request */
	s.Msg1()
	log.Infof("Exit HandleRequest Snowflake.go")
}

// Majority just checks the number of responses		// Majority just checks the number of responses

/*
Starts gossip by multicasting to the random sample of nodes
 */
func (s * Snowflake) Msg1() {
	log.Infof("Enter Msg1")
	s.slot++
	s.log[s.slot] = &entry {
		request: s.request,
		quorum: paxi.NewQuorum(),
		timestamp: time.Now(),
	}

	/* Add randomness here, send the query to the random sample of nodes
	 * Use s.Send(); to send to each node in the random sample
	 */
	for i := 0; i < s.noOfSamples; i++ {
		log.Infof("For round: %v", i)
		s.MulticastToSample((i % 3) + 1, Msg1{ID: s.ID(), Col: s.Col})
	}
	log.Infof("Exit Msg1")
}

func (s * Snowflake) Msg2() {
	log.Infof("Enter Msg2")

	for i := 0; i < s.noOfSamples; i++ {
		log.Infof("For round: %v", i)
		s.MulticastToSample((i % 3) + 1, Msg2{ID: s.ID(), Col: s.Col})
	}
	log.Infof("Exit Msg2")
}

func (s* Snowflake) Msg3() {
	log.Infof("Enter Msg3")
	s.Broadcast(Msg3{ID: s.ID()})
	log.Infof("Exit Msg3")
}

/*
This method is called on both the querying nodes and other nodes
involved in the consensus, querying nodes will get responses from their
samples and uncolored non-querying nodes will initiate their own queries
by using s.Msg2(), after adapting the color.
If a node is colored already it simply responds back with its own color
 */
func (s * Snowflake) HandleMsg1(m Msg1) {
	log.Infof("Enter HandleMsg1")
	if s.isQuerying() == true {
		log.Infof("Querying node got response %v", m.ID)
		// Majority just checks the number of responses
		// but we are interested in the responses of queryColor
		s.quorum.SampleACK(m.ID, m.Col)
		for i := 0; i < s.noOfSamples; i++ {
			/* We check if we already attained majority for a particular sample
				if yes, then we just record the response
			 */
			log.Infof("Checking majority for sample ID: %v", i)
			if true == s.quorum.SampleMajority((i%3)+1) {
				log.Infof("Got majority from sample ID: %v , majority color: %v", i,
					s.quorum.SampleMajorityColor((i%3)+1))
				if s.samples[i%3+1] == 0 { /* Majority is attained for this sample */
					s.samples[i%3+1] = 1
					s.convictionCnt++
					log.Infof("Conviction counter %v", s.convictionCnt)
					s.SetMajority(true)
				}
				if s.Col != s.quorum.SampleMajorityColor((i%3)+1) {
					s.SetColor(s.quorum.SampleMajorityColor((i % 3) + 1))
					log.Infof("Color is flipped to the majority color: %v",
						s.quorum.SampleMajorityColor((i%3)+1))
					s.convictionCnt = 1
					log.Infof("Conviction counter %v", s.convictionCnt)
				} else { /* Majority color is equal to the querying color */
					/* No need to flip my color, increment the conviction counter for this color */
					log.Infof("Color is not flipped")

					if s.convictionCnt >= s.Beta {
						log.Infof("Conviction counter %v is greater than Beta: %v", s.convictionCnt, s.Beta)
						s.setAccepted()
						/* Reset the Quorum information since the consensus instance is complete */
						s.quorum.Reset()
						s.quorum_.Reset()
						for i := 0; i < 5; i++ {
							s.samples[i] = 0
						}
						s.convictionCnt = 0
						s.SetMajority()
						/* Send reset message to other nodes, so that they are ready for the next consensus
						instance */
						s.Msg3()
						log.Infof("Sending response to the client")
						s.request.Reply(paxi.Reply{
							Value: s.request.Command.Value,
							Command: s.request.Command,
							Timestamp: s.request.Timestamp})


					}
				}
			}
		}
	} else {
		log.Infof("Non-querying node in handleMsg1")
		log.Infof("Initial Node Color %v:", s.GetColor())
		if s.GetColor() == -1 {
			s.SetColor(m.Col)
			log.Infof("Decided Color %v:", s.GetColor())
			s.Send(m.ID, Msg1{ID:s.ID(), Col:s.GetColor()})
			/* Now the node should start its own rounds and send the query to yet another random
			sample of nodes.
			Now start my own query!!
			 */
			/*s.Msg1()
			s.Msg2()*/
		} else {
			log.Infof("Color stays same %v:", s.GetColor())
			s.Send(m.ID, Msg1{ID:s.ID(), Col:s.GetColor()})
		}
	}
	log.Infof("Exit HandleMsg1")
}

/*
This function is run by the nodes who initiate their own
queries and send them to another random sample of nodes
if the responses received from the sample are of different
majority than its own color, the node flips its color
 */
func (s* Snowflake) HandleMsg2(m Msg2) {
	log.Infof("Enter HandleMsg2")
	log.Infof("Initial Node Color %v:", s.GetColor())

	if s.GetColor() == -1 {
		s.SetColor(m.Col)
		log.Infof("Decided Color %v:", s.GetColor())
		s.Send(m.ID, Msg2{ID:s.ID(), Col:s.GetColor()})
	} else {
		s.quorum_.SampleACK(m.ID, m.Col)
		for i := 0; i < s.noOfSamples; i++ {
			if s.Col != s.quorum_.SampleMajorityColor((i%3)+1) {
				s.SetColor(s.quorum_.SampleMajorityColor((i % 3) + 1))
				log.Infof("Color is flipped to the majority color: %v", s.quorum_.SampleMajorityColor((i%3)+1))
			} else { /* Majority color is equal to the querying color */
				/* No need to flip my color */
				log.Infof("Color is not flipped")
				log.Infof("Color stays same %v:", s.GetColor())
			}
		}
		s.Send(m.ID, Msg2{ID:s.ID(), Col:s.GetColor()})
	}
	log.Infof("Exit HandleMsg2")
}

func (s * Snowflake) HandleMsg3(m Msg3) {
	log.Infof("Enter HandleMsg3")
	if s.ID().Node() %	2 == 0 {
		s.Col = 1
	} else {
		s.Col = -1
	}
	log.Infof("Exit HandleMsg3")
}