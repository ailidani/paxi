package slush

import (
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
)

// Slush instance
type Slush struct{
	paxi.Node
	Col int
	IsQuerying bool // True for the querying replica
	accept bool // Final decision variable
	quorum *paxi.Quorum
	noOfSamples int // K value
	request *paxi.Request
}

// NewSlush creates new slush instance
func NewSlush(n paxi.Node, options ...func(*Slush)) *Slush {
	s := &Slush{
		Node:	n,
		Col: -1,
		IsQuerying:false,
		accept:false,
		quorum:paxi.NewQuorum(),
	}

	for _, opt := range options {
		opt(s)
	}

	return s
}

// SetQuerying sets current slush instance as the querying node
func (s *Slush) SetQuerying(querying bool) {
	s.IsQuerying = querying
}

// IsQuerying indicates if this node is a querying node
func (s *Slush) isQuerying() bool {
	return s.IsQuerying
}

// SetColor sets the color of the node
func (s *Slush) SetColor(color int){
	s.Col = color
}

// GetColor returns the current color of the node
func (s *Slush) GetColor()int{
	return s.Col
}

func (s *Slush) isAccepted() bool {
	return s.accept
}

func (s *Slush) setAccepted() {
	log.Infof("Accepted color %v:", s.GetColor())
	s.accept = true
}

// HandleRequest handles request and starts the gossip
func (s* Slush) HandleRequest(r paxi.Request) {
	log.Infof("Enter HandleRequest slush.go")
	s.request = &r
	log.Infof("Received from client: %v, key: %v, value: %s", s.request.Command.ClientID,
		s.request.Command.Key, s.request.Command.Value)
	if s.isAccepted() == true {
		s.accept = false
	}
	s.SetQuerying(true)
	s.SetColor(5)
	s.Msg1()
	log.Infof("Exit HandleRequest slush.go")
}
		// Majority just checks the number of responses		// Majority just checks the number of responses


// Starts gossip by multicasting to the sampled nodes
func (s * Slush) Msg1() {
	log.Infof("Enter Msg1")
	//s.MulticastQuorum(s.noOfSamples, Msg1{ID:s.ID(),Col:s.GetColor()})
	s.Broadcast(Msg1{ID: s.ID(), Col: s.Col})
	log.Infof("Exit Msg1")
}

//
func (s * Slush) HandleMsg1(m Msg1) {
	log.Infof("Enter HandleMsg1")
	if s.isQuerying() == true {
		log.Infof("Querying node got response %v", m.ID)
		// Majority just checks the number of responses
		// but we are interested in the responses of queryColor
		s.quorum.ACK(m.ID)
		if s.quorum.Majority() == true && s.isAccepted() == false {
			s.setAccepted()
			log.Infof("Sending response to the client")
			s.request.Reply(paxi.Reply{
				Value: s.request.Command.Value,
				Command: s.request.Command,
				Timestamp: s.request.Timestamp})
			log.Infof("Exit function")
		}
	} else {
		log.Infof("Non-querying node in handleMsg1")
		log.Infof("Initial Node Color %v:", s.GetColor())
		if s.GetColor() == -1 {
			s.SetColor(m.Col)
		}
		log.Infof("Decided Color %v:", s.GetColor())
		s.Send(m.ID, Msg1{ID:s.ID(), Col:s.GetColor()})
	}
	log.Infof("Exit HandleMsg1")
}