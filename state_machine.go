package paxi

// StateMachine defines a deterministic state machine
type StateMachine interface {
	// Execute is the state-transition function
	// returns current state value if state unchanged or previous state value
	Execute(interface{}) interface{}
}

type State interface {
	Hash() uint64
}
