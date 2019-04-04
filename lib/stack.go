package lib

// Stack implements the stack data structure backed by single linked list
type Stack struct {
	top    *stackNode
	length int
}
type stackNode struct {
	value interface{}
	prev  *stackNode
}

// NewStack creates a new Stack
func NewStack() *Stack {
	return new(Stack)
}

// Len returns the number of items in the stack
func (s *Stack) Len() int {
	return s.length
}

// Peek views the top item on the stack
func (s *Stack) Peek() interface{} {
	if s.length == 0 {
		return nil
	}
	return s.top.value
}

// Pop the top item of the stack and return it
func (s *Stack) Pop() interface{} {
	if s.length == 0 {
		return nil
	}

	n := s.top
	s.top = n.prev
	s.length--
	return n.value
}

// Push a value onto the top of the stack
func (s *Stack) Push(value interface{}) {
	n := &stackNode{value, s.top}
	s.top = n
	s.length++
}

func (s *Stack) Empty() bool {
	return s.length == 0
}
