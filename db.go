package paxi

import (
	"container/list"
	"errors"
	"fmt"
	"sync"
)

var (
	ErrStateMachineExecution = errors.New("StateMachine execution error")
)

type Key int
type Value []byte
type Version int

type Operation uint8

const (
	NOOP Operation = iota
	PUT
	GET
	DELETE
	RLOCK
	RUNLOCK
	WLOCK
	WUNLOCK
)

type Command struct {
	Operation Operation
	Key       Key
	Value     Value
}

func (c Command) String() string {
	if c.Operation == GET {
		return fmt.Sprintf("Get{key=%v}", c.Key)
	}
	return fmt.Sprintf("Put{key=%v, val=%v}", c.Key, c.Value)
}

func (c *Command) IsRead() bool {
	return c.Operation == GET
}

// StateMachine maintains the multi-version key-value data store
type StateMachine struct {
	lock *sync.RWMutex
	// data  map[Key]map[Version]Value
	data map[Key]*list.List
	sync.RWMutex
}

func NewStateMachine() *StateMachine {
	s := new(StateMachine)
	s.lock = new(sync.RWMutex)
	s.data = make(map[Key]*list.List)
	return s
}

func (s *StateMachine) Execute(c Command) (Value, error) {
	var v Value
	s.RLock()
	if s.data[c.Key] == nil {
		s.data[c.Key] = list.New()
	}
	tail := s.data[c.Key].Back()
	if tail != nil {
		v = tail.Value.(Value)
	}
	s.RUnlock()
	switch c.Operation {
	case PUT:
		s.Lock()
		defer s.Unlock()
		s.data[c.Key].PushBack(c.Value)
		return v, nil
	case GET:
		return v, nil
	case DELETE:
		s.Lock()
		defer s.Unlock()
		delete(s.data, c.Key)
		return v, nil
	}
	return nil, ErrStateMachineExecution
}

func Conflict(gamma *Command, delta *Command) bool {
	if gamma.Key == delta.Key {
		if gamma.Operation == PUT || delta.Operation == PUT {
			return true
		}
	}
	return false
}

func ConflictBatch(batch1 []Command, batch2 []Command) bool {
	for i := 0; i < len(batch1); i++ {
		for j := 0; j < len(batch2); j++ {
			if Conflict(&batch1[i], &batch2[j]) {
				return true
			}
		}
	}
	return false
}
