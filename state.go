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

type Key int64
type Value int64
type Version int

const NIL Value = 0

type Operation uint8

const (
	NOOP Operation = iota
	PUT
	GET
	DELETE
	RLOCK
	WLOCK
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
	lock  *sync.Mutex
	data  map[Key]map[Version]Value
	data2 *MMap
	data3 map[Key]*list.List
	sync.RWMutex
}

func NewStateMachine() *StateMachine {
	return &StateMachine{
		lock:  new(sync.Mutex),
		data:  make(map[Key]map[Version]Value),
		data2: NewMMap(),
		data3: make(map[Key]*list.List),
	}
}

func versions(m map[Version]Value) []Version {
	versions := make([]Version, len(m))
	i := 0
	for v := range m {
		versions[i] = v
		i++
	}
	return versions
}

func (s *StateMachine) maxVersion(key Key) Version {
	max := 0
	for v := range s.data[key] {
		if int(v) >= max {
			max = int(v)
		}
	}
	return Version(max)
}

func (s *StateMachine) Execute(commands ...Command) (Value, error) {
	s.Lock()
	defer s.Unlock()
	for _, c := range commands {
		switch c.Operation {
		case PUT:
			if s.data[c.Key] == nil {
				s.data[c.Key] = make(map[Version]Value)
				s.data[c.Key][0] = NIL
			}
			v := s.maxVersion(c.Key) + 1
			s.data[c.Key][v] = c.Value
			return c.Value, nil
		case GET:
			if value, present := s.data[c.Key]; present {
				return value[s.maxVersion(c.Key)], nil
			}
		}
	}
	return NIL, ErrStateMachineExecution
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
