package paxi

import (
	"container/list"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
)

var (
	ErrStateMachineExecution = errors.New("StateMachine execution error")
)

type Key int
type Value []byte

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

// IsRead return true if command operation is GET
func (c Command) IsRead() bool {
	return c.Operation == GET
}

// StateMachine interface provides execution of command against database
// the implementation should be thread safe
type StateMachine interface {
	Execute(c Command) (Value, error)
}

// database maintains the multi-version key-value datastore
type database struct {
	lock *sync.RWMutex
	// data  map[Key]map[Version]Value
	data map[Key]*list.List
	sync.RWMutex
}

// NewStateMachine returns database that impelements StateMachine interface
func NewStateMachine() StateMachine {
	db := new(database)
	db.lock = new(sync.RWMutex)
	db.data = make(map[Key]*list.List)
	return db
}

func (db *database) Execute(c Command) (Value, error) {
	var v Value
	db.RLock()
	if db.data[c.Key] == nil {
		db.data[c.Key] = list.New()
	}
	tail := db.data[c.Key].Back()
	if tail != nil {
		v = tail.Value.(Value)
	}
	db.RUnlock()
	switch c.Operation {
	case PUT:
		db.Lock()
		defer db.Unlock()
		db.data[c.Key].PushBack(c.Value)
		return v, nil
	case GET:
		return v, nil
	case DELETE:
		db.Lock()
		defer db.Unlock()
		delete(db.data, c.Key)
		return v, nil
	}
	return nil, ErrStateMachineExecution
}

func (db *database) String() string {
	db.RLock()
	defer db.RUnlock()
	b, _ := json.Marshal(db.data)
	return string(b)
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
