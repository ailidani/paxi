package paxi

import (
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
	return fmt.Sprintf("Put{key=%v, val=%x}", c.Key, c.Value)
}

// IsRead return true if command operation is GET
func (c Command) IsRead() bool {
	return c.Operation == GET
}

// Database interface provides execution of command against database
// the implementation should be thread safe
type Database interface {
	Execute(Command) (Value, error)
	Version(Key) int
	History(Key) []Value
}

// database maintains the multi-version key-value datastore
type database struct {
	lock *sync.RWMutex
	// data map[Key]map[Version]Value
	// data map[Key]*list.List
	data map[Key][]Value
	sync.RWMutex
}

// NewDatabase returns database that impelements Database interface
func NewDatabase() Database {
	db := new(database)
	db.lock = new(sync.RWMutex)
	db.data = make(map[Key][]Value)
	return db
}

// Execute implements Database interface
func (db *database) Execute(c Command) (Value, error) {
	db.Lock()
	defer db.Unlock()
	if db.data[c.Key] == nil {
		db.data[c.Key] = make([]Value, 0)
	}
	var v Value
	l := len(db.data[c.Key])
	if l > 0 {
		v = db.data[c.Key][l-1]
	}
	switch c.Operation {
	case PUT:
		db.data[c.Key] = append(db.data[c.Key], c.Value)
		return v, nil
	case GET:
		return v, nil
	case DELETE:
		delete(db.data, c.Key)
		return v, nil
	}
	return nil, ErrStateMachineExecution
}

// Version implements Database interface
func (db *database) Version(k Key) int {
	db.RLock()
	defer db.RUnlock()
	return len(db.data[k])
}

func (db *database) History(k Key) []Value {
	db.RLock()
	defer db.RUnlock()
	return db.data[k]
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
