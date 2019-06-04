package paxi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sync"
)

// Key type of the key-value database
// TODO key should be general too
type Key int

// Value type of key-value database
type Value []byte

// Command of key-value database
type Command struct {
	Key       Key
	Value     Value
	ClientID  ID
	CommandID int
}

func (c Command) Empty() bool {
	if c.Key == 0 && c.Value == nil && c.ClientID == "" && c.CommandID == 0 {
		return true
	}
	return false
}

func (c Command) IsRead() bool {
	return c.Value == nil
}

func (c Command) IsWrite() bool {
	return c.Value != nil
}

func (c Command) Equal(a Command) bool {
	return c.Key == a.Key && bytes.Equal(c.Value, a.Value) && c.ClientID == a.ClientID && c.CommandID == a.CommandID
}

func (c Command) String() string {
	if c.Value == nil {
		return fmt.Sprintf("Get{key=%v id=%s cid=%d}", c.Key, c.ClientID, c.CommandID)
	}
	return fmt.Sprintf("Put{key=%v value=%x id=%s cid=%d", c.Key, c.Value, c.ClientID, c.CommandID)
}

// Database defines a database interface
// TODO replace with more general StateMachine interface
type Database interface {
	Execute(Command) Value
	History(Key) []Value
	Get(Key) Value
	Put(Key, Value)
}

// Database implements a multi-version key-value datastore as the StateMachine
type database struct {
	sync.RWMutex
	data         map[Key]Value
	version      int
	multiversion bool
	history      map[Key][]Value
}

// NewDatabase returns database that impelements Database interface
func NewDatabase() Database {
	return &database{
		data:         make(map[Key]Value),
		version:      0,
		multiversion: config.MultiVersion,
		history:      make(map[Key][]Value),
	}
}

/*
// Execute implements StateMachine interface
func (d *database) Execute(c interface{}) interface{} {
	cmd, ok := c.(Command)
	if !ok {
		log.Error("cannot execute non command")
	}
	k := cmd.Key
	v := cmd.Value
	d.Lock()
	defer d.Unlock()
	if d.data[k] == nil {
		d.data[k] = make([]Value, 0)
	}
	d.data[k] = append(d.data[k], v)
	version := len(d.data[k])
	if version < 2 {
		return nil
	}
	return d.data[k][version-2]
}
*/

// Execute executes a command agaist database
func (d *database) Execute(c Command) Value {
	d.Lock()
	defer d.Unlock()

	// get previous value
	v := d.data[c.Key]

	// writes new value
	d.put(c.Key, c.Value)

	return v
}

// Get gets the current value and version of given key
func (d *database) Get(k Key) Value {
	d.RLock()
	defer d.RUnlock()
	return d.data[k]
}

func (d *database) put(k Key, v Value) {
	if v != nil {
		d.data[k] = v
		d.version++
		if d.multiversion {
			if d.history[k] == nil {
				d.history[k] = make([]Value, 0)
			}
			d.history[k] = append(d.history[k], v)
		}
	}
}

// Put puts a new value of given key
func (d *database) Put(k Key, v Value) {
	d.Lock()
	defer d.Unlock()
	d.put(k, v)
}

// Version returns current version of given key
func (d *database) Version(k Key) int {
	d.RLock()
	defer d.RUnlock()
	return d.version
}

// History returns entire vlue history in order
func (d *database) History(k Key) []Value {
	d.RLock()
	defer d.RUnlock()
	return d.history[k]
}

func (d *database) String() string {
	d.RLock()
	defer d.RUnlock()
	b, _ := json.Marshal(d.data)
	return string(b)
}

// Conflict checks if two commands are conflicting as reorder them will end in different states
func Conflict(gamma *Command, delta *Command) bool {
	if gamma.Key == delta.Key {
		if !gamma.IsRead() || !delta.IsRead() {
			return true
		}
	}
	return false
}

// ConflictBatch checks if two batchs of commands are conflict
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
