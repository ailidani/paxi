package paxi

import (
	"sort"

	"github.com/ailidani/paxi/lib"
)

type History map[int][]operation

func NewHistory() History {
	return make(map[int][]operation)
}

func (h History) Add(key int, input, output interface{}, start, end int64) {
	if _, exists := h[key]; !exists {
		h[key] = make([]operation, 0)
	}
	h[key] = append(h[key], operation{input, output, start, end})
}

func (h History) Linearizable() bool {
	ok := true
	stop := make(chan bool)
	results := make(chan bool)
	for _, partition := range h {
		c := newChecker(stop)
		go func() {
			results <- c.linearizable(partition)
		}()
	}
	for range h {
		ok = <-results
		if !ok {
			close(stop)
			break
		}
	}
	return ok
}

// A simple linearizability checker based on https://pdos.csail.mit.edu/6.824/papers/fb-consistency.pdf

type operation struct {
	input  interface{}
	output interface{}
	// timestamps
	start int64
	end   int64
}

func (a operation) happenBefore(b operation) bool {
	return a.end < b.start
}

func (a operation) concurrent(b operation) bool {
	return !a.happenBefore(b) && !b.happenBefore(a)
}

type checker struct {
	*lib.Graph
	stop chan bool
}

func newChecker(stop chan bool) *checker {
	return &checker{
		Graph: lib.NewGraph(),
		stop:  stop,
	}
}

func (c *checker) add(o operation) {
	if c.Graph.Has(o) {
		// already in graph from lookahead
		return
	}
	c.Graph.Add(o)
	for v := range c.Graph.Vertices() {
		if v.(operation).happenBefore(o) {
			c.AddEdge(o, v)
		}
	}
}

func (c *checker) remove(o operation) {
	c.Remove(o)
}

func (c *checker) clear() {
	c.Graph = lib.NewGraph()
}

// match finds the first matching write operation to the given read operation
func (c *checker) match(o operation) *operation {
	for _, v := range c.Graph.BFS(o) {
		if o.output == v.(operation).input {
			match := v.(operation)
			return &match
		}
	}
	return nil
}

// matched write inherits edges read
func (c *checker) merge(read, write operation) {
	for s := range c.From(read) {
		if s.(operation) != write {
			c.Graph.AddEdge(write, s.(operation))
		}
	}

	// refine response time of merged vertex
	if read.end < write.end {
		write.end = read.end
	}
	c.Graph.Remove(read)
}

func (c *checker) linearizable(history []operation) bool {
	c.clear()
	sort.Sort(byTime(history))
	for i, o := range history {
		select {
		case <-c.stop:
			return false
		default:
			c.add(o)
			// o is read operation
			if o.input == nil {
				// look ahead for concurrent writes
				for j := i + 1; j < len(history) && o.concurrent(history[j]); j++ {
					// next operation is write
					if history[j].output == nil {
						c.Graph.Add(history[j])
					}
				}
				match := c.match(o)
				if match != nil {
					c.merge(o, *match)
				}
				if c.Graph.Cyclic() {
					return false
				}
			}
		}
	}
	return true
}

// sort operations by invocation time
type byTime []operation

func (a byTime) Len() int           { return len(a) }
func (a byTime) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byTime) Less(i, j int) bool { return a[i].start < a[j].start }
