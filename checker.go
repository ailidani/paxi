package paxi

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"sync"

	"github.com/ailidani/paxi/lib"
)

// History client operation history mapped by key
type History struct {
	sync.RWMutex
	data map[int][]*operation
}

// NewHistory creates a History map
func NewHistory() *History {
	return &History{
		data: make(map[int][]*operation),
	}
}

// Add puts an operation in History
func (h *History) Add(key int, input, output interface{}, start, end int64) {
	h.Lock()
	defer h.Unlock()
	if _, exists := h.data[key]; !exists {
		h.data[key] = make([]*operation, 0)
	}
	h.data[key] = append(h.data[key], &operation{input, output, start, end})
}

// Linearizable concurrently checks if each partition of the history is linearizable and returns the total number of anomaly reads
func (h *History) Linearizable() int {
	anomalies := make(chan []*operation)
	h.RLock()
	defer h.RUnlock()
	for _, partition := range h.data {
		c := newChecker()
		go func(p []*operation) {
			anomalies <- c.linearizable(p)
		}(partition)
	}
	sum := 0
	for range h.data {
		a := <-anomalies
		sum += len(a)
	}
	return sum
}

// WriteFile writes entire operation history into file
func (h *History) WriteFile(path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	w := bufio.NewWriter(file)
	h.RLock()
	defer h.RUnlock()
	for k, ops := range h.data {
		fmt.Fprintf(w, "key=%d\n", k)
		for _, o := range ops {
			fmt.Fprintln(w, o)
		}
	}
	return w.Flush()
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

func (a operation) equal(b operation) bool {
	return a.input == b.input && a.output == b.output && a.start == b.start && a.end == b.end
}

func (o operation) String() string {
	return fmt.Sprintf("{input=%v, output=%v, start=%d, end=%d}", o.input, o.output, o.start, o.end)
}

type checker struct {
	*lib.Graph
}

func newChecker() *checker {
	return &checker{
		Graph: lib.NewGraph(),
	}
}

func (c *checker) add(o *operation) {
	if c.Graph.Has(o) {
		// already in graph from lookahead
		return
	}
	c.Graph.Add(o)
	for v := range c.Graph.Vertices() {
		if v.(*operation).happenBefore(*o) {
			c.AddEdge(o, v)
		}
	}
}

func (c *checker) remove(o *operation) {
	c.Remove(o)
}

func (c *checker) clear() {
	c.Graph = lib.NewGraph()
}

// match finds the first matching write operation to the given read operation
func (c *checker) match(read *operation) *operation {
	// for _, v := range c.Graph.BFS(read) {
	for v := range c.Graph.Vertices() {
		if read.output == v.(*operation).input {
			return v.(*operation)
		}
	}
	return nil
}

// matched write inherits edges read
func (c *checker) merge(read, write *operation) {
	for s := range c.From(read) {
		if s.(*operation) != write {
			c.Graph.AddEdge(write, s.(*operation))
		}
	}

	// refine response time of merged vertex
	if read.end < write.end {
		write.end = read.end
	}
	c.Graph.Remove(read)
}

func (c *checker) linearizable(history []*operation) []*operation {
	c.clear()
	sort.Sort(byTime(history))
	anomaly := make([]*operation, 0)
	for i, o := range history {
		c.add(o)
		// o is read operation
		if o.input == nil {
			// look ahead for concurrent writes
			for j := i + 1; j < len(history) && o.concurrent(*history[j]); j++ {
				// next operation is write
				if history[j].output == nil {
					c.Graph.Add(history[j])
				}
			}
			match := c.match(o)
			if match != nil {
				c.merge(o, match)
			}
			cycle := c.Graph.Cycle()
			if cycle != nil {
				anomaly = append(anomaly, o)
				for _, u := range cycle {
					for _, v := range cycle {
						if c.Graph.From(u).Has(v) && v.(*operation).start > u.(*operation).end {
							c.Graph.RemoveEdge(u, v)
						}
					}
				}
			}
		}
	}
	return anomaly
}

// sort operations by invocation time
type byTime []*operation

func (a byTime) Len() int           { return len(a) }
func (a byTime) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byTime) Less(i, j int) bool { return a[i].start < a[j].start }
