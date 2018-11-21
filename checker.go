package paxi

import (
	"sort"

	"github.com/ailidani/paxi/lib"
)

// A simple linearizability checker based on https://pdos.csail.mit.edu/6.824/papers/fb-consistency.pdf

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
			//c.AddEdge(o, v)
			c.AddEdge(v, o)
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
	//for _, v := range c.Graph.BFSReverse(read) {
	for v := range c.Graph.Vertices() {
		if read.output == v.(*operation).input {
			return v.(*operation)
		}
	}
	return nil
}

// matched write inherits edges read
func (c *checker) merge(read, write *operation) {
	for s := range c.To(read) {
		if s.(*operation) != write {
			c.Graph.AddEdge(s.(*operation), write)
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
					c.add(history[j])
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
						if c.Graph.From(u).Has(v) && u.(*operation).start > v.(*operation).end {
							c.Graph.RemoveEdge(u, v)
						}
					}
				}
			}
		}
	}
	return anomaly
}
