package lib

import "container/list"

type Graph struct {
	vertices Set
	from     map[interface{}]Set
	to       map[interface{}]Set
}

func NewGraph() *Graph {
	return &Graph{
		vertices: NewSet(),
		from:     make(map[interface{}]Set),
		to:       make(map[interface{}]Set),
	}
}

func (g *Graph) Size() int {
	return len(g.vertices)
}

func (g *Graph) Has(v interface{}) bool {
	_, exists := g.vertices[v]
	return exists
}

func (g *Graph) Add(v interface{}) {
	if !g.Has(v) {
		g.vertices[v] = struct{}{}
	}
	g.from[v] = NewSet()
	g.to[v] = NewSet()
}

func (g *Graph) Remove(v interface{}) {
	if !g.Has(v) {
		return
	}
	delete(g.vertices, v)

	for u := range g.vertices {
		g.from[u].Remove(v)
		g.to[u].Remove(v)
	}
	delete(g.from, v)
	delete(g.to, v)
}

func (g *Graph) AddEdge(from interface{}, to interface{}) {
	if from == to {
		panic("graph: adding self edge")
	}
	if !g.Has(from) {
		g.Add(from)
	}
	if !g.Has(to) {
		g.Add(to)
	}
	g.from[from].Add(to)
	g.to[to].Add(from)
}

func (g *Graph) Vertices() Set {
	return g.vertices
}

// From returns all vertices in graph that can be reached directly from v
func (g *Graph) From(v interface{}) Set {
	return g.from[v]
}

// To returns all vertices that can reach to v
func (g *Graph) To(v interface{}) Set {
	return g.to[v]
}

// BFS returns breadth first search vertices from a given source
func (g *Graph) BFS(v interface{}) []interface{} {
	vertices := make([]interface{}, 0)
	visited := make(map[interface{}]bool)
	queue := list.New()

	visited[v] = true
	queue.PushBack(v)

	for queue.Len() > 0 {
		s := queue.Front()
		vertices = append(vertices, s.Value)
		queue.Remove(s)

		for t := range g.from[s] {
			if !visited[t] {
				visited[t] = true
				queue.PushBack(t)
			}
		}
	}

	return vertices
}

type color int

const (
	white color = iota // unvisited nodes
	gray               // visiting nodes
	black              // visited nodes
)

func (g *Graph) visit(v interface{}, colors map[interface{}]color) bool {
	colors[v] = gray
	for _, u := range g.from[v].Slice() {
		if colors[u] == gray {
			return true
		}
		if colors[u] == white && g.visit(u, colors) {
			return true
		}
	}
	colors[v] = black
	return false
}

// Cyclic returns true if the graph contains a cycle
func (g *Graph) Cyclic() bool {
	colors := make(map[interface{}]color)
	for v := range g.vertices {
		colors[v] = white
	}

	for v := range g.vertices {
		if colors[v] == white {
			if g.visit(v, colors) {
				return true
			}
		}
	}
	return false
}
