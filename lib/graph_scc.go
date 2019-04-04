package lib

// SCC find SCCs using Tarjan's algorithm
func (g *Graph) SCC() [][]interface{} {
	data := &data{
		graph: g.from,
		nodes: make([]node, 0, len(g.from)),
		index: make(map[interface{}]int, len(g.from)),
	}
	for v := range data.graph {
		if _, ok := data.index[v]; !ok {
			data.strongConnect(v)
		}
	}
	return data.output
}

// data contains all common data for a single operation.
type data struct {
	graph  map[interface{}]Set
	nodes  []node
	stack  []interface{}
	index  map[interface{}]int
	output [][]interface{}
}

// node stores data for a single vertex in the connection process.
type node struct {
	lowlink int
	stacked bool
}

// strongConnect runs Tarjan's algorithm recursivley and outputs a grouping of
// strongly connected vertices.
func (data *data) strongConnect(v interface{}) *node {
	index := len(data.nodes)
	data.index[v] = index
	data.stack = append(data.stack, v)
	data.nodes = append(data.nodes, node{lowlink: index, stacked: true})
	node := &data.nodes[index]

	for _, w := range data.graph[v] {
		i, seen := data.index[w]
		if !seen {
			n := data.strongConnect(w)
			if n.lowlink < node.lowlink {
				node.lowlink = n.lowlink
			}
		} else if data.nodes[i].stacked {
			if i < node.lowlink {
				node.lowlink = i
			}
		}
	}

	if node.lowlink == index {
		var vertices []interface{}
		i := len(data.stack) - 1
		for {
			w := data.stack[i]
			stackIndex := data.index[w]
			data.nodes[stackIndex].stacked = false
			vertices = append(vertices, w)
			if stackIndex == index {
				break
			}
			i--
		}
		data.stack = data.stack[:i]
		data.output = append(data.output, vertices)
	}

	return node
}
