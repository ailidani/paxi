package blockchain

import (
	"bytes"
	"encoding/gob"

	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
)

// Miner is the maintainer of blockchain
type Miner struct {
	paxi.Node

	blockchain map[uint64]*Block
	chain      *Block
	// requests   map[uint64]*paxi.Request
	pool  []*paxi.Request
	index uint64
}

// NewMiner creates new Miner as paxi node
func NewMiner(id paxi.ID) *Miner {
	miner := &Miner{
		Node:       paxi.NewNode(id),
		blockchain: make(map[uint64]*Block),
		// chain:      Genesis(),
		pool: make([]*paxi.Request, 0),
		// requests:   make(map[uint64]*paxi.Request),
	}
	if id == "1.1" {
		miner.chain = Genesis()
	}
	miner.Node.Register(paxi.Request{}, miner.handleRequest)
	miner.Node.Register(Block{}, miner.handleBlock)
	return miner
}

func (m *Miner) handleRequest(r paxi.Request) {
	buf := new(bytes.Buffer)
	encoder := gob.NewEncoder(buf)
	err := encoder.Encode(r.Command)
	if err != nil {
		log.Error("gob encode error: ", err)
		return
	}
	block := m.blockchain[m.index].Next(buf.Bytes())
	m.blockchain[block.Index] = block
	// m.requests[block.Index] = &r
	m.index = block.Index
	r.Reply(paxi.Reply{
		Command: r.Command,
	})
}

func (m *Miner) handleBlock(b Block) {
}
