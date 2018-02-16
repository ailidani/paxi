package blockchain

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/gob"
	"math"

	"github.com/ailidani/paxi/log"
)

// PREFIX is the leading zeros in hash value as proof of work
var PREFIX = []byte("0000")

func init() {
	gob.Register(Block{})
}

// Block contains some amount of data in blockchain
type Block struct {
	Index uint64
	Nonce uint64
	Data  []byte
	Prev  []byte // previous block hash
	Hash  []byte // current block hash

	next *Block
}

// Next generates the next block given some data
func (b *Block) Next(data []byte) *Block {
	next := &Block{
		Index: b.Index + 1,
		Data:  data,
		Prev:  b.Hash,
	}
	b.next = next
	h := sha256.New()
	h.Write(next.bytes())
	for i := uint64(0); i <= math.MaxUint64; i++ {
		t := h
		err := binary.Write(t, binary.LittleEndian, i)
		if err != nil {
			log.Error("binary write failed: ", err)
			return nil
		}
		thash := t.Sum(nil)
		if bytes.HasPrefix(thash, PREFIX) {
			next.Nonce = i
			next.Hash = thash
			log.Debugf("Nonce found %d", i)
			return next
		}
	}
	return nil
}

func (b *Block) bytes() []byte {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, b.Index)
	if err != nil {
		log.Error("binary write failed: ", err)
		return nil
	}
	buf.Write(b.Data)
	buf.Write(b.Prev)
	return buf.Bytes()
}
