package paxos

import (
	"strconv"

	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
)

// Client overwrites read operation for Paxos
type Client struct {
	*paxi.HTTPClient
	ballot paxi.Ballot
}

func NewClient(id paxi.ID) *Client {
	return &Client{
		HTTPClient: paxi.NewHTTPClient(id),
	}
}

// Get implements paxi.Client interface
// there are three reading modes:
// (1) read as normal command
// (2) read from leader with current ballot number
// (3) read from quorum of replicas with barrier
func (c *Client) Get(key paxi.Key) (paxi.Value, error) {
	c.HTTPClient.CID++
	if *readLeader {
		if c.ballot == 0 {
			v, meta, err := c.HTTPClient.RESTGet(c.ID, key)
			c.ballot = paxi.NewBallotFromString(meta[HTTPHeaderBallot])
			return v, err
		}
		// check ballot number
		v, meta, err := c.HTTPClient.RESTGet(c.ballot.ID(), key)
		b := paxi.NewBallotFromString(meta[HTTPHeaderBallot])
		if b > c.ballot {
			c.ballot = b
		}
		return v, err
	} else if *readQuorum {
		majority := c.N/2 + 1
		barrier := -1
		numReachedBarrier := 0
		var value paxi.Value

		// quorum read
		values, metadatas := c.QuorumGet(key)
		for i, v := range values {
			slot, err := strconv.Atoi(metadatas[i][HTTPHeaderSlot])
			if err != nil {
				log.Error(err)
				continue
			}
			if slot > barrier {
				barrier = slot
				numReachedBarrier = 1
				value = v
			} else if slot == barrier {
				numReachedBarrier++
			}
		}

		log.Infof("numReachedBarrier = %d", numReachedBarrier)

	loop:
		// barrier on largest slot number
		for numReachedBarrier < majority {
			_, metadatas := c.HTTPClient.MultiGet(majority-numReachedBarrier, key)
			for _, meta := range metadatas {
				execute, err := strconv.Atoi(meta[HTTPHeaderExecute])
				if err != nil {
					log.Error(err)
					continue
				}
				if execute >= barrier {
					break loop
				}

				slot, err := strconv.Atoi(meta[HTTPHeaderSlot])
				if err != nil {
					log.Error(err)
					continue
				}
				if slot >= barrier {
					numReachedBarrier++
				}
			}
		}

		return value, nil
	} else {
		return c.HTTPClient.Get(key)
	}
}

func (c *Client) Put(key paxi.Key, value paxi.Value) error {
	c.CID++
	_, meta, err := c.RESTPut(c.ID, key, value)
	b := paxi.NewBallotFromString(meta[HTTPHeaderBallot])
	if b > c.ballot {
		c.ballot = b
	}
	return err
}
