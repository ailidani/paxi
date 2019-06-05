package chain

import (
	"sort"

	"github.com/ailidani/paxi"
)

type Client struct {
	*paxi.HTTPClient
	head paxi.ID
	tail paxi.ID
}

func NewClient() *Client {
	c := new(Client)
	c.HTTPClient = paxi.NewHTTPClient("")

	ids := make([]paxi.ID, 0)
	for id := range paxi.GetConfig().Addrs {
		ids = append(ids, id)
	}
	sort.Sort(paxi.IDs(ids))

	c.head = ids[0]
	c.tail = ids[len(ids)-1]

	return c
}

func (c *Client) Get(key paxi.Key) (paxi.Value, error) {
	v, _, err := c.HTTPClient.RESTGet(c.tail, key)
	return v, err
}

func (c *Client) Put(key paxi.Key, value paxi.Value) error {
	c.HTTPClient.CID++
	_, _, err := c.RESTPut(c.head, key, value)
	return err
}
