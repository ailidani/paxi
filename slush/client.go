package slush

import (
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
)

type Client struct {
	*paxi.HTTPClient
	Col int
}

func NewClient(id paxi.ID) *Client {
	return &Client{
		HTTPClient: paxi.NewHTTPClient(id),
	}
}

func (c *Client) Put(key paxi.Key, value paxi.Value) error {
	c.HTTPClient.CID++
	log.Infof("Client sending put request")
	key= paxi.Key(1234)
	value = paxi.Value("5")
	_, _, err := c.RESTPut(c.ID, key, value)
	log.Infof("Client exiting put request")
	return err
}

func (c *Client) Get(key paxi.Key) (paxi.Value, error) {
	c.HTTPClient.CID++
	log.Debugf("Client entering Get")
	log.Debugf("Client exiting Get")
	return c.readAny(1234)
}

func (c *Client) readAny(key paxi.Key) (paxi.Value, error) {
	log.Debugf("Client entering readAny")
	v, _, err := c.HTTPClient.RESTGet(c.ID, key)
	log.Debugf("Client exiting readAny")
	return v, err
}

