package slush

import "github.com/ailidani/paxi"

type Client struct {
	*paxi.HTTPClient
	Col int
}

func NewClient(id paxi.ID) *Client {
	return &Client{
		HTTPClient: paxi.NewHTTPClient(id),
	}
}

func (c *Client) Put() {
	c.HTTPClient.CID++
	key:= paxi.Key(1234)
	value:= paxi.Value("RED")
	c.RESTPut(c.ID, key, value)
}

func (c *Client) Get() (paxi.Value, error) {
	c.HTTPClient.CID++
	return c.readAny(1234)
}

func (c *Client) readAny(key paxi.Key) (paxi.Value, error) {
	v, _, err := c.HTTPClient.RESTGet(c.ID, key)
	return v, err
}

