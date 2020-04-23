package slush

import "Slush/paxi"

type Client struct {
	*paxi.HTTPClient
	Col int
}

func NewClient(id paxi.ID) *Client {
	return &Client{
		HTTPClient: paxi.NewHTTPClient(id),
	}
}