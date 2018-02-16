package wankeeper

import "github.com/ailidani/paxi"

type tokens struct {
	tokens map[paxi.Key]bool
}

func newTokens() *tokens {
	return &tokens{
		tokens: make(map[paxi.Key]bool),
	}
}

func (t *tokens) contains(key paxi.Key) bool {
	return t.tokens[key]
}

func (t *tokens) add(key paxi.Key) {
	t.tokens[key] = true
}
