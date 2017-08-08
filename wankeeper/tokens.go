package wankeeper

import . "paxi"

type tokens struct {
	tokens map[Key]bool
}

func NewTokens() *tokens {
	return &tokens{
		tokens: make(map[Key]bool),
	}
}

func (t *tokens) contains(key Key) bool {
	return t.tokens[key]
}

func (t *tokens) add(key Key) {
	t.tokens[key] = true
}
