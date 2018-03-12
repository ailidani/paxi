package wankeeper

import "github.com/ailidani/paxi"

// records of who owns the tokens
type tokens struct {
	id     paxi.ID
	tokens map[paxi.Key]paxi.ID
}

func newTokens(id paxi.ID) *tokens {
	return &tokens{
		id:     id,
		tokens: make(map[paxi.Key]paxi.ID),
	}
}

func (t *tokens) contains(key paxi.Key) bool {
	return t.tokens[key] == t.id
}

func (t *tokens) add(key paxi.Key) {
	t.tokens[key] = t.id
}

func (t *tokens) get(key paxi.Key) paxi.ID {
	return t.tokens[key]
}

func (t *tokens) set(key paxi.Key, id paxi.ID) {
	t.tokens[key] = id
}
