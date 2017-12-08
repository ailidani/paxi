package paxi

import "testing"

func TestConfig(t *testing.T) {
	c := MakeDefaultConfig()
	c.Save()
}
