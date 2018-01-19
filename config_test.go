package paxi

import (
	"os"
	"testing"
)

func TestConfig(t *testing.T) {
	c1 := MakeDefaultConfig()
	err := c1.Save()
	if err != nil {
		t.Error(err)
	}
	defer os.Remove(*config)

	var c2 Config
	err = c2.Load()
	if err != nil {
		t.Error(err)
	}

	if c1.String() != c2.String() {
		t.Fail()
	}
}
