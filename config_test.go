package paxi

import (
	"flag"
	"os"
	"testing"
)

func TestConfig(t *testing.T) {
	flag.Parse()
	if len(Config.Addrs()) < 1 {
		t.Fatal("expect config to be non-empty")
	}

	c1 := newConfig("1.1")
	err := c1.Save()
	if err != nil {
		t.Error(err)
	}
	defer os.Remove(*configFile)

	c2 := newConfig("1.1")
	err = c2.Load()
	if err != nil {
		t.Error(err)
	}

	if c1.String() != c2.String() {
		t.Fail()
	}
}
