package paxi

import (
	"flag"
	"os"
	"testing"
)

func TestConfig(t *testing.T) {
	flag.Parse()
	*configFile = "bin/config.json"
	config.Load()

	if len(config.Addrs) < 1 {
		t.Fatal("expect config to be non-empty")
	}

	c1 := MakeDefaultConfig()
	err := c1.Save()
	if err != nil {
		t.Error(err)
	}
	defer os.Remove(*configFile)

	var c2 Config
	c2.Load()

	if c1.String() != c2.String() {
		t.Fail()
	}
}
