package paxi

import (
	"reflect"
	"testing"
)

func TestConfig(t *testing.T) {
	c1 := MakeDefaultConfig()
	err := c1.Save()
	if err != nil {
		t.Error(err)
	}

	var c2 Config
	err = c2.Load()
	if err != nil {
		t.Error(err)
	}

	if reflect.DeepEqual(c1, c2) {
		t.Fail()
	}
}
