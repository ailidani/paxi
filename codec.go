package paxi

import (
	"bytes"
	"encoding/gob"
	"encoding/json"

	"github.com/ailidani/paxi/log"
)

// Codec interface provide methods for serialization and deserialization
type Codec interface {
	Scheme() string
	Encode(msg interface{}) []byte
	Decode(data []byte) interface{}
}

// NewCodec creates new codec object based on scheme, i.e. json and gob
func NewCodec(scheme string) Codec {
	switch scheme {
	case "json":
		return &jsonCodec{}
	case "gob":
		return &gobCodec{}
	}
	return nil
}

type jsonCodec struct{}

func (j *jsonCodec) Scheme() string {
	return "json"
}

func (j *jsonCodec) Encode(msg interface{}) []byte {
	b, err := json.Marshal(msg)
	if err != nil {
		log.Errorln(err)
		return nil
	}
	return b
}

func (j *jsonCodec) Decode(data []byte) interface{} {
	var msg interface{}
	err := json.Unmarshal(data, &msg)
	if err != nil {
		log.Errorln(err)
		return nil
	}
	return msg
}

type gobCodec struct{}

func (g *gobCodec) Scheme() string {
	return "gob"
}

func (g *gobCodec) Encode(msg interface{}) []byte {
	buffer := new(bytes.Buffer)
	encoder := gob.NewEncoder(buffer)
	err := encoder.Encode(&msg)
	if err != nil {
		log.Fatalln(err)
	}
	return buffer.Bytes()
}

func (g *gobCodec) Decode(data []byte) interface{} {
	var msg interface{}
	buffer := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buffer)
	err := decoder.Decode(&msg)
	if err != nil {
		log.Fatalln(err)
	}
	return msg
}
