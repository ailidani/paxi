package paxi

import (
	"encoding/gob"
	"encoding/json"
	"io"

	"github.com/ailidani/paxi/log"
)

// Codec interface provide methods for serialization and deserialization
// combines json and gob encoder decoder interface
type Codec interface {
	Scheme() string
	Encode(interface{})
	Decode(interface{})
}

// NewCodec creates new codec object based on scheme, i.e. json and gob
func NewCodec(scheme string, rw io.ReadWriter) Codec {
	switch scheme {
	case "json":
		return &codecJSON{
			encoder: json.NewEncoder(rw),
			decoder: json.NewDecoder(rw),
		}
	case "gob":
		return &codecGOB{
			encoder: gob.NewEncoder(rw),
			decoder: gob.NewDecoder(rw),
		}
	}
	return nil
}

type codecJSON struct {
	encoder *json.Encoder
	decoder *json.Decoder
}

func (j *codecJSON) Scheme() string {
	return "json"
}

func (j *codecJSON) Encode(m interface{}) {
	err := j.encoder.Encode(m)
	if err != nil {
		log.Error(err)
	}
}

func (j *codecJSON) Decode(m interface{}) {
	err := j.decoder.Decode(m)
	if err != nil {
		log.Error(err)
	}
}

type codecGOB struct {
	encoder *gob.Encoder
	decoder *gob.Decoder
}

func (g *codecGOB) Scheme() string {
	return "gob"
}

func (g *codecGOB) Encode(m interface{}) {
	err := g.encoder.Encode(m)
	if err != nil {
		log.Error(err)
	}
}

func (g *codecGOB) Decode(m interface{}) {
	err := g.decoder.Decode(m)
	if err != nil {
		log.Error(err)
	}
}
