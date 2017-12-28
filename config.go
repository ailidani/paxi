package paxi

import (
	"encoding/json"
	"os"
	"strconv"

	"github.com/ailidani/paxi/log"
)

// default values
const (
	PORT             = 1735
	HTTP_PORT        = 8080
	CHAN_BUFFER_SIZE = 1024 * 1
	BUFFER_SIZE      = 1024 * 10000
)

type Config struct {
	ID              ID            `json:"-"`
	Addrs           map[ID]string `json:"address"`      // address for node communication
	HTTPAddrs       map[ID]string `json:"http_address"` // address for client server communication
	Algorithm       string        `json:"algorithm"`    // replication algorithm name
	F               int           `json:"f"`            // number of failure zones in general grid quorums
	Threshold       int           `json:"threshold"`    // threshold for leader change, 0 means immediate
	BackOff         int           `json:"backoff"`      // random backoff interval
	Thrifty         bool          `json:"thrifty"`      // only send messages to a quorum
	ChanBufferSize  int           `json:"chan_buffer_size"`
	BufferSize      int           `json:"buffer_size"`
	ConfigFile      string        `json:"file"`
	Transport       string        `json:"transport"` // not used
	Codec           string        `json:"codec"`
	ReplyWhenCommit bool          `json:"reply_when_commit` // reply to client when request is committed, instead of executed

	// for future implementation
	// Batching bool `json:"batching"`
	// Consistency int `json:"consistency"`
}

func MakeDefaultConfig() Config {
	id := NewID(0, 0)
	config := new(Config)
	config.ID = NewID(1, 1)
	config.Addrs = map[ID]string{id: "127.0.0.1:" + strconv.Itoa(PORT)}
	config.HTTPAddrs = map[ID]string{id: "http://localhost:" + strconv.Itoa(HTTP_PORT)}
	config.Algorithm = "wpaxos"
	config.ChanBufferSize = CHAN_BUFFER_SIZE
	config.BufferSize = BUFFER_SIZE
	config.ConfigFile = "config.json"
	config.Transport = "chan"
	config.Codec = "gob"
	return *config
}

// NewConfig creates config object with given node id and config file path
func NewConfig(id ID, file string) Config {
	config := new(Config)
	config.ID = id
	config.ConfigFile = file
	config.Load()
	return *config
}

// String is implemented to print the config
func (c *Config) String() string {
	config, err := json.Marshal(c)
	if err != nil {
		log.Errorln(err)
	}
	return string(config)
}

// Load load configurations from config file in JSON format
func (c *Config) Load() error {
	file, err := os.Open(c.ConfigFile)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(file)
	return decoder.Decode(c)
}

// Save save configurations to file in JSON format
func (c *Config) Save() error {
	file, err := os.Create(c.ConfigFile)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(file)
	return encoder.Encode(c)
}
