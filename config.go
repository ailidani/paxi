package paxi

import (
	"encoding/json"
	"os"
	"paxi/glog"
	"strconv"
)

const (
	PORT             = 1735
	CHAN_BUFFER_SIZE = 1024 * 10000
	BUFFER_SIZE      = 1024 * 10000
)

type Protocol int

const (
	WPaxos = iota
	EPaxos
	KPaxos
	WanKeeper
	Cosmos
)

type Config struct {
	ID             ID            `json:"id"`
	Addrs          map[ID]string `json:"address"`
	Protocol       Protocol      `json:"protocol"`
	F              int           `json:"f"`
	Threshold      int           `json:"threshold"`
	BackOff        int           `json:"backoff"`
	Thrifty        bool          `json:"thrifty"`
	ChanBufferSize int           `json:"chan_buffer_size"`
	BufferSize     int           `json:"buffer_size"`
	ConfigFile     string        `json:"file"`
	Consistency    int
	// Persistent     bool          `json:"persistent"`
	// Transport      string        `json:"transport"`
	// RecvRoutines   int           `json:"recv_routines"`
	// Codec          string        `json:"codec"`
	// Batching       bool          `json:"batching"`
}

func MakeDefaultConfig() *Config {
	id := NewID(0, 0)
	return &Config{
		ID:             id,
		Addrs:          map[ID]string{id: ":" + strconv.Itoa(PORT)},
		Protocol:       WPaxos,
		F:              0,
		Threshold:      0,
		BackOff:        0,
		Thrifty:        false,
		ChanBufferSize: CHAN_BUFFER_SIZE,
		BufferSize:     BUFFER_SIZE,
		ConfigFile:     "config.json",
		// Transport:      "tcp4",
		// RecvRoutines:   RECV_ROUTINES,
		// Codec:          "gob",
		// Batching:       false,
		// Persistent:     false,
	}
}

// String is implemented to print the config.
func (c *Config) String() string {
	config, err := json.Marshal(c)
	if err != nil {
		glog.Errorln(err)
	}
	return string(config)
	//return fmt.Sprintf("Config[MyID:%s,Address:%s]", c.ID.String(), c.Addrs[c.ID])
}

func (c *Config) Load() error {
	file, err := os.Open(c.ConfigFile)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(file)
	return decoder.Decode(c)
}

func (c *Config) Save() error {
	file, err := os.Create(c.ConfigFile)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(file)
	return encoder.Encode(c)
}
