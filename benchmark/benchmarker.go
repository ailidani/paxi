package benchmark

import "flag"

type Client interface {
	Read()
	Write()
}

type Benchmarker struct {
	config config
}

func (b *Benchmarker) Run() {
	flag.Parse()

}
