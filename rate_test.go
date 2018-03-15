package paxi

import (
	"math"
	"sync/atomic"
	"testing"
	"time"
)

var total uint64

func worker(tasks <-chan int) {
	for task := range tasks {
		time.Sleep(time.Duration(task) * time.Millisecond)
		atomic.AddUint64(&total, 1)
	}
}

func TestLimiter(t *testing.T) {
	rate := 1000
	limiter := NewLimiter(rate)

	tasks := make(chan int)

	for i := 0; i < 100; i++ {
		go worker(tasks)
	}

	start := time.Now()
	for i := 0; i < 10000; i++ {
		limiter.Wait()
		tasks <- 1
	}
	close(tasks)
	end := time.Now()
	throughput := float64(total) / end.Sub(start).Seconds()
	t.Logf("throughput = %f", throughput)
	if math.Abs(throughput-float64(rate))/float64(rate) > 0.001 {
		t.Errorf("throughput is %f limit is %d", throughput, rate)
	}
}
