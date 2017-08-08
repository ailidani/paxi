package paxi

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"time"
)

type Stat struct {
	Data   []float64
	Size   int
	Mean   float64
	Min    float64
	Max    float64
	Median float64
	P95    float64
	P99    float64
	P999   float64
}

func (s Stat) WriteFile(path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	w := bufio.NewWriter(file)
	for _, line := range s.Data {
		fmt.Fprintln(w, line)
	}
	return w.Flush()
}

func (s Stat) String() string {
	return fmt.Sprintf("size %d\nmean %f\nmin %f\nmax %f\nmedian %f\np95 %f\np99 %f\np999 %f\n", s.Size, s.Mean, s.Min, s.Max, s.Median, s.P95, s.P99, s.P999)
}

func Statistic(latency []time.Duration) Stat {
	ms := make([]float64, 0)
	for _, l := range latency {
		ms = append(ms, float64(l.Nanoseconds())/1000000.0)
	}
	sort.Float64s(ms)
	sum := 0.0
	for _, m := range ms {
		sum += m
	}
	size := len(ms)
	return Stat{
		Data:   ms,
		Size:   size,
		Mean:   sum / float64(size),
		Min:    ms[0],
		Max:    ms[size-1],
		Median: ms[int(0.5*float64(size))],
		P95:    ms[int(0.95*float64(size))],
		P99:    ms[int(0.99*float64(size))],
		P999:   ms[int(0.999*float64(size))],
	}
}
