package quantile

import (
	"encoding/json"
	"log"
	"testing"
	"time"
)

func TestQuantileAndResult(t *testing.T) {
	e2eProcessingLatencyStream := New(
		10*time.Second,
		[]float64{0.90, 0.99, 0.50},
	)
	msgStartTime := time.Now().UnixNano()
	for i := 0; i < 13; i++ {
		for j := 0; j < 100; j++ {
			e2eProcessingLatencyStream.Insert(msgStartTime)
		}
		r := e2eProcessingLatencyStream.Result()
		nowStr := time.Now().Format("2006-01-02 15:04:05")
		for _, percentile := range r.Percentiles {
			b, _ := json.Marshal(percentile)
			log.Printf("%s; count:%d; percentile:%s;\n", nowStr, r.Count, string(b))
		}
		time.Sleep(time.Second)
	}
}
