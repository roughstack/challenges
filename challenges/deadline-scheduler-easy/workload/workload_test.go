package workload

import (
	"reflect"
	"testing"
)

func TestGenerateIsDeterministicAndCoversShapes(t *testing.T) {
	config := Config{Seed: ^uint64(0), Jobs: 400, MaxWork: 64, MaxGap: 8, BurstEvery: 4, LongEvery: 53}
	first := Generate(config)
	second := Generate(config)
	if !reflect.DeepEqual(first, second) {
		t.Fatal("same seed produced different jobs")
	}

	if first[0].EstimatedWork != config.MaxWork {
		t.Fatalf("first job work = %d, want long job %d", first[0].EstimatedWork, config.MaxWork)
	}

	sawBurst := false
	sawGap := false
	sawLong := false
	for index := 1; index < len(first); index++ {
		if first[index].Arrival == first[index-1].Arrival {
			sawBurst = true
		} else {
			sawGap = true
		}
		if first[index].EstimatedWork == config.MaxWork {
			sawLong = true
		}
		if first[index].EstimatedWork < 1 || first[index].EstimatedWork > config.MaxWork {
			t.Fatalf("job %d has out-of-range work %d", first[index].ID, first[index].EstimatedWork)
		}
	}
	if !sawBurst || !sawGap || !sawLong {
		t.Fatalf("workload did not exercise bursts, gaps, and long jobs: %+v", first[:minInt(len(first), 12)])
	}
}

func TestGenerateHandlesEmptyConfig(t *testing.T) {
	if jobs := Generate(Config{}); jobs != nil {
		t.Fatalf("empty config produced %d jobs", len(jobs))
	}
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
