package workload

import (
	"reflect"
	"testing"
)

func TestGenerateIsDeterministic(t *testing.T) {
	config := Config{Seed: ^uint64(0), Requests: 400, Burst: 10, RateNum: 2, BurstEvery: 4, MaxGap: 8, CostUpper: 3}
	first := Generate(config)
	second := Generate(config)
	if !reflect.DeepEqual(first, second) {
		t.Fatal("same seed produced different request streams")
	}

	other := Generate(Config{Seed: 1, Requests: 400, Burst: 10, RateNum: 2, BurstEvery: 4, MaxGap: 8, CostUpper: 3})
	if reflect.DeepEqual(first, other) {
		t.Fatal("different seeds produced identical request streams")
	}
}

func TestGenerateKeepsTicksMonotonicAndCostsInBurst(t *testing.T) {
	requests := Generate(Config{Seed: 7, Requests: 500, Burst: 6, RateNum: 2, BurstEvery: 5, MaxGap: 9, CostUpper: 8})
	if len(requests) != 500 {
		t.Fatalf("requests = %d, want 500", len(requests))
	}

	sawBurst := false
	sawGap := false
	for index, request := range requests {
		if request.Cost > 6 {
			t.Fatalf("request %d cost %d exceeds burst 6", index, request.Cost)
		}
		if index > 0 {
			if request.Tick < requests[index-1].Tick {
				t.Fatalf("tick regressed at request %d: %d after %d", index, request.Tick, requests[index-1].Tick)
			}
			if request.Tick == requests[index-1].Tick {
				sawBurst = true
			} else {
				sawGap = true
			}
		}
	}
	if !sawBurst || !sawGap {
		t.Fatal("workload did not exercise same-tick bursts and idle gaps")
	}
}

func TestGenerateZeroBurstProducesZeroCost(t *testing.T) {
	requests := Generate(Config{Seed: 3, Requests: 20, Burst: 0, RateNum: 1, BurstEvery: 4, MaxGap: 8, CostUpper: 3})
	if len(requests) != 20 {
		t.Fatalf("requests = %d, want 20", len(requests))
	}
	for index, request := range requests {
		if request.Cost != 0 {
			t.Fatalf("zero-burst request %d cost = %d, want 0", index, request.Cost)
		}
	}
}

func TestGenerateEmptyConfig(t *testing.T) {
	if requests := Generate(Config{}); requests != nil {
		t.Fatalf("empty config produced %d requests", len(requests))
	}
}
