package harness

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/roughstack/challenges/challenges/cache-pressure-medium/contract"
	"github.com/roughstack/challenges/challenges/cache-pressure-medium/starter"
	"github.com/roughstack/challenges/challenges/cache-pressure-medium/workload"
)

func TestEvaluateIsDeterministicAndWithinCapacity(t *testing.T) {
	config := DefaultPublicConfig()
	first := Evaluate(^uint64(0), config, starter.NewCache)
	second := Evaluate(^uint64(0), config, starter.NewCache)

	if !reflect.DeepEqual(first, second) {
		t.Fatalf("same seed produced different results:\n%+v\n%+v", first, second)
	}
	if first.Verdict != "pass" || len(first.Violations) != 0 {
		t.Fatalf("unexpected verdict: %+v", first)
	}
	if first.Metrics.WeightedBackingCost == 0 {
		t.Fatal("workload did not exercise backing reads")
	}
	if first.Metrics.ByteHitRate > 10000 || first.Metrics.RequestHitRate > 10000 {
		t.Fatalf("hit rates out of range: %+v", first.Metrics)
	}
	if first.Metrics.PeakMemoryHeadroom > config.CapacityBytes {
		t.Fatal("peak memory headroom exceeded capacity")
	}
}

func TestResultEnvelopeContainsEveryPublicMetric(t *testing.T) {
	result := Evaluate(7, DefaultPublicConfig(), starter.NewCache)
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}

	var envelope map[string]any
	if err := json.Unmarshal(encoded, &envelope); err != nil {
		t.Fatal(err)
	}
	metrics, ok := envelope["metrics"].(map[string]any)
	if !ok {
		t.Fatal("result metrics are not an object")
	}
	for _, metric := range []string{
		"weighted_backing_cost",
		"byte_hit_rate",
		"request_hit_rate",
		"logical_policy_work",
		"peak_memory_headroom",
	} {
		if _, ok := metrics[metric]; !ok {
			t.Fatalf("result is missing metric %q", metric)
		}
	}
}

func TestScanResistanceWorkloadAndFloor(t *testing.T) {
	config := DefaultPublicConfig()

	// The default config declares exactly one cache-sized scan.
	if config.ScanKeys == 0 || config.Operations <= int(config.ScanKeys) {
		t.Fatalf("config does not leave room for a hot phase around the scan: %+v", config)
	}

	starterRate := Evaluate(^uint64(0), config, starter.NewCache).Metrics.RequestHitRate
	noCacheRate := Evaluate(^uint64(0), config, newNoCache).Metrics.RequestHitRate

	if noCacheRate != 0 {
		t.Fatalf("no-cache policy reported a non-zero hit rate: %d", noCacheRate)
	}
	if starterRate < ScanResistanceFloorBasisPoints {
		t.Fatalf("starter request hit rate %d is below the published floor %d", starterRate, ScanResistanceFloorBasisPoints)
	}
	if starterRate <= noCacheRate {
		t.Fatalf("starter hit rate %d did not beat a no-cache policy (%d)", starterRate, noCacheRate)
	}
}

func TestEvaluateRejectsCacheThatIgnoresDelete(t *testing.T) {
	config := PublicConfig{CapacityBytes: 1_000_000, Operations: 8, HotKeys: 1, ScanKeys: 1}
	seed, ok := findSeedForOperations(config, hasDeleteThenReadWithoutInterveningWrite)
	if !ok {
		t.Fatal("no deterministic seed produces a delete followed by a read of the same key")
	}

	result := Evaluate(seed, config, newIgnoringDeleteCache)
	if result.Verdict != "fail" {
		t.Fatalf("verdict = %q, want fail; result: %+v", result.Verdict, result)
	}
	if !containsViolation(result.Violations, "cache returned a deleted entry") {
		t.Fatalf("violations = %v, want %q", result.Violations, "cache returned a deleted entry")
	}
}

func TestEvaluateRejectsCacheThatUnderReportsUsage(t *testing.T) {
	config := PublicConfig{CapacityBytes: 1_000_000, Operations: 8, HotKeys: 1, ScanKeys: 1}
	seed, ok := findSeedForOperations(config, func(operations []workload.Operation) bool {
		return len(operations) > 0 && operations[0].Kind == workload.Write
	})
	if !ok {
		t.Fatal("no deterministic seed starts the workload with a write")
	}

	result := Evaluate(seed, config, newZeroUsageCache)
	if result.Verdict != "fail" {
		t.Fatalf("verdict = %q, want fail; result: %+v", result.Verdict, result)
	}
	if !containsViolation(result.Violations, "reported usage does not cover a served entry") {
		t.Fatalf("violations = %v, want %q", result.Violations, "reported usage does not cover a served entry")
	}
}

type noCache struct{}

func (noCache) Get(uint64, contract.RequestMeta) ([]byte, bool) { return nil, false }
func (noCache) Put(uint64, []byte, contract.RequestMeta)        {}
func (noCache) Delete(uint64)                                   {}
func (noCache) UsedBytes() uint64                               { return 0 }

func newNoCache(uint64) contract.Cache { return noCache{} }

type ignoringDeleteCache struct {
	values map[uint64][]byte
}

func newIgnoringDeleteCache(uint64) contract.Cache {
	return &ignoringDeleteCache{values: make(map[uint64][]byte)}
}

func (c *ignoringDeleteCache) Get(key uint64, _ contract.RequestMeta) ([]byte, bool) {
	value, ok := c.values[key]
	if !ok {
		return nil, false
	}
	return cloneBytes(value), true
}

func (c *ignoringDeleteCache) Put(key uint64, value []byte, _ contract.RequestMeta) {
	c.values[key] = cloneBytes(value)
}

func (c *ignoringDeleteCache) Delete(key uint64) {
	// This cache intentionally ignores Delete and keeps serving the value.
	_ = key
}

func (c *ignoringDeleteCache) UsedBytes() uint64 {
	var total uint64
	for _, value := range c.values {
		total += contract.AccountedBytes(value)
	}
	return total
}

type zeroUsageCache struct {
	values map[uint64][]byte
}

func newZeroUsageCache(uint64) contract.Cache {
	return &zeroUsageCache{values: make(map[uint64][]byte)}
}

func (c *zeroUsageCache) Get(key uint64, _ contract.RequestMeta) ([]byte, bool) {
	value, ok := c.values[key]
	if !ok {
		return nil, false
	}
	return cloneBytes(value), true
}

func (c *zeroUsageCache) Put(key uint64, value []byte, _ contract.RequestMeta) {
	c.values[key] = cloneBytes(value)
}

func (c *zeroUsageCache) Delete(key uint64) {
	delete(c.values, key)
}

func (c *zeroUsageCache) UsedBytes() uint64 {
	return 0
}

func cloneBytes(value []byte) []byte {
	cloned := make([]byte, len(value))
	copy(cloned, value)
	return cloned
}

func containsViolation(violations []string, want string) bool {
	for _, violation := range violations {
		if violation == want {
			return true
		}
	}
	return false
}

func findSeedForOperations(config PublicConfig, predicate func([]workload.Operation) bool) (uint64, bool) {
	for seed := uint64(0); seed < 1000; seed++ {
		operations := workload.Generate(workload.Config{
			Seed:          seed,
			Operations:    config.Operations,
			HotKeys:       config.HotKeys,
			ScanKeys:      config.ScanKeys,
			CapacityBytes: config.CapacityBytes,
		})
		if predicate(operations) {
			return seed, true
		}
	}
	return 0, false
}

func hasDeleteThenReadWithoutInterveningWrite(operations []workload.Operation) bool {
	for index, operation := range operations {
		if operation.Kind != workload.Delete {
			continue
		}
		if !hasPriorStore(operations, index, operation.Key) {
			continue
		}
		for later := index + 1; later < len(operations); later++ {
			if operations[later].Key != operation.Key {
				continue
			}
			if operations[later].Kind == workload.Write {
				break
			}
			if operations[later].Kind == workload.Read {
				return true
			}
		}
	}
	return false
}

func hasPriorStore(operations []workload.Operation, before int, key uint64) bool {
	for index := 0; index < before; index++ {
		if operations[index].Key == key && operations[index].Kind != workload.Delete {
			return true
		}
	}
	return false
}
