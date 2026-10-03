// Package harness owns public execution, backing data, accounting, and results.
package harness

import (
	"bytes"
	"strconv"

	"github.com/roughstack/challenges/challenges/cache-pressure-easy/contract"
	"github.com/roughstack/challenges/challenges/cache-pressure-easy/workload"
)

const (
	ArenaID      = "cache-pressure-easy"
	ArenaVersion = "1.0.0"
	WorkloadID   = "public-smoke-v1"
	valueBytes   = 96
)

// Metrics are deterministic logical measurements from one workload run.
type Metrics struct {
	BackingReads       uint64 `json:"backing_reads"`
	MetadataWork       uint64 `json:"metadata_work"`
	PeakAccountedBytes uint64 `json:"peak_accounted_bytes"`
}

// RunInfo records protocol-required run metadata without wall-clock noise.
type RunInfo struct {
	WorkloadID      string `json:"workload_id"`
	DurationNS      uint64 `json:"duration_ns"`
	PeakMemoryBytes uint64 `json:"peak_memory_bytes"`
}

// Result is the versioned one-object stdout protocol.
type Result struct {
	ProtocolVersion int      `json:"protocol_version"`
	ArenaID         string   `json:"arena_id"`
	ArenaVersion    string   `json:"arena_version"`
	Seed            string   `json:"seed"`
	Verdict         string   `json:"verdict"`
	Score           int      `json:"score"`
	Metrics         Metrics  `json:"metrics"`
	Violations      []string `json:"violations"`
	Run             RunInfo  `json:"run"`
}

// PublicConfig is intentionally small and distinct from private ranked inputs.
type PublicConfig struct {
	CapacityBytes uint64
	Operations    int
	HotKeys       uint64
	ScanKeys      uint64
}

// DefaultPublicConfig returns the fast public smoke workload.
func DefaultPublicConfig() PublicConfig {
	return PublicConfig{
		CapacityBytes: contract.AccountedBytes(make([]byte, valueBytes)) * 32,
		Operations:    4000,
		HotKeys:       16,
		ScanKeys:      96,
	}
}

// Evaluate runs a policy-independent request stream against a contestant cache.
func Evaluate(seed uint64, config PublicConfig, factory contract.Factory) Result {
	result := Result{
		ProtocolVersion: 1,
		ArenaID:         ArenaID,
		ArenaVersion:    ArenaVersion,
		Seed:            strconv.FormatUint(seed, 10),
		Verdict:         "pass",
		Score:           0,
		Violations:      []string{},
		Run: RunInfo{
			WorkloadID: WorkloadID,
		},
	}

	cache := factory(config.CapacityBytes)
	versions := make(map[uint64]uint64)
	operations := workload.Generate(workload.Config{
		Seed:       seed,
		Operations: config.Operations,
		HotKeys:    config.HotKeys,
		ScanKeys:   config.ScanKeys,
	})

	for index, operation := range operations {
		meta := contract.RequestMeta{NowTick: uint64(index), Cost: 1}
		switch operation.Kind {
		case workload.Read:
			expected := backingValue(seed, operation.Key, versions[operation.Key])
			value, ok := cache.Get(operation.Key, meta)
			result.Metrics.MetadataWork++
			if ok {
				if !bytes.Equal(value, expected) {
					result.Violations = append(result.Violations, "cache hit returned the wrong value")
				}
				if index%257 == 0 && len(value) > 0 {
					value[0] ^= 0xff
					second, secondOK := cache.Get(operation.Key, meta)
					result.Metrics.MetadataWork++
					if !secondOK || !bytes.Equal(second, expected) {
						result.Violations = append(result.Violations, "returned value aliases cache storage")
					}
				}
			} else {
				result.Metrics.BackingReads++
				cache.Put(operation.Key, expected, meta)
				result.Metrics.MetadataWork++
			}
		case workload.Write:
			versions[operation.Key]++
			value := backingValue(seed, operation.Key, versions[operation.Key])
			cache.Put(operation.Key, value, meta)
			result.Metrics.MetadataWork++
			value[0] ^= 0xff
		case workload.Delete:
			cache.Delete(operation.Key)
			result.Metrics.MetadataWork++
		}

		used := cache.UsedBytes()
		result.Metrics.MetadataWork++
		if used > config.CapacityBytes {
			result.Violations = append(result.Violations, "reported cache memory exceeds capacity")
		}
		if used > result.Metrics.PeakAccountedBytes {
			result.Metrics.PeakAccountedBytes = used
		}
		if len(result.Violations) >= 8 {
			break
		}
	}

	result.Run.PeakMemoryBytes = result.Metrics.PeakAccountedBytes
	if len(result.Violations) > 0 {
		result.Verdict = "fail"
	}
	return result
}

func backingValue(seed, key, version uint64) []byte {
	value := make([]byte, valueBytes)
	state := seed ^ rotateLeft(key, 17) ^ rotateLeft(version, 41)
	for index := range value {
		state ^= state << 13
		state ^= state >> 7
		state ^= state << 17
		value[index] = byte(state)
	}
	return value
}

func rotateLeft(value uint64, shift uint) uint64 {
	return value<<shift | value>>(64-shift)
}
