// Package harness owns public execution, backing data, accounting, and results.
package harness

import (
	"bytes"
	"strconv"

	"github.com/bytearena/arenas/arenas/cache-pressure-medium/contract"
	"github.com/bytearena/arenas/arenas/cache-pressure-medium/workload"
)

const (
	ArenaID      = "cache-pressure-medium"
	ArenaVersion = "1.0.0"
	WorkloadID   = "public-smoke-v1"
)

// ScanResistanceFloorBasisPoints is the published request-hit-rate floor, in
// basis points, that any functional cache must clear on the public workload.
// It rejects degenerate policies while remaining achievable by the simple
// starter and by better scan-resistant policies alike.
const ScanResistanceFloorBasisPoints = 4000

// Metrics are deterministic logical measurements from one workload run.
type Metrics struct {
	WeightedBackingCost uint64 `json:"weighted_backing_cost"`
	ByteHitRate         uint64 `json:"byte_hit_rate"`
	RequestHitRate      uint64 `json:"request_hit_rate"`
	LogicalPolicyWork   uint64 `json:"logical_policy_work"`
	PeakMemoryHeadroom  uint64 `json:"peak_memory_headroom"`
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

// DefaultPublicConfig returns the fast public smoke workload. The scan is
// exactly one cache-sized pass: ScanKeys cold entries whose accounted size
// fills the byte budget.
func DefaultPublicConfig() PublicConfig {
	capacity := uint64(64 * 1024)
	scanEntryBytes := contract.AccountedBytes(make([]byte, workload.ScanValueBytes))
	// Ceiling division fills the byte budget even when the accounted entry size
	// does not divide the capacity exactly.
	scanKeys := capacity / scanEntryBytes
	if capacity%scanEntryBytes != 0 {
		scanKeys++
	}
	if scanKeys == 0 {
		scanKeys = 1
	}
	return PublicConfig{
		CapacityBytes: capacity,
		Operations:    4096,
		HotKeys:       16,
		ScanKeys:      scanKeys,
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
	putTick := make(map[uint64]uint64)
	ttl := make(map[uint64]uint64)
	deletedPending := make(map[uint64]bool)

	operations := workload.Generate(workload.Config{
		Seed:          seed,
		Operations:    config.Operations,
		HotKeys:       config.HotKeys,
		ScanKeys:      config.ScanKeys,
		CapacityBytes: config.CapacityBytes,
	})

	var (
		reads          uint64
		readHits       uint64
		requestedBytes uint64
		hitBytes       uint64
		peakUsed       uint64
	)

	for index, operation := range operations {
		meta := contract.RequestMeta{
			NowTick: uint64(index),
			Cost:    costFor(seed, operation.Key),
			TTL:     operation.TTL,
		}

		var lowerBoundEntryBytes uint64
		lowerBoundSet := false

		switch operation.Kind {
		case workload.Read:
			reads++
			requestedBytes += operation.SizeBytes
			expected := backingValue(seed, operation.Key, versions[operation.Key], operation.SizeBytes)

			value, ok := cache.Get(operation.Key, meta)
			result.Metrics.LogicalPolicyWork++
			if ok {
				readHits++
				hitBytes += operation.SizeBytes
				if !bytes.Equal(value, expected) {
					result.Violations = append(result.Violations, "cache hit returned the wrong value")
				}
				if deletedPending[operation.Key] {
					result.Violations = append(result.Violations, "cache returned a deleted entry")
				}
				if ttl[operation.Key] != 0 && meta.NowTick >= contract.ExpiryTick(putTick[operation.Key], ttl[operation.Key]) {
					result.Violations = append(result.Violations, "cache returned an expired entry")
				}
				lowerBoundEntryBytes = contract.AccountedBytes(value)
				lowerBoundSet = true
				// Sample aliasing on Get: an exhaustive double-Get would double
				// logical policy work, so only every 257th read is checked here.
				// The Write and miss-fill Put mutations below close the remaining
				// Put-aliasing surface.
				if index%257 == 0 && len(value) > 0 {
					value[0] ^= 0xff
					second, secondOK := cache.Get(operation.Key, meta)
					result.Metrics.LogicalPolicyWork++
					if !secondOK || !bytes.Equal(second, expected) {
						result.Violations = append(result.Violations, "returned value aliases cache storage")
					}
				}
			} else {
				result.Metrics.WeightedBackingCost += uint64(meta.Cost)
				cache.Put(operation.Key, expected, meta)
				result.Metrics.LogicalPolicyWork++
				putTick[operation.Key] = meta.NowTick
				ttl[operation.Key] = operation.TTL
				deletedPending[operation.Key] = false
				lowerBoundEntryBytes = contract.AccountedBytes(expected)
				lowerBoundSet = true
				if len(expected) > 0 {
					expected[0] ^= 0xff
				}
			}

		case workload.Write:
			versions[operation.Key]++
			value := backingValue(seed, operation.Key, versions[operation.Key], operation.SizeBytes)
			cache.Put(operation.Key, value, meta)
			result.Metrics.LogicalPolicyWork++
			putTick[operation.Key] = meta.NowTick
			ttl[operation.Key] = operation.TTL
			deletedPending[operation.Key] = false
			lowerBoundEntryBytes = contract.AccountedBytes(value)
			lowerBoundSet = true
			value[0] ^= 0xff

		case workload.Delete:
			cache.Delete(operation.Key)
			result.Metrics.LogicalPolicyWork++
			deletedPending[operation.Key] = true
		}

		used := cache.UsedBytes()
		result.Metrics.LogicalPolicyWork++
		if lowerBoundSet && used < lowerBoundEntryBytes {
			result.Violations = append(result.Violations, "reported usage does not cover a served entry")
		}
		if used > config.CapacityBytes {
			result.Violations = append(result.Violations, "reported cache memory exceeds capacity")
		}
		if used > peakUsed {
			peakUsed = used
		}
		if len(result.Violations) >= 8 {
			break
		}
	}

	if requestedBytes > 0 {
		result.Metrics.ByteHitRate = hitBytes * 10000 / requestedBytes
	}
	if reads > 0 {
		result.Metrics.RequestHitRate = readHits * 10000 / reads
	}
	if peakUsed < config.CapacityBytes {
		result.Metrics.PeakMemoryHeadroom = config.CapacityBytes - peakUsed
	}

	result.Run.PeakMemoryBytes = peakUsed
	if len(result.Violations) > 0 {
		result.Verdict = "fail"
	}
	return result
}

const costSalt = uint64(0x94d049bb133111eb)

func costFor(seed, key uint64) uint32 {
	return 1 + uint32(workload.KeyMix(seed, key, costSalt)%100)
}

func backingValue(seed, key, version, size uint64) []byte {
	value := make([]byte, int(size))
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
