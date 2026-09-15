// Package workload generates the deterministic public request stream for the
// cache-pressure-medium arena.
package workload

import "github.com/bytearena/arenas/arenas/cache-pressure-medium/contract"

// Kind identifies an operation owned by the public harness.
type Kind uint8

const (
	Read Kind = iota
	Write
	Delete
)

// Value size classes for the public workload, in bytes.
const (
	// HotValueMin is the smallest hot-key value size.
	HotValueMin uint64 = 32
	// HotValueSpan is the range of hot-key value sizes; hot values span
	// [HotValueMin, HotValueMin+HotValueSpan).
	HotValueSpan uint64 = 160
	// ScanValueBytes is the fixed value size of a scan entry. The harness sizes
	// the scan so ScanKeys * (ScanValueBytes + per-entry metadata) fills the
	// cache byte budget.
	ScanValueBytes uint64 = 96
)

// Operation is one deterministic request. Value content is derived by the
// harness from key versions; SizeBytes and TTL are workload-determined so the
// stream remains compact and reproducible.
type Operation struct {
	Kind      Kind
	Key       uint64
	SizeBytes uint64
	TTL       uint64
}

// Config controls the public workload without exposing ranked distributions.
type Config struct {
	Seed          uint64
	Operations    int
	HotKeys       uint64
	ScanKeys      uint64
	CapacityBytes uint64
}

// Generate creates a hot phase, one cache-sized scan, and a repeat hot phase
// from a 64-bit seed. The scan reads ScanKeys distinct cold keys whose total
// accounted size is at least CapacityBytes, so it displaces a simple policy.
func Generate(config Config) []Operation {
	if config.Operations <= 0 {
		return nil
	}
	if config.HotKeys == 0 {
		config.HotKeys = 16
	}
	if config.ScanKeys == 0 {
		if config.CapacityBytes > 0 {
			scanEntryBytes := ScanValueBytes + contract.PerEntryMetadataBytes
			config.ScanKeys = config.CapacityBytes / scanEntryBytes
			if config.CapacityBytes%scanEntryBytes != 0 {
				config.ScanKeys++
			}
		} else {
			config.ScanKeys = 256
		}
	}

	random := splitMix64{state: config.Seed}
	operations := make([]Operation, 0, config.Operations)

	scanStart := (config.Operations - int(config.ScanKeys)) / 2
	if scanStart < 0 {
		scanStart = 0
	}
	scanEnd := scanStart + int(config.ScanKeys)
	if scanEnd > config.Operations {
		scanEnd = config.Operations
	}

	for index := 0; index < config.Operations; index++ {
		switch {
		case index < scanStart:
			key := random.next() % config.HotKeys
			operations = append(operations, mixedOperation(config.Seed, &random, key, hotSize(config.Seed, key)))
		case index < scanEnd:
			key := config.HotKeys + uint64(index-scanStart)
			operations = append(operations, Operation{
				Kind:      Read,
				Key:       key,
				SizeBytes: ScanValueBytes,
				TTL:       scanTTL(config.Seed, key),
			})
		default:
			key := random.next() % config.HotKeys
			operations = append(operations, mixedOperation(config.Seed, &random, key, hotSize(config.Seed, key)))
		}
	}

	return operations
}

func mixedOperation(seed uint64, random *splitMix64, key, size uint64) Operation {
	ttl := hotTTL(seed, key)
	switch random.next() % 20 {
	case 0:
		return Operation{Kind: Delete, Key: key, SizeBytes: size, TTL: ttl}
	case 1, 2, 3:
		return Operation{Kind: Write, Key: key, SizeBytes: size, TTL: ttl}
	default:
		return Operation{Kind: Read, Key: key, SizeBytes: size, TTL: ttl}
	}
}

func hotSize(seed, key uint64) uint64 {
	return HotValueMin + KeyMix(seed, key, sizeSalt)%HotValueSpan
}

func hotTTL(seed, key uint64) uint64 {
	if KeyMix(seed, key, ttlSalt)%8 == 0 {
		// Ephemeral entries expire within the run and exercise lazy expiry.
		return 20 + KeyMix(seed, key, ttlSalt>>1)%180
	}
	return longTTL
}

func scanTTL(seed, key uint64) uint64 {
	// Scan entries stay live so the scan genuinely pollutes a simple policy.
	return longTTL
}

const (
	longTTL  = uint64(1) << 50
	sizeSalt = uint64(0x9e3779b97f4a7c15)
	ttlSalt  = uint64(0xbf58476d1ce4e5b9)
)

// KeyMix returns a deterministic 64-bit mix of a seed, key, and salt. Callers
// use distinct salts so size, TTL, and cost classes stay decorrelated.
func KeyMix(seed, key, salt uint64) uint64 {
	state := seed ^ rotateLeft(key, 17) ^ salt
	state ^= state << 13
	state ^= state >> 7
	state ^= state << 17
	return state
}

func rotateLeft(value uint64, shift uint) uint64 {
	return value<<shift | value>>(64-shift)
}

type splitMix64 struct {
	state uint64
}

func (r *splitMix64) next() uint64 {
	r.state += 0x9e3779b97f4a7c15
	value := r.state
	value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9
	value = (value ^ (value >> 27)) * 0x94d049bb133111eb
	return value ^ (value >> 31)
}
