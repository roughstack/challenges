// Package workload generates the deterministic public blob set for the
// chunk-store-easy arena. The harness owns this data; the store sees it only
// through Put and ReadAt calls.
package workload

import (
	"bytes"

	"github.com/roughstack/challenges/challenges/chunk-store-easy/contract"
)

// Kind identifies the shape of a generated blob.
type Kind uint8

const (
	// KindEmpty is a zero-byte blob.
	KindEmpty Kind = iota
	// KindOneByte is a one-byte blob.
	KindOneByte
	// KindCompressible is a repeated-byte blob larger than one block.
	KindCompressible
	// KindIncompressible is a deterministic random-byte blob.
	KindIncompressible
	// KindMixed mixes repeated and random regions.
	KindMixed
)

// Blob is one deterministic object to store. Duplicate entries intentionally
// reuse an earlier (ID, Version) pair; the harness expects Put to reject them.
type Blob struct {
	ID      uint64
	Version uint64
	Data    []byte
	Kind    Kind
}

// DefaultMaxBlobBytes is used when Config.MaxBlobBytes is unset or
// non-positive. It keeps the public smoke workload small while still covering
// multi-block blobs.
const DefaultMaxBlobBytes = 4*int(contract.BlockSize) + 17

// Config controls deterministic public generation without exposing ranked
// workload distributions.
type Config struct {
	Seed         uint64
	Blobs        int
	MaxBlobBytes int
}

// Generate produces a deterministic blob list. Every seventh blob (index 6,
// 13, 20, ...) duplicates the key of the blob five positions earlier so the
// public workload exercises duplicate ID/version handling deterministically.
func Generate(config Config) []Blob {
	if config.Blobs <= 0 {
		return nil
	}
	maxBytes := config.MaxBlobBytes
	if maxBytes <= 0 {
		maxBytes = DefaultMaxBlobBytes
	}
	if maxBytes <= int(contract.BlockSize) {
		maxBytes = int(contract.BlockSize) + 1
	}
	if uint64(maxBytes) > contract.MaxBlobBytes {
		maxBytes = int(contract.MaxBlobBytes)
	}

	random := splitMix64{state: config.Seed}
	blobs := make([]Blob, config.Blobs)
	for index := range blobs {
		if index > 0 && index%7 == 6 {
			base := &blobs[index-5]
			blobs[index] = Blob{
				ID:      base.ID,
				Version: base.Version,
				Data:    dataFor(config.Seed, base.ID, base.Version, len(base.Data), base.Kind),
				Kind:    base.Kind,
			}
			continue
		}

		id := uint64(index + 1)
		version := 1 + random.next()%5
		kind := blobKind(index)
		size := blobSize(kind, maxBytes, &random)
		blobs[index] = Blob{
			ID:      id,
			Version: version,
			Data:    dataFor(config.Seed, id, version, size, kind),
			Kind:    kind,
		}
	}
	return blobs
}

func blobKind(index int) Kind {
	switch index % 10 {
	case 0:
		return KindEmpty
	case 1:
		return KindOneByte
	case 2, 3, 4:
		return KindCompressible
	case 5, 6, 7:
		return KindIncompressible
	default:
		return KindMixed
	}
}

func blobSize(kind Kind, maxBytes int, random *splitMix64) int {
	switch kind {
	case KindEmpty:
		return 0
	case KindOneByte:
		return 1
	case KindCompressible:
		min := int(contract.BlockSize) + 1
		return min + int(random.next()%uint64(maxBytes-min+1))
	case KindIncompressible:
		min := int(contract.BlockSize) + 1
		return min + int(random.next()%uint64(maxBytes-min+1))
	case KindMixed:
		min := 2*int(contract.BlockSize) + 1
		if maxBytes < min {
			min = int(contract.BlockSize) + 1
		}
		return min + int(random.next()%uint64(maxBytes-min+1))
	default:
		return 0
	}
}

// dataFor returns deterministic payload bytes for a generated blob.
func dataFor(seed, id, version uint64, size int, kind Kind) []byte {
	switch kind {
	case KindEmpty:
		return nil
	case KindOneByte:
		return []byte{byte(seed ^ rotateLeft(id, 17) ^ rotateLeft(version, 41))}
	case KindCompressible:
		fill := byte(seed ^ rotateLeft(id, 17) ^ rotateLeft(version, 41))
		return bytes.Repeat([]byte{fill}, size)
	case KindIncompressible:
		return deterministicRandom(seed, id, version, size)
	case KindMixed:
		data := deterministicRandom(seed^0x5bd1e995, id, version, size)
		// Overwrite alternating fixed-size runs with a repeated byte so the
		// blob contains both compressible and incompressible regions.
		fill := byte(seed ^ rotateLeft(version, 13) ^ rotateLeft(id, 29))
		run := int(contract.BlockSize) / 2
		for off := 0; off < size; off += run {
			end := off + run
			if end > size {
				end = size
			}
			for index := off; index < end; index++ {
				data[index] = fill
			}
		}
		return data
	default:
		return nil
	}
}

func deterministicRandom(seed, id, version uint64, size int) []byte {
	data := make([]byte, size)
	state := seed ^ rotateLeft(id, 17) ^ rotateLeft(version, 41)
	for index := range data {
		state ^= state << 13
		state ^= state >> 7
		state ^= state << 17
		data[index] = byte(state)
	}
	return data
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

func rotateLeft(value uint64, shift uint) uint64 {
	return value<<shift | value>>(64-shift)
}
