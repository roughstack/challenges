// Package workload generates the deterministic public record stream for the
// journal-recovery-easy arena.
package workload

import "github.com/roughstack/challenges/challenges/journal-recovery-easy/contract"

// Config controls deterministic public record generation without exposing
// ranked workload distributions.
type Config struct {
	Seed       uint64
	Records    int
	MaxPayload int
}

// DefaultMaxPayload is the payload bound used when the config leaves it unset
// or non-positive.
const DefaultMaxPayload = 4096

// Generate produces the ordered record list. Records carry their pre-assigned
// LSN 1..N, and the journal must return that LSN from Append.
func Generate(config Config) []contract.Record {
	if config.Records <= 0 {
		return nil
	}
	if config.MaxPayload <= 0 {
		config.MaxPayload = DefaultMaxPayload
	}

	random := splitMix64{state: config.Seed}
	records := make([]contract.Record, config.Records)
	for index := range records {
		lsn := uint64(index + 1)
		size := 1 + int(random.next()%uint64(config.MaxPayload))
		records[index] = contract.Record{
			LSN:     lsn,
			Payload: Payload(config.Seed, lsn, size),
		}
	}
	return records
}

// Payload returns the deterministic payload bytes for (seed, lsn, size).
func Payload(seed, lsn uint64, size int) []byte {
	payload := make([]byte, size)
	state := seed ^ rotateLeft(lsn, 17) ^ rotateLeft(uint64(size), 41)
	for index := range payload {
		state ^= state << 13
		state ^= state >> 7
		state ^= state << 17
		payload[index] = byte(state)
	}
	return payload
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
