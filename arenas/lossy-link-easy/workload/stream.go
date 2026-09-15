// Package workload generates the deterministic public application stream. The
// harness owns this stream; it is the exact byte sequence the endpoints must
// transfer once and in order.
package workload

// Generate returns a deterministic byte stream of the requested length derived
// entirely from the 64-bit seed. Empty and boundary-sized streams are legal.
func Generate(seed uint64, length uint64) []byte {
	if length == 0 {
		return []byte{}
	}
	if length > maxStreamBytes {
		length = maxStreamBytes
	}

	stream := make([]byte, int(length))
	rng := splitMix64{state: seed}
	for index := range stream {
		stream[index] = byte(rng.next())
	}
	return stream
}

// maxStreamBytes bounds public workload generation to a fast, in-memory size.
const maxStreamBytes = 1 << 20

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
