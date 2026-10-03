// Package contract defines the narrow contestant-owned surface for the
// journal-recovery-easy arena: a length-delimited record journal over an
// injected deterministic Device. Everything else — the device, fault injection,
// crash simulation, workload, accounting, correctness gates, and result
// construction — is owned by the trusted harness.
package contract

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

// magic is the fixed byte sequence that marks the start of a journal frame.
// It is private so contestant code cannot mutate the parser's ground truth.
var magic = [4]byte{'J', 'R', 'N', 'L'}

// MagicBytes returns a copy of the fixed frame magic. Mutating the returned
// array has no effect on the contract's internal state.
func MagicBytes() [4]byte { return magic }

// Frame layout constants. A frame is laid out as:
//
//	magic(4) | version(1) | length(4, big-endian) | seq(8, big-endian) |
//	payload(length) | checksum(4, big-endian)
//
// The checksum is CRC-32 (IEEE) over every preceding byte of the frame.
const (
	// Version is the only frame format version accepted by this arena.
	Version = 1

	// FrameHeaderSize is the number of bytes before the payload.
	FrameHeaderSize = 4 + 1 + 4 + 8

	// ChecksumSize is the trailing checksum field size.
	ChecksumSize = 4

	// FrameOverhead is the fixed cost of framing one record.
	FrameOverhead = FrameHeaderSize + ChecksumSize

	// MaxPayloadBytes is the strict upper bound on a declared payload length.
	// Recovery validates a declared length against this bound before it
	// allocates, so a corrupt length field cannot force a huge allocation.
	MaxPayloadBytes = 1 << 20
)

// Record is one durable journal entry.
type Record struct {
	LSN     uint64
	Payload []byte
}

// RecoveryStatus describes how the journal ended during recovery.
type RecoveryStatus uint8

const (
	// StatusEmpty reports a zero-length journal.
	StatusEmpty RecoveryStatus = iota
	// StatusClean reports a journal that ends exactly on a frame boundary and
	// contains no invalid region.
	StatusClean
	// StatusTornTail reports an incomplete final frame: either a partial header
	// or a plausible declared length that extends past the end of the device.
	StatusTornTail
	// StatusTailGarbage reports a trailing region that does not begin with a
	// valid frame header and is not followed by any valid frame.
	StatusTailGarbage
	// StatusCorrupt reports a fully present frame that failed validation, a
	// length bomb, or an interior malformed region followed by a valid frame.
	StatusCorrupt
)

// String returns a stable, human-readable status name.
func (s RecoveryStatus) String() string {
	switch s {
	case StatusEmpty:
		return "empty"
	case StatusClean:
		return "clean"
	case StatusTornTail:
		return "torn-tail"
	case StatusTailGarbage:
		return "tail-garbage"
	case StatusCorrupt:
		return "corrupt"
	default:
		return "unknown"
	}
}

// RecoveryInfo reports what Recover found and applied.
type RecoveryInfo struct {
	Status         RecoveryStatus
	RecordsApplied uint64
	LastLSN        uint64
}

// Device is the only storage API available to a journal. It models one
// deterministic byte device with an explicit durability boundary: writes and
// truncations are visible immediately but are not durable until Sync.
//
// The harness owns the concrete device, its fault hooks, and its deterministic
// accounting. Contestant code must never touch os file APIs.
type Device interface {
	// Size returns the current committed device size in bytes.
	Size() (uint64, error)
	// Read returns exactly length bytes starting at offset. The returned slice
	// is owned by the caller. It errors if the range is outside [0, Size()].
	Read(offset uint64, length uint64) ([]byte, error)
	// Write copies data into the device at offset and may extend the device.
	// The change is not durable until Sync.
	Write(offset uint64, data []byte) error
	// Sync makes all prior writes and truncations durable.
	Sync() error
	// Truncate sets the device size. Shrinking drops trailing bytes; growing
	// zero-fills. Truncation is not durable until Sync.
	Truncate(size uint64) error
}

// Journal is the complete contestant-owned surface for this arena.
//
//   - Append frames record and writes it to the device, then returns the LSN
//     assigned to it. LSNs start at 1 and increase by one. Append copies the
//     payload before returning; later mutation of the caller's slice must not
//     change the journaled bytes. A record is not durable until Sync.
//   - Sync makes every record up to and including upto durable. It returns
//     ErrUnknownLSN when upto has not been appended.
//   - Recover scans the device, applies the longest valid prefix of records to
//     apply in order, and reports how the journal ended. A torn tail or trailing
//     garbage is reported as a non-error status; interior corruption and length
//     bombs are reported as errors. If apply returns an error, Recover stops and
//     returns that error unchanged.
//   - Close is terminal and idempotent. After Close every method returns
//     ErrClosed.
type Journal interface {
	Append(record Record) (uint64, error)
	Sync(upto uint64) error
	Recover(apply func(Record) error) (RecoveryInfo, error)
	Close() error
}

// Factory constructs a fresh journal over an injected device.
type Factory func(dir string, device Device) (Journal, error)

// Documented errors returned by the journal.
var (
	// ErrCorrupt reports a fully present frame that failed validation or an
	// interior malformed region followed by a valid frame.
	ErrCorrupt = errors.New("journal: corrupt record")
	// ErrLengthBomb reports a declared payload length above MaxPayloadBytes.
	ErrLengthBomb = errors.New("journal: declared length exceeds maximum")
	// ErrSequence reports a gap or duplicate in frame sequence numbers.
	ErrSequence = errors.New("journal: sequence discontinuity")
	// ErrClosed is returned by every method after Close has been called.
	ErrClosed = errors.New("journal: closed")
	// ErrUnknownLSN is returned by Sync for an LSN that was never appended.
	ErrUnknownLSN = errors.New("journal: unknown LSN")
)

// EncodeRecord builds the complete frame bytes for a record. It copies the
// payload, so the caller keeps ownership of its slice.
func EncodeRecord(record Record) []byte {
	frame := make([]byte, FrameHeaderSize+len(record.Payload)+ChecksumSize)
	copy(frame[0:4], magic[:])
	frame[4] = Version
	binary.BigEndian.PutUint32(frame[5:9], uint32(len(record.Payload)))
	binary.BigEndian.PutUint64(frame[9:17], record.LSN)
	copy(frame[17:], record.Payload)
	checksum := crc32.ChecksumIEEE(frame[:len(frame)-ChecksumSize])
	binary.BigEndian.PutUint32(frame[len(frame)-ChecksumSize:], checksum)
	return frame
}

// ParseHeader decodes a FrameHeaderSize-byte header. It returns ok=false when
// the magic or version is invalid.
func ParseHeader(header []byte) (length uint32, seq uint64, ok bool) {
	if len(header) < FrameHeaderSize {
		return 0, 0, false
	}
	if !equalMagic(header[0:4]) || header[4] != Version {
		return 0, 0, false
	}
	length = binary.BigEndian.Uint32(header[5:9])
	seq = binary.BigEndian.Uint64(header[9:17])
	return length, seq, true
}

// VerifyFrame reports whether a complete frame's checksum is valid.
func VerifyFrame(frame []byte) bool {
	if len(frame) < FrameHeaderSize+ChecksumSize {
		return false
	}
	body := frame[:len(frame)-ChecksumSize]
	return binary.BigEndian.Uint32(frame[len(frame)-ChecksumSize:]) == crc32.ChecksumIEEE(body)
}

func equalMagic(b []byte) bool {
	m := magic
	return len(b) == len(m) && b[0] == m[0] && b[1] == m[1] && b[2] == m[2] && b[3] == m[3]
}
