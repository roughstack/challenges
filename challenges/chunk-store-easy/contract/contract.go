// Package contract defines the narrow contestant-owned surface for the
// chunk-store-easy arena: a versioned blob store over an injected
// deterministic Device. Everything else — the device, workload, corruption
// injection, accounting, correctness gates, and result construction — is owned
// by the trusted harness.
package contract

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
)

// magic is the fixed byte sequence that starts every blob header. It is
// private so contestant code cannot mutate the parser's ground truth.
var magic = [4]byte{'C', 'H', 'N', 'K'}

// MagicBytes returns a copy of the fixed blob-header magic. Mutating the
// returned array has no effect on the contract's internal state.
func MagicBytes() [4]byte { return magic }

// Version is the only on-device format version accepted by this arena.
const Version = 1

// BlockSize is the fixed raw chunk size. Every non-final block of a blob is
// exactly this many raw bytes; the final block may be shorter.
const BlockSize uint64 = 4096

// MaxBlobBytes is the strict upper bound on a single blob size. Put and
// recovery validate a declared size against this bound before they allocate.
const MaxBlobBytes uint64 = 1 << 20

// MaxBlocksPerBlob is the strict upper bound on the number of blocks in a
// blob. It is derived from MaxBlobBytes and BlockSize.
const MaxBlocksPerBlob uint64 = MaxBlobBytes / BlockSize

// MaxBlockPayloadBytes is the strict upper bound on an encoded block payload.
// Raw blocks never exceed BlockSize, and compressed blocks are stored only
// when their encoded length is strictly smaller than raw, so this bound is
// enough to reject corrupt encoded lengths before allocation.
const MaxBlockPayloadBytes uint64 = BlockSize

// Blob header layout:
//
//	magic(4) | version(1) | id(8) | blobVersion(8) | size(8) |
//	blockCount(4) | checksum(4)
const (
	// BlobHeaderSize is the number of bytes in a complete blob header.
	BlobHeaderSize = 4 + 1 + 8 + 8 + 8 + 4 + 4
	// BlobHeaderChecksumSize is the trailing blob-header checksum size.
	BlobHeaderChecksumSize = 4
)

// Block header layout:
//
//	rawLen(4) | encLen(4) | checksum(4)
const (
	// BlockHeaderSize is the number of bytes in a complete block header.
	BlockHeaderSize = 4 + 4 + 4
	// BlockHeaderChecksumSize is the trailing block-header checksum size.
	BlockHeaderChecksumSize = 4
)

// Store is the complete contestant-owned surface for this arena.
//
// Blobs are immutable and keyed by (id, version). Put stores a blob as a
// sequence of fixed-size raw chunks; each block is stored compressed only when
// compression strictly reduces its size. ReadAt reads only the blocks that
// intersect the requested range. The easy tier does not reclaim device bytes:
// Delete removes index entries and Compact is a no-op.
type Store interface {
	Put(id uint64, version uint64, data io.Reader, size uint64) error
	ReadAt(id uint64, version uint64, p []byte, off int64) (int, error)
	Delete(id uint64, version uint64) error
	Compact() error
	Close() error
	IndexBytes() uint64
}

// Factory constructs a fresh store over an injected device.
type Factory func(dir string, device Device, memoryLimit uint64) (Store, error)

// Device is the only storage API available to a store. It models one
// deterministic byte device. The harness owns the concrete device, its fault
// hooks, and its deterministic accounting. Contestant code must never touch os
// file APIs.
type Device interface {
	// Size returns the current committed device size in bytes.
	Size() (uint64, error)
	// Read returns exactly length bytes starting at offset. The returned slice
	// is owned by the caller. It errors if the range is outside [0, Size()].
	Read(offset uint64, length uint64) ([]byte, error)
	// Write copies data into the device at offset and may extend the device.
	Write(offset uint64, data []byte) error
	// Sync makes all prior writes durable.
	Sync() error
	// Truncate sets the device size. Shrinking drops trailing bytes; growing
	// zero-fills.
	Truncate(size uint64) error
}

// Documented errors returned by the store.
var (
	// ErrClosed is returned by every method after Close has been called.
	ErrClosed = errors.New("chunk store: closed")
	// ErrDuplicate reports a Put for an (id, version) that is already present.
	ErrDuplicate = errors.New("chunk store: duplicate id/version")
	// ErrNotFound reports a read or delete for an absent (id, version).
	ErrNotFound = errors.New("chunk store: blob not found")
	// ErrRange reports a negative ReadAt offset.
	ErrRange = errors.New("chunk store: negative read offset")
	// ErrBlobTooLarge reports a Put whose declared size exceeds MaxBlobBytes.
	ErrBlobTooLarge = errors.New("chunk store: blob exceeds maximum size")
	// ErrMemoryLimit reports that admitting a blob would exceed memoryLimit.
	ErrMemoryLimit = errors.New("chunk store: index memory limit exceeded")
	// ErrCorrupt reports an invalid magic/version, a truncated record, a
	// checksum mismatch, or an inconsistent block/blob header.
	ErrCorrupt = errors.New("chunk store: corrupt record")
	// ErrLengthBomb reports a declared block or blob length above the public
	// bound.
	ErrLengthBomb = errors.New("chunk store: declared length exceeds maximum")
)

// EncodeBlobHeader builds the complete blob header bytes for one blob. It
// copies no caller-owned slices; the returned header is owned by the caller.
func EncodeBlobHeader(id, version, size, blockCount uint64) []byte {
	header := make([]byte, BlobHeaderSize)
	copy(header[0:4], magic[:])
	header[4] = Version
	binary.BigEndian.PutUint64(header[5:13], id)
	binary.BigEndian.PutUint64(header[13:21], version)
	binary.BigEndian.PutUint64(header[21:29], size)
	binary.BigEndian.PutUint32(header[29:33], uint32(blockCount))
	sum := crc32.ChecksumIEEE(header[:BlobHeaderSize-BlobHeaderChecksumSize])
	binary.BigEndian.PutUint32(header[33:37], sum)
	return header
}

// ParseBlobHeader decodes a blob header. It returns ok=false when the header is
// shorter than BlobHeaderSize, the magic is invalid, or the version is unknown.
// It does not verify the trailing checksum.
func ParseBlobHeader(header []byte) (id, version, size, blockCount uint64, ok bool) {
	if len(header) < BlobHeaderSize {
		return 0, 0, 0, 0, false
	}
	if !equalMagic(header[0:4]) || header[4] != Version {
		return 0, 0, 0, 0, false
	}
	id = binary.BigEndian.Uint64(header[5:13])
	version = binary.BigEndian.Uint64(header[13:21])
	size = binary.BigEndian.Uint64(header[21:29])
	blockCount = uint64(binary.BigEndian.Uint32(header[29:33]))
	return id, version, size, blockCount, true
}

// VerifyBlobHeader reports whether a complete blob header has a valid checksum.
func VerifyBlobHeader(header []byte) bool {
	if len(header) < BlobHeaderSize {
		return false
	}
	body := header[:BlobHeaderSize-BlobHeaderChecksumSize]
	want := binary.BigEndian.Uint32(header[BlobHeaderSize-BlobHeaderChecksumSize:])
	return want == crc32.ChecksumIEEE(body)
}

// ValidateBlobHeader checks a decoded blob header against the public size and
// block-count bounds. It returns ErrLengthBomb for an over-large size or block
// count and ErrCorrupt for a size/block-count mismatch.
func ValidateBlobHeader(size, blockCount uint64) error {
	if size > MaxBlobBytes || blockCount > MaxBlocksPerBlob {
		return ErrLengthBomb
	}
	want := (size + BlockSize - 1) / BlockSize
	if size == 0 {
		want = 0
	}
	if blockCount != want {
		return ErrCorrupt
	}
	return nil
}

// EncodeBlockRecord builds one complete on-device block record:
//
//	rawLen(4) | encLen(4) | checksum(4) | payload(encLen)
//
// The checksum covers the raw/encoded length fields and the payload, so a
// flipped payload byte is always detected by VerifyBlockRecord.
func EncodeBlockRecord(rawLen, encLen uint32, payload []byte) []byte {
	record := make([]byte, BlockHeaderSize+len(payload))
	binary.BigEndian.PutUint32(record[0:4], rawLen)
	binary.BigEndian.PutUint32(record[4:8], encLen)
	sum := crc32.ChecksumIEEE(record[:BlockHeaderSize-BlockHeaderChecksumSize])
	sum = crc32.Update(sum, crc32.IEEETable, payload)
	binary.BigEndian.PutUint32(record[BlockHeaderSize-BlockHeaderChecksumSize:BlockHeaderSize], sum)
	copy(record[BlockHeaderSize:], payload)
	return record
}

// ParseBlockHeader decodes a block header. It returns ok=false only when the
// header is shorter than BlockHeaderSize; callers must validate the decoded
// lengths separately.
func ParseBlockHeader(header []byte) (rawLen, encLen uint32, ok bool) {
	if len(header) < BlockHeaderSize {
		return 0, 0, false
	}
	rawLen = binary.BigEndian.Uint32(header[0:4])
	encLen = binary.BigEndian.Uint32(header[4:8])
	return rawLen, encLen, true
}

// ValidateBlockRecord checks decoded block lengths against the public bounds.
// A block with rawLen == 0 is malformed, and a rawLen or encLen above
// MaxBlockPayloadBytes is a length bomb. Encoded length may never exceed raw
// length: compressed blocks are stored only when strictly smaller, and raw
// blocks store exactly rawLen.
func ValidateBlockRecord(rawLen, encLen uint32) error {
	if rawLen == 0 || rawLen > uint32(MaxBlockPayloadBytes) {
		if rawLen > uint32(MaxBlockPayloadBytes) {
			return ErrLengthBomb
		}
		return ErrCorrupt
	}
	if encLen == 0 || encLen > rawLen || encLen > uint32(MaxBlockPayloadBytes) {
		if encLen > uint32(MaxBlockPayloadBytes) {
			return ErrLengthBomb
		}
		return ErrCorrupt
	}
	return nil
}

// VerifyBlockRecord reports whether a block header plus its payload has a valid
// checksum. It never allocates a combined buffer.
func VerifyBlockRecord(header, payload []byte) bool {
	if len(header) < BlockHeaderSize {
		return false
	}
	sum := crc32.ChecksumIEEE(header[:BlockHeaderSize-BlockHeaderChecksumSize])
	sum = crc32.Update(sum, crc32.IEEETable, payload)
	return binary.BigEndian.Uint32(header[BlockHeaderSize-BlockHeaderChecksumSize:]) == sum
}

func equalMagic(b []byte) bool {
	m := magic
	return len(b) == len(m) && b[0] == m[0] && b[1] == m[1] && b[2] == m[2] && b[3] == m[3]
}
