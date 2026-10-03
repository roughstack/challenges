// Package starter contains a deliberately simple, correct chunk store over the
// injected Device.
//
// The implementation is intentionally straightforward: Put splits a blob into
// fixed-size blocks, compresses each block with the standard-library flate
// codec only when the encoded block is strictly smaller, and appends one blob
// record to the device. OpenStore rebuilds the in-memory index by reading the
// whole device once. ReadAt decodes only the blocks that intersect the
// requested range. This is correct but non-optimal: it uses one device write
// per block header/payload and one whole-device read at open. That leaves the
// central random-read and indexing optimizations to the contestant.
package starter

import (
	"bytes"
	"compress/flate"
	"errors"
	"fmt"
	"io"

	"github.com/bytearena/arenas/arenas/chunk-store-easy/contract"
)

// Accounting constants describe the deterministic in-memory index model used by
// the starter's IndexBytes report. They intentionally overestimate Go map and
// slice overhead so index growth is visible and monotonic.
const (
	blobIndexBytes  uint64 = 64
	blockIndexBytes uint64 = 32
)

type blobKey struct {
	id      uint64
	version uint64
}

type blockMeta struct {
	offset uint64
	rawLen uint32
	encLen uint32
}

type blobMeta struct {
	size   uint64
	blocks []blockMeta
}

type store struct {
	device      contract.Device
	memoryLimit uint64
	closed      bool
	blobs       map[blobKey]*blobMeta
	indexBytes  uint64
}

// OpenStore opens a store over device. dir is reserved for persistent tiers and
// is ignored here. It reconstructs the in-memory index by scanning the device.
func OpenStore(_ string, device contract.Device, memoryLimit uint64) (contract.Store, error) {
	if device == nil {
		return nil, errors.New("chunk store: nil device")
	}
	s := &store{
		device:      device,
		memoryLimit: memoryLimit,
		blobs:       make(map[blobKey]*blobMeta),
	}
	if err := s.scanDevice(); err != nil {
		return nil, err
	}
	if s.indexBytes > memoryLimit {
		return nil, contract.ErrMemoryLimit
	}
	return s, nil
}

func (s *store) Put(id uint64, version uint64, data io.Reader, size uint64) error {
	if s.closed {
		return contract.ErrClosed
	}
	key := blobKey{id: id, version: version}
	if _, ok := s.blobs[key]; ok {
		return contract.ErrDuplicate
	}
	if size > contract.MaxBlobBytes {
		return contract.ErrBlobTooLarge
	}
	blockCount := (size + contract.BlockSize - 1) / contract.BlockSize
	if size == 0 {
		blockCount = 0
	}
	if blockCount > contract.MaxBlocksPerBlob {
		return contract.ErrLengthBomb
	}
	if data == nil {
		return errors.New("chunk store: nil reader")
	}

	cost := indexCost(blockCount)
	if s.indexBytes > s.memoryLimit || cost > s.memoryLimit-s.indexBytes {
		return contract.ErrMemoryLimit
	}

	start, err := s.device.Size()
	if err != nil {
		return err
	}

	// Reserve the blob header up front. If a later read or codec step fails,
	// the partial blob record is truncated back to start so the device never
	// contains an unindexed record.
	header := contract.EncodeBlobHeader(id, version, size, blockCount)
	if err := s.device.Write(start, header); err != nil {
		return err
	}
	offset := start + uint64(len(header))

	blocks := make([]blockMeta, 0, blockCount)
	for index := uint64(0); index < blockCount; index++ {
		rawLen := contract.BlockSize
		remaining := size - index*contract.BlockSize
		if remaining < rawLen {
			rawLen = remaining
		}

		raw := make([]byte, rawLen)
		if _, err := io.ReadFull(data, raw); err != nil {
			_ = s.device.Truncate(start)
			return fmt.Errorf("chunk store: read blob: %w", err)
		}

		payload := raw
		encLen := uint32(len(raw))
		if compressed, ok := compressBlock(raw); ok && len(compressed) < len(raw) {
			payload = compressed
			encLen = uint32(len(compressed))
		}

		record := contract.EncodeBlockRecord(uint32(len(raw)), encLen, payload)
		blockOffset := offset
		if err := s.device.Write(offset, record); err != nil {
			_ = s.device.Truncate(start)
			return err
		}
		offset += uint64(len(record))

		blocks = append(blocks, blockMeta{
			offset: blockOffset,
			rawLen: uint32(len(raw)),
			encLen: encLen,
		})
	}

	s.blobs[key] = &blobMeta{size: size, blocks: blocks}
	s.indexBytes += cost
	return nil
}

func (s *store) ReadAt(id uint64, version uint64, p []byte, off int64) (int, error) {
	if s.closed {
		return 0, contract.ErrClosed
	}
	meta, ok := s.blobs[blobKey{id: id, version: version}]
	if !ok {
		return 0, contract.ErrNotFound
	}
	if len(p) == 0 {
		return 0, nil
	}
	if off < 0 {
		return 0, contract.ErrRange
	}
	if uint64(off) >= meta.size {
		return 0, io.EOF
	}

	end := uint64(off)
	if uint64(len(p)) <= meta.size-end {
		end += uint64(len(p))
	} else {
		end = meta.size
	}

	written := 0
	for index := int(uint64(off) / contract.BlockSize); uint64(index) < uint64(len(meta.blocks)); index++ {
		block := meta.blocks[index]
		blockStart := uint64(index) * contract.BlockSize
		blockEnd := blockStart + uint64(block.rawLen)
		if blockEnd <= uint64(off) || blockStart >= end {
			continue
		}

		decoded, err := s.decodeBlock(block)
		if err != nil {
			return 0, err
		}

		intersectStart := uint64(off)
		if blockStart > intersectStart {
			intersectStart = blockStart
		}
		intersectEnd := end
		if blockEnd < intersectEnd {
			intersectEnd = blockEnd
		}

		srcStart := intersectStart - blockStart
		srcEnd := intersectEnd - blockStart
		copy(p[written:written+int(srcEnd-srcStart)], decoded[srcStart:srcEnd])
		written += int(srcEnd - srcStart)
		if uint64(written) == end-uint64(off) {
			break
		}
	}

	if uint64(written) < uint64(len(p)) {
		return written, io.EOF
	}
	return written, nil
}

func (s *store) decodeBlock(block blockMeta) ([]byte, error) {
	header, err := s.device.Read(block.offset, contract.BlockHeaderSize)
	if err != nil {
		return nil, err
	}
	rawLen, encLen, ok := contract.ParseBlockHeader(header)
	if !ok {
		return nil, contract.ErrCorrupt
	}
	if err := contract.ValidateBlockRecord(rawLen, encLen); err != nil {
		return nil, err
	}

	payload, err := s.device.Read(block.offset+contract.BlockHeaderSize, uint64(encLen))
	if err != nil {
		return nil, err
	}
	if !contract.VerifyBlockRecord(header, payload) {
		return nil, contract.ErrCorrupt
	}

	if encLen == rawLen {
		return payload, nil
	}

	// Compressed block: decode exactly rawLen bytes and reject any payload that
	// decompresses to more than the declared raw length.
	reader := flate.NewReader(bytes.NewReader(payload))
	defer reader.Close()
	decoded := make([]byte, rawLen)
	if _, err := io.ReadFull(reader, decoded); err != nil {
		return nil, contract.ErrCorrupt
	}
	var extra [1]byte
	if n, err := reader.Read(extra[:]); n != 0 || err != io.EOF {
		return nil, contract.ErrLengthBomb
	}
	return decoded, nil
}

func (s *store) Delete(id uint64, version uint64) error {
	if s.closed {
		return contract.ErrClosed
	}
	key := blobKey{id: id, version: version}
	meta, ok := s.blobs[key]
	if !ok {
		return contract.ErrNotFound
	}
	s.indexBytes -= indexCost(uint64(len(meta.blocks)))
	delete(s.blobs, key)
	return nil
}

func (s *store) Compact() error {
	if s.closed {
		return contract.ErrClosed
	}
	return nil
}

func (s *store) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	return s.device.Sync()
}

func (s *store) IndexBytes() uint64 {
	return s.indexBytes
}

// scanDevice reads the whole device once and reconstructs the in-memory blob
// and block index. It rejects malformed headers, inconsistent lengths, and
// length bombs before allocating large buffers.
func (s *store) scanDevice() error {
	size, err := s.device.Size()
	if err != nil {
		return err
	}
	if size == 0 {
		return nil
	}
	data, err := s.device.Read(0, size)
	if err != nil {
		return err
	}
	if uint64(len(data)) != size {
		return errors.New("chunk store: device returned a short read")
	}

	offset := uint64(0)
	for offset < size {
		if size-offset < contract.BlobHeaderSize {
			return contract.ErrCorrupt
		}
		header := data[offset : offset+contract.BlobHeaderSize]
		id, version, blobSize, blockCount, ok := contract.ParseBlobHeader(header)
		if !ok {
			return contract.ErrCorrupt
		}
		if !contract.VerifyBlobHeader(header) {
			return contract.ErrCorrupt
		}
		if err := contract.ValidateBlobHeader(blobSize, blockCount); err != nil {
			return err
		}

		key := blobKey{id: id, version: version}
		if _, exists := s.blobs[key]; exists {
			return contract.ErrCorrupt
		}

		meta := &blobMeta{size: blobSize, blocks: make([]blockMeta, 0, blockCount)}
		offset += contract.BlobHeaderSize
		remaining := blobSize
		for index := uint64(0); index < blockCount; index++ {
			if size-offset < contract.BlockHeaderSize {
				return contract.ErrCorrupt
			}
			blockHeader := data[offset : offset+contract.BlockHeaderSize]
			rawLen, encLen, ok := contract.ParseBlockHeader(blockHeader)
			if !ok {
				return contract.ErrCorrupt
			}
			if err := contract.ValidateBlockRecord(rawLen, encLen); err != nil {
				return err
			}
			// Reject non-uniform block framing: only the final block may be
			// shorter than BlockSize, and it must hold exactly the remaining
			// logical bytes.
			expectedRaw := contract.BlockSize
			if index == blockCount-1 {
				expectedRaw = blobSize - (blockCount-1)*contract.BlockSize
			}
			if uint64(rawLen) != expectedRaw {
				return contract.ErrCorrupt
			}
			if uint64(encLen) > size-(offset+contract.BlockHeaderSize) {
				return contract.ErrCorrupt
			}
			payload := data[offset+contract.BlockHeaderSize : offset+contract.BlockHeaderSize+uint64(encLen)]
			if !contract.VerifyBlockRecord(blockHeader, payload) {
				return contract.ErrCorrupt
			}

			meta.blocks = append(meta.blocks, blockMeta{
				offset: offset,
				rawLen: rawLen,
				encLen: encLen,
			})

			if uint64(rawLen) > remaining {
				return contract.ErrCorrupt
			}
			remaining -= uint64(rawLen)
			offset += contract.BlockHeaderSize + uint64(encLen)
		}
		if remaining != 0 {
			return contract.ErrCorrupt
		}

		s.blobs[key] = meta
		s.indexBytes += indexCost(blockCount)
		if s.indexBytes > s.memoryLimit {
			return contract.ErrMemoryLimit
		}
	}
	return nil
}

func indexCost(blockCount uint64) uint64 {
	return blobIndexBytes + blockCount*blockIndexBytes
}

var errCompressOverflow = errors.New("chunk store: compressed block exceeded bound")

// boundedBuffer is a write sink that refuses to grow past limit. It prevents a
// codec edge case from turning one bounded block into an unbounded allocation.
type boundedBuffer struct {
	buf      []byte
	limit    int
	overflow bool
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	if len(b.buf)+len(p) > b.limit {
		b.overflow = true
		return 0, errCompressOverflow
	}
	b.buf = append(b.buf, p...)
	return len(p), nil
}

// compressBlock returns the flate-compressed block and true when compression
// completed inside the pre-computed bound for one block. The caller stores raw
// when compression overflows or does not reduce size.
func compressBlock(raw []byte) ([]byte, bool) {
	if len(raw) == 0 {
		return nil, false
	}
	limit := uint64(len(raw)) + uint64(len(raw))/16 + 64
	if limit > uint64(int(^uint(0)>>1)) {
		return nil, false
	}
	buf := &boundedBuffer{limit: int(limit)}
	writer, err := flate.NewWriter(buf, flate.DefaultCompression)
	if err != nil {
		return nil, false
	}
	if _, err := writer.Write(raw); err != nil {
		_ = writer.Close()
		return nil, false
	}
	if err := writer.Close(); err != nil {
		return nil, false
	}
	if buf.overflow {
		return nil, false
	}
	return buf.buf, true
}
