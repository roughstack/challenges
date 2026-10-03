package starter

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"

	"github.com/roughstack/challenges/challenges/chunk-store-easy/contract"
)

// byteDevice is a minimal deterministic Device for public starter tests. The
// production device with fault hooks lives in the harness package.
type byteDevice struct {
	data []byte
}

func (d *byteDevice) Size() (uint64, error) { return uint64(len(d.data)), nil }

func (d *byteDevice) Read(offset uint64, length uint64) ([]byte, error) {
	if offset > uint64(len(d.data)) || length > uint64(len(d.data))-offset {
		return nil, errors.New("byteDevice: read out of range")
	}
	return append([]byte(nil), d.data[offset:offset+length]...), nil
}

func (d *byteDevice) Write(offset uint64, p []byte) error {
	end := offset + uint64(len(p))
	if end > uint64(len(d.data)) {
		grown := make([]byte, end)
		copy(grown, d.data)
		d.data = grown
	}
	copy(d.data[offset:], p)
	return nil
}

func (d *byteDevice) Sync() error { return nil }

func (d *byteDevice) Truncate(size uint64) error {
	if size > uint64(len(d.data)) {
		grown := make([]byte, size)
		copy(grown, d.data)
		d.data = grown
		return nil
	}
	d.data = d.data[:size]
	return nil
}

func openTestStore(t *testing.T, device contract.Device, memoryLimit uint64) contract.Store {
	t.Helper()
	store, err := OpenStore("", device, memoryLimit)
	if err != nil {
		t.Fatalf("OpenStore failed: %v", err)
	}
	return store
}

func putTestBlob(t *testing.T, store contract.Store, id, version uint64, data []byte) {
	t.Helper()
	if err := store.Put(id, version, bytes.NewReader(data), uint64(len(data))); err != nil {
		t.Fatalf("Put(%d, %d) failed: %v", id, version, err)
	}
}

func readTestAll(t *testing.T, store contract.Store, id, version uint64, size int) []byte {
	t.Helper()
	p := make([]byte, size)
	n, err := store.ReadAt(id, version, p, 0)
	if err != nil {
		t.Fatalf("ReadAt(%d, %d) failed: %v", id, version, err)
	}
	if n != size {
		t.Fatalf("ReadAt(%d, %d) returned %d bytes, want %d", id, version, n, size)
	}
	return p[:n]
}

func deterministicRandomBytes(seed uint64, size int) []byte {
	data := make([]byte, size)
	state := seed
	for index := range data {
		state ^= state << 13
		state ^= state >> 7
		state ^= state << 17
		data[index] = byte(state)
	}
	return data
}

func TestCompressibleBlobStoresBelowRawAndReadsBack(t *testing.T) {
	raw := bytes.Repeat([]byte{0x5a}, int(contract.BlockSize)*3+17)
	device := &byteDevice{}
	store := openTestStore(t, device, 1<<20)
	putTestBlob(t, store, 1, 1, raw)

	if got, _ := device.Size(); got >= uint64(len(raw)) {
		t.Fatalf("stored bytes = %d, want below raw size %d", got, len(raw))
	}
	if got := readTestAll(t, store, 1, 1, len(raw)); !bytes.Equal(got, raw) {
		t.Fatal("compressible blob readback mismatch")
	}
}

func TestIncompressibleBlobStoresRaw(t *testing.T) {
	raw := deterministicRandomBytes(0x9e3779b97f4a7c15, int(contract.BlockSize)*2+13)
	device := &byteDevice{}
	store := openTestStore(t, device, 1<<20)
	putTestBlob(t, store, 1, 1, raw)

	if got, _ := device.Size(); got <= uint64(len(raw)) {
		t.Fatalf("stored bytes = %d, want raw payload plus headers", got)
	}
	if got := readTestAll(t, store, 1, 1, len(raw)); !bytes.Equal(got, raw) {
		t.Fatal("incompressible blob readback mismatch")
	}

	// Every block must be raw: encoded length equals raw length.
	header, err := device.Read(0, contract.BlobHeaderSize)
	if err != nil {
		t.Fatal(err)
	}
	_, _, size, blockCount, ok := contract.ParseBlobHeader(header)
	if !ok || size != uint64(len(raw)) {
		t.Fatalf("unexpected blob header: size=%d ok=%v", size, ok)
	}
	offset := uint64(contract.BlobHeaderSize)
	for index := uint64(0); index < blockCount; index++ {
		blockHeader, err := device.Read(offset, contract.BlockHeaderSize)
		if err != nil {
			t.Fatal(err)
		}
		rawLen, encLen, ok := contract.ParseBlockHeader(blockHeader)
		if !ok {
			t.Fatalf("block %d header is invalid", index)
		}
		if rawLen != encLen {
			t.Fatalf("block %d is encoded length %d, want raw length %d", index, encLen, rawLen)
		}
		offset += contract.BlockHeaderSize + uint64(encLen)
	}
}

func TestCrossBlockReadReturnsExactSlice(t *testing.T) {
	raw := bytes.Repeat([]byte{0x6d}, int(contract.BlockSize)*3+7)
	for index := range raw {
		raw[index] = byte(index * 31)
	}
	device := &byteDevice{}
	store := openTestStore(t, device, 1<<20)
	putTestBlob(t, store, 7, 2, raw)

	off := int64(contract.BlockSize - 1)
	length := int(contract.BlockSize)*2 + 2
	p := make([]byte, length)
	n, err := store.ReadAt(7, 2, p, off)
	if err != nil {
		t.Fatalf("cross-block ReadAt failed: %v", err)
	}
	if n != length {
		t.Fatalf("cross-block ReadAt returned %d bytes, want %d", n, length)
	}
	if !bytes.Equal(p, raw[off:off+int64(length)]) {
		t.Fatal("cross-block read returned the wrong slice")
	}
}

func TestEmptyAndOneByteBlobsRoundTrip(t *testing.T) {
	device := &byteDevice{}
	store := openTestStore(t, device, 1<<20)

	putTestBlob(t, store, 10, 1, nil)
	if p := make([]byte, 0); true {
		n, err := store.ReadAt(10, 1, p, 0)
		if err != nil || n != 0 {
			t.Fatalf("empty blob ReadAt = (%d, %v), want (0, nil)", n, err)
		}
	}

	one := []byte{0x7f}
	putTestBlob(t, store, 11, 1, one)
	if got := readTestAll(t, store, 11, 1, 1); !bytes.Equal(got, one) {
		t.Fatal("one-byte blob readback mismatch")
	}
}

func TestCorruptHeaderRejectsImpossibleLengthWithoutHugeAllocation(t *testing.T) {
	raw := bytes.Repeat([]byte{0x11}, int(contract.BlockSize)*2)
	device := &byteDevice{}
	store := openTestStore(t, device, 1<<20)
	putTestBlob(t, store, 3, 3, raw)

	// The first block header starts immediately after the blob header. Its
	// encoded length field is bytes [4:8] of the block header.
	encLenOffset := uint64(contract.BlobHeaderSize) + 4
	binary.BigEndian.PutUint32(device.data[encLenOffset:encLenOffset+4], uint32(contract.MaxBlockPayloadBytes)+1)

	p := make([]byte, len(raw))
	n, err := store.ReadAt(3, 3, p, 0)
	if n != 0 {
		t.Fatalf("corrupt ReadAt returned %d bytes, want 0", n)
	}
	if !errors.Is(err, contract.ErrLengthBomb) {
		t.Fatalf("corrupt ReadAt error = %v, want ErrLengthBomb", err)
	}
}

func TestOpenStoreRejectsNonUniformBlockFraming(t *testing.T) {
	size := contract.BlockSize + 1
	rawLens := []uint32{uint32(contract.BlockSize - 1), 2}
	data := contract.EncodeBlobHeader(1, 1, size, 2)
	for _, rawLen := range rawLens {
		payload := make([]byte, rawLen)
		data = append(data, contract.EncodeBlockRecord(rawLen, rawLen, payload)...)
	}
	device := &byteDevice{data: data}

	if _, err := OpenStore("", device, 1<<20); !errors.Is(err, contract.ErrCorrupt) {
		t.Fatalf("OpenStore error = %v, want ErrCorrupt", err)
	}
}

func TestDuplicatePutIsRejectedAndOriginalIsPreserved(t *testing.T) {
	device := &byteDevice{}
	store := openTestStore(t, device, 1<<20)
	first := []byte("immutable")
	putTestBlob(t, store, 1, 2, first)

	if err := store.Put(1, 2, bytes.NewReader([]byte("replacement")), uint64(len("replacement"))); !errors.Is(err, contract.ErrDuplicate) {
		t.Fatalf("duplicate Put error = %v, want ErrDuplicate", err)
	}
	if got := readTestAll(t, store, 1, 2, len(first)); !bytes.Equal(got, first) {
		t.Fatal("duplicate Put changed the original immutable blob")
	}
}

func TestDeleteNotFoundAndClose(t *testing.T) {
	device := &byteDevice{}
	store := openTestStore(t, device, 1<<20)
	putTestBlob(t, store, 1, 1, []byte("hello"))

	if err := store.Delete(1, 1); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if _, err := store.ReadAt(1, 1, make([]byte, 1), 0); !errors.Is(err, contract.ErrNotFound) {
		t.Fatalf("ReadAt after Delete = %v, want ErrNotFound", err)
	}
	if err := store.Delete(1, 1); !errors.Is(err, contract.ErrNotFound) {
		t.Fatalf("second Delete = %v, want ErrNotFound", err)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close is not idempotent: %v", err)
	}
	if err := store.Put(2, 1, bytes.NewReader([]byte{1}), 1); !errors.Is(err, contract.ErrClosed) {
		t.Fatalf("Put after Close = %v, want ErrClosed", err)
	}
	if _, err := store.ReadAt(1, 1, make([]byte, 1), 0); !errors.Is(err, contract.ErrClosed) {
		t.Fatalf("ReadAt after Close = %v, want ErrClosed", err)
	}
	if err := store.Compact(); !errors.Is(err, contract.ErrClosed) {
		t.Fatalf("Compact after Close = %v, want ErrClosed", err)
	}
}

func TestMemoryLimitRejectsAdmission(t *testing.T) {
	device := &byteDevice{}
	store := openTestStore(t, device, 0)
	if err := store.Put(1, 1, bytes.NewReader([]byte{1}), 1); !errors.Is(err, contract.ErrMemoryLimit) {
		t.Fatalf("Put over memory limit = %v, want ErrMemoryLimit", err)
	}
	if store.IndexBytes() != 0 {
		t.Fatalf("index bytes = %d, want 0 after rejected Put", store.IndexBytes())
	}
}

func TestReadAtRangeSemantics(t *testing.T) {
	device := &byteDevice{}
	store := openTestStore(t, device, 1<<20)
	data := []byte("0123456789")
	putTestBlob(t, store, 1, 1, data)

	p := make([]byte, 4)
	n, err := store.ReadAt(1, 1, p, 8)
	if err != io.EOF || n != 2 {
		t.Fatalf("partial read = (%d, %v), want (2, io.EOF)", n, err)
	}
	if !bytes.Equal(p[:n], data[8:]) {
		t.Fatalf("partial read bytes = %q, want %q", p[:n], data[8:])
	}

	if _, err := store.ReadAt(1, 1, make([]byte, 1), -1); !errors.Is(err, contract.ErrRange) {
		t.Fatalf("negative read = %v, want ErrRange", err)
	}
	if _, err := store.ReadAt(1, 1, make([]byte, 1), int64(len(data))); !errors.Is(err, io.EOF) {
		t.Fatalf("past-end read = %v, want io.EOF", err)
	}
}
