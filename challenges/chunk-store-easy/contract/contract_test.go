package contract

import (
	"bytes"
	"errors"
	"testing"
)

func TestBlobHeaderRoundTrip(t *testing.T) {
	header := EncodeBlobHeader(7, 3, BlockSize*3+1, 4)

	id, version, size, blockCount, ok := ParseBlobHeader(header)
	if !ok {
		t.Fatal("ParseBlobHeader rejected a valid header")
	}
	if id != 7 || version != 3 || size != BlockSize*3+1 || blockCount != 4 {
		t.Fatalf("ParseBlobHeader = (%d, %d, %d, %d), want (7, 3, %d, 4)", id, version, size, blockCount, BlockSize*3+1)
	}
	if !VerifyBlobHeader(header) {
		t.Fatal("VerifyBlobHeader rejected a valid header")
	}

	corrupt := append([]byte(nil), header...)
	corrupt[0] ^= 0xff
	if VerifyBlobHeader(corrupt) {
		t.Fatal("VerifyBlobHeader accepted a corrupt header")
	}
}

func TestParseBlobHeaderRejectsBadMagicAndVersion(t *testing.T) {
	header := EncodeBlobHeader(1, 1, 1, 1)
	badMagic := append([]byte(nil), header...)
	badMagic[0] ^= 0xff
	if _, _, _, _, ok := ParseBlobHeader(badMagic); ok {
		t.Fatal("ParseBlobHeader accepted a bad magic byte")
	}

	badVersion := append([]byte(nil), header...)
	badVersion[4] = Version + 1
	if _, _, _, _, ok := ParseBlobHeader(badVersion); ok {
		t.Fatal("ParseBlobHeader accepted an unknown version")
	}
}

func TestValidateBlobHeaderBounds(t *testing.T) {
	if err := ValidateBlobHeader(MaxBlobBytes, MaxBlocksPerBlob); err != nil {
		t.Fatalf("maximum legal blob was rejected: %v", err)
	}
	if !errors.Is(ValidateBlobHeader(MaxBlobBytes+1, 0), ErrLengthBomb) {
		t.Fatal("oversized blob did not return ErrLengthBomb")
	}
	if !errors.Is(ValidateBlobHeader(BlockSize, 2), ErrCorrupt) {
		t.Fatal("block-count mismatch did not return ErrCorrupt")
	}
	if err := ValidateBlobHeader(0, 0); err != nil {
		t.Fatalf("empty blob was rejected: %v", err)
	}
}

func TestBlockRecordRoundTripAndChecksum(t *testing.T) {
	payload := bytes.Repeat([]byte{0xab}, int(BlockSize/2))
	record := EncodeBlockRecord(uint32(BlockSize), uint32(BlockSize/2), payload)
	header := record[:BlockHeaderSize]
	rawLen, encLen, ok := ParseBlockHeader(header)
	if !ok || rawLen != uint32(BlockSize) || encLen != uint32(BlockSize/2) {
		t.Fatalf("ParseBlockHeader = (%d, %d, %v), want (%d, %d, true)", rawLen, encLen, ok, BlockSize, BlockSize/2)
	}
	if !VerifyBlockRecord(header, record[BlockHeaderSize:]) {
		t.Fatal("VerifyBlockRecord rejected a valid record")
	}

	badPayload := append([]byte(nil), payload...)
	badPayload[0] ^= 0x01
	if VerifyBlockRecord(header, badPayload) {
		t.Fatal("VerifyBlockRecord accepted a payload with a flipped byte")
	}
}

func TestValidateBlockRecordRejectsLengthBombs(t *testing.T) {
	if err := ValidateBlockRecord(uint32(BlockSize), uint32(BlockSize/2)); err != nil {
		t.Fatalf("valid compressed block was rejected: %v", err)
	}
	if err := ValidateBlockRecord(uint32(BlockSize), uint32(BlockSize)); err != nil {
		t.Fatalf("valid raw block was rejected: %v", err)
	}
	if !errors.Is(ValidateBlockRecord(uint32(BlockSize)+1, 1), ErrLengthBomb) {
		t.Fatal("over-large raw length did not return ErrLengthBomb")
	}
	if !errors.Is(ValidateBlockRecord(uint32(BlockSize), uint32(BlockSize)+1), ErrLengthBomb) {
		t.Fatal("over-large encoded length did not return ErrLengthBomb")
	}
	if !errors.Is(ValidateBlockRecord(uint32(BlockSize), uint32(BlockSize)+1), ErrLengthBomb) {
		t.Fatal("encoded length above raw length did not return ErrLengthBomb")
	}
	if !errors.Is(ValidateBlockRecord(0, 0), ErrCorrupt) {
		t.Fatal("zero raw length did not return ErrCorrupt")
	}
}

func TestMagicBytesReturnsAnIndependentCopy(t *testing.T) {
	first := MagicBytes()
	if first != [4]byte{'C', 'H', 'N', 'K'} {
		t.Fatalf("MagicBytes() = %v, want CHNK", first)
	}
	first[0] = 'X'
	second := MagicBytes()
	if second != [4]byte{'C', 'H', 'N', 'K'} {
		t.Fatalf("MagicBytes() after mutation = %v, want CHNK", second)
	}
}

func TestPublicBoundsAreReasonable(t *testing.T) {
	if BlockSize == 0 || MaxBlobBytes < BlockSize || MaxBlocksPerBlob == 0 {
		t.Fatal("public chunk bounds are not reasonable")
	}
	if MaxBlobBytes > 1<<30 {
		t.Fatal("MaxBlobBytes is too large to bound allocations safely")
	}
}
