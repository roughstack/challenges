package contract

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestEncodeRecordCopiesPayload(t *testing.T) {
	payload := []byte("hello")
	record := Record{LSN: 7, Payload: payload}
	frame := EncodeRecord(record)

	payload[0] = 'X'
	if !bytes.Equal(frame[17:17+5], []byte("hello")) {
		t.Fatalf("EncodeRecord retained a caller alias: %q", frame[17:])
	}
	frame[17] = 'Y'
	if string(payload) != "Xello" {
		t.Fatalf("EncodeRecord shared its buffer with the caller: %q", payload)
	}
	if len(frame) != FrameHeaderSize+len(payload)+ChecksumSize {
		t.Fatalf("frame size = %d, want %d", len(frame), FrameHeaderSize+len(payload)+ChecksumSize)
	}
}

func TestFrameRoundTripAndChecksum(t *testing.T) {
	record := Record{LSN: 3, Payload: bytes.Repeat([]byte{0xab}, 64)}
	frame := EncodeRecord(record)

	length, seq, ok := ParseHeader(frame[:FrameHeaderSize])
	if !ok {
		t.Fatal("ParseHeader rejected a valid header")
	}
	if length != 64 || seq != 3 {
		t.Fatalf("ParseHeader = (%d, %d), want (64, 3)", length, seq)
	}
	if !VerifyFrame(frame) {
		t.Fatal("VerifyFrame rejected a valid frame")
	}

	// A payload bit flip must invalidate the checksum without changing the
	// header, which is exactly how interior corruption is detected.
	corrupt := append([]byte(nil), frame...)
	corrupt[FrameHeaderSize] ^= 0x01
	if VerifyFrame(corrupt) {
		t.Fatal("VerifyFrame accepted a frame with a flipped payload byte")
	}
	length, seq, ok = ParseHeader(corrupt[:FrameHeaderSize])
	if !ok || length != 64 || seq != 3 {
		t.Fatalf("header changed after payload flip: (%d, %d, %v)", length, seq, ok)
	}
}

func TestParseHeaderRejectsBadMagicAndVersion(t *testing.T) {
	frame := EncodeRecord(Record{LSN: 1, Payload: []byte("x")})
	badMagic := append([]byte(nil), frame...)
	badMagic[0] ^= 0xff
	if _, _, ok := ParseHeader(badMagic[:FrameHeaderSize]); ok {
		t.Fatal("ParseHeader accepted a header with a bad magic byte")
	}

	badVersion := append([]byte(nil), frame...)
	badVersion[4] = Version + 1
	if _, _, ok := ParseHeader(badVersion[:FrameHeaderSize]); ok {
		t.Fatal("ParseHeader accepted a header with an unknown version")
	}
}

func TestFrameOverheadIsSelfConsistent(t *testing.T) {
	frame := EncodeRecord(Record{LSN: 1, Payload: nil})
	if len(frame) != FrameOverhead {
		t.Fatalf("empty record frame size = %d, want FrameOverhead %d", len(frame), FrameOverhead)
	}
	checksum := binary.BigEndian.Uint32(frame[len(frame)-ChecksumSize:])
	if checksum == 0 {
		t.Fatal("empty record frame checksum is zero")
	}
}

func TestMagicBytesReturnsAnIndependentCopy(t *testing.T) {
	first := MagicBytes()
	if first != [4]byte{'J', 'R', 'N', 'L'} {
		t.Fatalf("MagicBytes() = %v, want JRNL", first)
	}
	first[0] = 'X'
	second := MagicBytes()
	if second != [4]byte{'J', 'R', 'N', 'L'} {
		t.Fatalf("MagicBytes() after mutating the returned copy = %v, want JRNL", second)
	}
}

func TestMaxPayloadBytesIsReasonable(t *testing.T) {
	if MaxPayloadBytes < 1 {
		t.Fatal("MaxPayloadBytes must be positive")
	}
	if MaxPayloadBytes > 1<<30 {
		t.Fatal("MaxPayloadBytes is too large to bound allocations safely")
	}
}
