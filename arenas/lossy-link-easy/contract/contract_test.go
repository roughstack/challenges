package contract

import (
	"bytes"
	"testing"
)

func TestChecksumDetectsPayloadCorruption(t *testing.T) {
	pkt := Packet{Kind: KindData, Seq: 0, Payload: []byte("hello")}
	pkt.Checksum = Checksum(pkt)
	if !pkt.Verify() {
		t.Fatal("valid packet failed checksum verification")
	}

	corrupt := pkt.Clone()
	corrupt.Payload[0] ^= 0x80
	if corrupt.Verify() {
		t.Fatal("corrupted payload still passed checksum verification")
	}
}

func TestChecksumDetectsHeaderCorruption(t *testing.T) {
	pkt := Packet{Kind: KindData, Seq: 1, Ack: 0, Payload: []byte("hello")}
	pkt.Checksum = Checksum(pkt)

	corrupt := pkt.Clone()
	corrupt.Seq ^= 0x01
	if corrupt.Verify() {
		t.Fatal("corrupted sequence field still passed checksum verification")
	}
}

func TestMaxPacketSizeAndCloneIsolation(t *testing.T) {
	if got, want := MaxPacketSize(128), 128+PacketSizeOverhead; got != want {
		t.Fatalf("MaxPacketSize(128) = %d, want %d", got, want)
	}
	if got := MaxPacketSize(-1); got != PacketSizeOverhead {
		t.Fatalf("MaxPacketSize(-1) = %d, want %d", got, PacketSizeOverhead)
	}

	original := Packet{Kind: KindData, Payload: []byte("payload")}
	cloned := original.Clone()
	cloned.Payload[0] = 'X'
	if bytes.Equal(original.Payload, cloned.Payload) {
		t.Fatal("Clone shared backing storage with the original")
	}
}

func TestConfigEffectiveLimits(t *testing.T) {
	cfg := Config{MaxPayloadBytes: 0, RetransmitTicks: 0}
	if cfg.MaxPayload() != DefaultMaxPayloadBytes {
		t.Fatalf("default max payload = %d", cfg.MaxPayload())
	}
	if cfg.RTO() != DefaultRetransmitTicks {
		t.Fatalf("default RTO = %d", cfg.RTO())
	}

	custom := Config{MaxPayloadBytes: 7, RetransmitTicks: 3}
	if custom.MaxPayload() != 7 || custom.RTO() != 3 {
		t.Fatal("custom limits were not honored")
	}
}
