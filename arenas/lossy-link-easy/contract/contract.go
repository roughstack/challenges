// Package contract defines the narrow contestant-owned surface for the
// lossy-link-easy arena. Everything else — the deterministic event loop,
// virtual clock, application stream, fault injection, accounting, and result
// construction — is owned by the trusted harness.
package contract

import (
	"encoding/binary"
	"hash/crc32"
)

// PacketKind distinguishes data packets from acknowledgements on the wire.
type PacketKind uint8

const (
	// KindData carries a slice of the application stream.
	KindData PacketKind = iota + 1
	// KindAck acknowledges the last valid data sequence received.
	KindAck
)

// PacketSizeOverhead is the fixed byte cost of the packet framing: one byte of
// kind, one byte of sequence, one byte of acknowledgement, two bytes of payload
// length, and four bytes of checksum.
const PacketSizeOverhead = 1 + 1 + 1 + 2 + 4

// MaxPacketSize returns the strict maximum wire size of a single packet for a
// configured maximum payload. The harness rejects any packet that exceeds it.
func MaxPacketSize(maxPayloadBytes int) int {
	if maxPayloadBytes < 0 {
		return PacketSizeOverhead
	}
	return maxPayloadBytes + PacketSizeOverhead
}

// Packet is the unit of transfer between sender and receiver. Sequence
// numbers use a small wrapping space (one byte) so a delayed or duplicated
// packet can be told apart from a new one; Payload is empty for ACKs.
type Packet struct {
	Kind     PacketKind
	Seq      uint8
	Ack      uint8
	Payload  []byte
	Checksum uint32
}

// Clone returns a deep copy of the packet so the harness and endpoints never
// alias each other's buffers.
func (p Packet) Clone() Packet {
	return Packet{
		Kind:     p.Kind,
		Seq:      p.Seq,
		Ack:      p.Ack,
		Payload:  CloneBytes(p.Payload),
		Checksum: p.Checksum,
	}
}

// CloneBytes copies a byte slice without sharing backing storage.
func CloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

// wire encodes the packet in the canonical order used for checksumming. The
// payload length is present explicitly so that a corrupted length field is
// detectable before any allocation.
func (p Packet) wire() []byte {
	out := make([]byte, PacketSizeOverhead+len(p.Payload))
	out[0] = byte(p.Kind)
	out[1] = p.Seq
	out[2] = p.Ack
	binary.BigEndian.PutUint16(out[3:5], uint16(len(p.Payload)))
	copy(out[5:], p.Payload)
	return out
}

// Checksum computes the deterministic checksum over the packet framing and
// payload. The stored checksum field is not part of its own input.
func Checksum(p Packet) uint32 {
	body := p.wire()
	return crc32.ChecksumIEEE(body[:len(body)-4])
}

// Verify recomputes the checksum and compares it to the stored value.
func (p Packet) Verify() bool {
	return p.Checksum == Checksum(Packet{
		Kind:    p.Kind,
		Seq:     p.Seq,
		Ack:     p.Ack,
		Payload: p.Payload,
	})
}

// Timer is a deterministic deadline expressed in virtual ticks. TimerID is
// chosen by the endpoint and passed back unchanged to OnTimer.
type Timer struct {
	TimerID  uint64
	Deadline uint64
}

// Config is the immutable per-run configuration shared by sender and receiver.
type Config struct {
	// MaxPayloadBytes is the largest application payload carried in one data
	// packet. A non-positive value falls back to the default.
	MaxPayloadBytes int
	// RetransmitTicks is the retransmission timeout in virtual ticks. A
	// non-positive value falls back to the default.
	RetransmitTicks uint64
	// StreamBytes is the exact length of the application stream. It is fixed
	// by the harness before either endpoint is constructed.
	StreamBytes uint64
}

// DefaultMaxPayloadBytes is the payload limit used when the config leaves it
// unset or non-positive.
const DefaultMaxPayloadBytes = 1024

// DefaultRetransmitTicks is the retransmission timeout used when the config
// leaves it unset or non-positive.
const DefaultRetransmitTicks = 8

// MaxPayload returns the effective configured payload limit.
func (c Config) MaxPayload() int {
	if c.MaxPayloadBytes <= 0 {
		return DefaultMaxPayloadBytes
	}
	return c.MaxPayloadBytes
}

// RTO returns the effective retransmission timeout in virtual ticks.
func (c Config) RTO() uint64 {
	if c.RetransmitTicks == 0 {
		return DefaultRetransmitTicks
	}
	return c.RetransmitTicks
}

// Endpoint is the complete contestant-owned surface. NewSender and NewReceiver
// both return an Endpoint; the harness drives whichever methods are meaningful
// for each role.
//
//   - OnAppData hands the sender another chunk of the application stream; eof
//     is true when that chunk is the final one.
//   - OnPacket delivers a packet arriving from the peer and returns any packets
//     the endpoint wants to send in response.
//   - OnTimer fires one of the timers previously reported by NextTimers.
//   - ReadDelivered returns and consumes bytes the receiver has committed to
//     the delivered, in-order stream.
//   - NextTimers reports the endpoint's currently pending timers.
//   - StateBytes reports the endpoint's current protocol-state size.
//   - Done reports whether the endpoint has finished its side of the transfer.
type Endpoint interface {
	OnAppData(data []byte, eof bool) []Packet
	OnPacket(pkt Packet, nowTick uint64) []Packet
	OnTimer(timerID uint64, nowTick uint64) []Packet
	ReadDelivered(max int) []byte
	NextTimers() []Timer
	StateBytes() uint64
	Done() bool
}

// Factory constructs a fresh sender/receiver pair for one isolated run.
type Factory struct {
	NewSender   func(cfg Config) Endpoint
	NewReceiver func(cfg Config) Endpoint
}
