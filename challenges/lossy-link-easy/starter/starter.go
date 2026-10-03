// Package starter contains a deliberately simple, correct stop-and-wait
// implementation. It never holds more than one unacknowledged data packet,
// retransmits on a fixed timeout, and acknowledges the last valid sequence
// while discarding duplicates. It is intentionally conservative: no
// pipelining, no adaptive timeout, and a single fixed timer.
package starter

import "github.com/roughstack/challenges/challenges/lossy-link-easy/contract"

// Factory constructs the public sender/receiver pair from the contestant
// surface. It is referenced by the harness and smoke command only; the
// contestant replaces the bodies behind these constructors.
var Factory = contract.Factory{
	NewSender:   NewSender,
	NewReceiver: NewReceiver,
}

const dataTimerID uint64 = 1

// NewSender returns a stop-and-wait sender bound to one fixed-size stream.
func NewSender(cfg contract.Config) contract.Endpoint {
	return &sender{cfg: cfg}
}

type sender struct {
	cfg contract.Config

	stream []byte // owned copy of all application data received so far
	pos    uint64 // acknowledged byte offset
	eof    bool   // final chunk has been received

	seq      uint8 // sequence number of the packet currently in flight
	inFlight *contract.Packet
	lastSend uint64 // virtual tick of the most recent transmission
	done     bool
}

func (s *sender) OnAppData(data []byte, eof bool) []contract.Packet {
	s.stream = append(s.stream, contract.CloneBytes(data)...)
	if eof {
		s.eof = true
	}

	if s.inFlight == nil && s.pos < uint64(len(s.stream)) {
		pkt := s.nextPacket()
		s.send(pkt, 0)
		return []contract.Packet{pkt}
	}
	return nil
}

func (s *sender) OnPacket(pkt contract.Packet, nowTick uint64) []contract.Packet {
	if pkt.Kind != contract.KindAck || !pkt.Verify() || len(pkt.Payload) != 0 {
		return nil
	}
	if s.inFlight == nil || pkt.Ack != s.seq {
		return nil
	}

	s.pos += uint64(len(s.inFlight.Payload))
	s.seq++
	s.inFlight = nil

	if s.pos >= uint64(len(s.stream)) && s.eof {
		s.done = true
		return nil
	}

	next := s.nextPacket()
	s.send(next, nowTick)
	return []contract.Packet{next}
}

func (s *sender) OnTimer(timerID uint64, nowTick uint64) []contract.Packet {
	if timerID != dataTimerID || s.inFlight == nil {
		return nil
	}
	// Ignore a timer that was armed for an earlier transmission. The sender
	// may have already advanced on a late acknowledgement, so this timer is
	// stale and must not retransmit the current in-flight packet early.
	if nowTick < s.lastSend+s.cfg.RTO() {
		return nil
	}

	pkt := s.inFlight.Clone()
	s.lastSend = nowTick
	return []contract.Packet{pkt}
}

func (s *sender) ReadDelivered(_ int) []byte {
	return nil
}

func (s *sender) NextTimers() []contract.Timer {
	if s.inFlight == nil {
		return nil
	}
	return []contract.Timer{{
		TimerID:  dataTimerID,
		Deadline: s.lastSend + s.cfg.RTO(),
	}}
}

func (s *sender) StateBytes() uint64 {
	state := uint64(len(s.stream))
	if s.inFlight != nil {
		state += uint64(len(s.inFlight.Payload))
	}
	return state
}

func (s *sender) Done() bool {
	return s.done || (s.eof && s.pos >= uint64(len(s.stream)) && s.inFlight == nil)
}

// nextPacket builds the data packet for the next unsent chunk and returns it
// with a checksum already attached.
func (s *sender) nextPacket() contract.Packet {
	remaining := uint64(len(s.stream)) - s.pos
	maxPayload := uint64(s.cfg.MaxPayload())
	if remaining > maxPayload {
		remaining = maxPayload
	}

	payload := contract.CloneBytes(s.stream[s.pos : s.pos+remaining])
	pkt := contract.Packet{
		Kind:    contract.KindData,
		Seq:     s.seq,
		Payload: payload,
	}
	pkt.Checksum = contract.Checksum(pkt)
	return pkt
}

func (s *sender) send(pkt contract.Packet, nowTick uint64) {
	owned := pkt.Clone()
	s.inFlight = &owned
	s.lastSend = nowTick
}

// NewReceiver returns a receiver that delivers the stream exactly once and in
// order, discarding duplicates and rejecting malformed packets.
func NewReceiver(cfg contract.Config) contract.Endpoint {
	return &receiver{cfg: cfg}
}

type receiver struct {
	cfg contract.Config

	delivered []byte // committed, in-order bytes awaiting ReadDelivered
	read      uint64 // offset of bytes already consumed by ReadDelivered
	expected  uint8  // sequence number of the next valid data packet
}

func (r *receiver) OnAppData(_ []byte, _ bool) []contract.Packet {
	return nil
}

func (r *receiver) OnPacket(pkt contract.Packet, _ uint64) []contract.Packet {
	if pkt.Kind != contract.KindData {
		return nil
	}
	if len(pkt.Payload) > r.cfg.MaxPayload() || !pkt.Verify() {
		return nil
	}

	if pkt.Seq != r.expected {
		// Duplicate or stale: acknowledge the last valid sequence.
		ack := contract.Packet{
			Kind: contract.KindAck,
			Ack:  r.expected - 1,
		}
		ack.Checksum = contract.Checksum(ack)
		return []contract.Packet{ack}
	}

	r.delivered = append(r.delivered, contract.CloneBytes(pkt.Payload)...)
	r.expected++
	ack := contract.Packet{
		Kind: contract.KindAck,
		Ack:  pkt.Seq,
	}
	ack.Checksum = contract.Checksum(ack)
	return []contract.Packet{ack}
}

func (r *receiver) OnTimer(_ uint64, _ uint64) []contract.Packet {
	return nil
}

func (r *receiver) ReadDelivered(max int) []byte {
	if max <= 0 {
		return nil
	}
	if r.read >= uint64(len(r.delivered)) {
		return nil
	}

	end := r.read + uint64(max)
	if end > uint64(len(r.delivered)) {
		end = uint64(len(r.delivered))
	}
	out := r.delivered[r.read:end]
	r.read = end
	return out
}

func (r *receiver) NextTimers() []contract.Timer {
	return nil
}

func (r *receiver) StateBytes() uint64 {
	return uint64(len(r.delivered)) - r.read
}

func (r *receiver) Done() bool {
	return uint64(len(r.delivered)) >= r.cfg.StreamBytes
}
