// Package starter is the learner-facing scaffold for a reliable byte stream.
package starter

import "github.com/roughstack/challenges/challenges/lossy-link-easy/contract"

type sender struct {
	cfg contract.Config
	// TODO: track bytes in flight, acknowledgements, and timers.
}

type receiver struct {
	cfg contract.Config
	// TODO: track the next expected sequence and delivered bytes.
}

func NewSender(cfg contract.Config) contract.Endpoint   { return &sender{cfg: cfg} }
func NewReceiver(cfg contract.Config) contract.Endpoint { return &receiver{cfg: cfg} }

func (s *sender) OnAppData(data []byte, eof bool) []contract.Packet {
	panic("TODO: packetize and send")
}
func (s *sender) OnPacket(pkt contract.Packet, nowTick uint64) []contract.Packet {
	panic("TODO: process valid acknowledgements")
}
func (s *sender) OnTimer(timerID, nowTick uint64) []contract.Packet {
	panic("TODO: retransmit the outstanding packet")
}
func (s *sender) ReadDelivered(max int) []byte { panic("TODO: sender delivers no application bytes") }
func (s *sender) NextTimers() []contract.Timer { panic("TODO: expose active timers") }
func (s *sender) StateBytes() uint64           { panic("TODO: report buffered state") }
func (s *sender) Done() bool                   { panic("TODO: report completion only after acknowledgement") }

func (r *receiver) OnAppData(data []byte, eof bool) []contract.Packet {
	panic("TODO: receiver accepts no outgoing app data")
}
func (r *receiver) OnPacket(pkt contract.Packet, nowTick uint64) []contract.Packet {
	panic("TODO: validate, deliver in order, and acknowledge")
}
func (r *receiver) OnTimer(timerID, nowTick uint64) []contract.Packet {
	panic("TODO: receiver timer behavior")
}
func (r *receiver) ReadDelivered(max int) []byte {
	panic("TODO: drain only the requested delivered bytes")
}
func (r *receiver) NextTimers() []contract.Timer { panic("TODO: receiver timer behavior") }
func (r *receiver) StateBytes() uint64           { panic("TODO: report buffered state") }
func (r *receiver) Done() bool                   { panic("TODO: report end-of-stream delivery") }
