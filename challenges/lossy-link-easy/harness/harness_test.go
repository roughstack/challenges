package harness

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/roughstack/challenges/challenges/lossy-link-easy/contract"
	"github.com/roughstack/challenges/challenges/lossy-link-easy/starter"
	"github.com/roughstack/challenges/challenges/lossy-link-easy/workload"
)

func run(seed uint64, fault FaultProfile, streamBytes uint64) Result {
	config := PublicConfig{
		StreamBytes:     streamBytes,
		MaxPayloadBytes: 64,
		RetransmitTicks: 8,
		Fault:           fault,
	}
	return Evaluate(seed, config, starter.Factory)
}

func TestCleanTransferDeliversExactStream(t *testing.T) {
	const streamBytes uint64 = 512
	result := run(1, FaultClean, streamBytes)
	if result.Verdict != "pass" || len(result.Violations) != 0 {
		t.Fatalf("clean transfer failed: %+v", result.Violations)
	}
	if result.Metrics.Retransmissions != 0 {
		t.Fatalf("clean transfer retransmitted %d packets", result.Metrics.Retransmissions)
	}
	if result.Metrics.CompletionTicks == 0 {
		t.Fatal("clean transfer reported zero completion ticks")
	}
	if result.Metrics.TransmittedBytes != streamBytes {
		t.Fatalf("transmitted bytes = %d, want %d", result.Metrics.TransmittedBytes, streamBytes)
	}
}

func TestLostFirstDataPacketRecoversViaRetransmission(t *testing.T) {
	result := run(2, FaultDropFirstData, 512)
	if result.Verdict != "pass" {
		t.Fatalf("lost-data transfer failed: %+v", result.Violations)
	}
	if result.Metrics.Retransmissions < 1 {
		t.Fatalf("lost data did not cause a retransmission: %+v", result.Metrics)
	}
}

func TestLostFirstAckDoesNotDuplicateDelivery(t *testing.T) {
	result := run(3, FaultDropFirstAck, 512)
	if result.Verdict != "pass" {
		t.Fatalf("lost-ack transfer failed: %+v", result.Violations)
	}
	if result.Metrics.Retransmissions < 1 {
		t.Fatalf("lost ack did not cause a retransmission: %+v", result.Metrics)
	}
}

func TestDuplicatePacketsStillDeliverOnce(t *testing.T) {
	result := run(4, FaultDuplicateAll, 512)
	if result.Verdict != "pass" {
		t.Fatalf("duplicate transfer failed: %+v", result.Violations)
	}
	if result.Metrics.Retransmissions != 0 {
		t.Fatalf("link duplication was counted as sender retransmission: %+v", result.Metrics)
	}
}

func TestCorruptPacketRejectedAndRecovered(t *testing.T) {
	result := run(5, FaultCorruptData, 512)
	if result.Verdict != "pass" {
		t.Fatalf("corrupt transfer failed: %+v", result.Violations)
	}
	if result.Metrics.Retransmissions < 1 {
		t.Fatalf("corruption did not cause a retransmission: %+v", result.Metrics)
	}
}

func TestDelayedStaleAckIsHandled(t *testing.T) {
	result := run(6, FaultDelayFirstAck, 512)
	if result.Verdict != "pass" {
		t.Fatalf("delayed-ack transfer failed: %+v", result.Violations)
	}
}

func TestEmptyAndBoundarySizedStreams(t *testing.T) {
	for _, size := range []uint64{0, 1, 64, 65, 512} {
		result := run(7, FaultClean, size)
		if result.Verdict != "pass" {
			t.Fatalf("stream size %d failed: %+v", size, result.Violations)
		}
		if size == 0 && result.Metrics.CompletionTicks != 0 {
			t.Fatalf("empty stream reported %d completion ticks", result.Metrics.CompletionTicks)
		}
	}
}

func TestDeterministicReplayMatchesExactly(t *testing.T) {
	config := PublicConfig{
		StreamBytes:     4096,
		MaxPayloadBytes: 256,
		RetransmitTicks: 16,
		Fault:           FaultRandom,
	}
	first := Evaluate(^uint64(0), config, starter.Factory)
	second := Evaluate(^uint64(0), config, starter.Factory)
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("same seed produced different results:\n%+v\n%+v", first, second)
	}
	if first.Verdict != "pass" {
		t.Fatalf("random fault transfer failed: %+v", first.Violations)
	}
}

func TestResultEnvelopeContainsEveryPublicMetric(t *testing.T) {
	result := run(8, FaultRandom, 1024)
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}

	var envelope map[string]any
	if err := json.Unmarshal(encoded, &envelope); err != nil {
		t.Fatal(err)
	}
	metrics, ok := envelope["metrics"].(map[string]any)
	if !ok {
		t.Fatal("result metrics are not an object")
	}
	for _, metric := range []string{"completion_ticks", "transmitted_bytes", "packet_count", "retransmissions", "state_bytes"} {
		if _, ok := metrics[metric]; !ok {
			t.Fatalf("result is missing metric %q", metric)
		}
	}
	if envelope["seed"] != "8" {
		t.Fatalf("seed not preserved as a decimal string: %v", envelope["seed"])
	}
}

// oversizedFactory emits a data packet that violates the configured maximum so
// the harness's packet-size enforcement is exercised end to end.
func oversizedFactory() contract.Factory {
	return contract.Factory{
		NewSender: func(cfg contract.Config) contract.Endpoint {
			return &oversizedSender{}
		},
		NewReceiver: func(cfg contract.Config) contract.Endpoint {
			return starter.NewReceiver(cfg)
		},
	}
}

type oversizedSender struct {
	sent bool
}

func (s *oversizedSender) OnAppData(data []byte, eof bool) []contract.Packet {
	if s.sent {
		return nil
	}
	s.sent = true
	pkt := contract.Packet{Kind: contract.KindData, Seq: 0, Payload: bytes.Repeat([]byte("x"), 4096)}
	pkt.Checksum = contract.Checksum(pkt)
	return []contract.Packet{pkt}
}

func (s *oversizedSender) OnPacket(contract.Packet, uint64) []contract.Packet { return nil }
func (s *oversizedSender) OnTimer(uint64, uint64) []contract.Packet           { return nil }
func (s *oversizedSender) ReadDelivered(int) []byte                           { return nil }
func (s *oversizedSender) NextTimers() []contract.Timer                       { return nil }
func (s *oversizedSender) StateBytes() uint64                                 { return 0 }
func (s *oversizedSender) Done() bool                                         { return false }

func TestPacketSizeLimitIsEnforced(t *testing.T) {
	config := PublicConfig{
		StreamBytes:     4096,
		MaxPayloadBytes: 64,
		RetransmitTicks: 8,
		Fault:           FaultClean,
	}
	result := Evaluate(9, config, oversizedFactory())
	if result.Verdict != "fail" {
		t.Fatalf("oversized endpoint was not failed: %+v", result)
	}
	found := false
	for _, violation := range result.Violations {
		if violation == "endpoint emitted an oversized packet" {
			found = true
		}
	}
	if !found {
		t.Fatalf("missing oversized-packet violation: %+v", result.Violations)
	}
}

func TestTransferredBytesMatchesSourceAcrossFaults(t *testing.T) {
	// Transmitted bytes always counts every data byte put on the wire, so it
	// must never be less than the source length for a completed transfer.
	for _, fault := range []FaultProfile{FaultClean, FaultDropFirstData, FaultDropFirstAck, FaultDuplicateAll, FaultCorruptData, FaultDelayFirstAck} {
		result := run(10, fault, 256)
		if result.Verdict != "pass" {
			t.Fatalf("fault %s failed: %+v", fault, result.Violations)
		}
		if result.Metrics.TransmittedBytes < 256 {
			t.Fatalf("fault %s transmitted fewer bytes than the source", fault)
		}
	}
}

func TestWorkloadGenerationMatchesHarnessStream(t *testing.T) {
	stream := workload.Generate(11, 1234)
	if len(stream) != 1234 {
		t.Fatalf("stream length = %d", len(stream))
	}
}

func TestSequenceWrapDeliversCorrectly(t *testing.T) {
	// A one-byte payload forces more than 256 data packets, exercising the
	// sequence-number wrap boundary.
	config := PublicConfig{
		StreamBytes:     300,
		MaxPayloadBytes: 1,
		RetransmitTicks: 8,
		Fault:           FaultClean,
	}
	result := Evaluate(12, config, starter.Factory)
	if result.Verdict != "pass" {
		t.Fatalf("sequence-wrap transfer failed: %+v", result.Violations)
	}
	if result.Metrics.PacketCount < 300 {
		t.Fatalf("expected at least 300 packets, got %d", result.Metrics.PacketCount)
	}
}

func hasViolation(result Result, want string) bool {
	for _, violation := range result.Violations {
		if violation == want {
			return true
		}
	}
	return false
}

type deadlineZeroEndpoint struct{}

func (deadlineZeroEndpoint) OnAppData([]byte, bool) []contract.Packet { return nil }
func (deadlineZeroEndpoint) OnPacket(contract.Packet, uint64) []contract.Packet {
	return nil
}
func (deadlineZeroEndpoint) OnTimer(uint64, uint64) []contract.Packet { return nil }
func (deadlineZeroEndpoint) ReadDelivered(int) []byte                 { return nil }
func (deadlineZeroEndpoint) NextTimers() []contract.Timer {
	return []contract.Timer{{TimerID: 1, Deadline: 0}}
}
func (deadlineZeroEndpoint) StateBytes() uint64 { return 0 }
func (deadlineZeroEndpoint) Done() bool         { return false }

func TestTimerDeadlineMustBeFutureAndTerminates(t *testing.T) {
	config := PublicConfig{
		StreamBytes:     0,
		MaxPayloadBytes: 64,
		RetransmitTicks: 8,
		Fault:           FaultClean,
	}
	factory := contract.Factory{
		NewSender:   func(contract.Config) contract.Endpoint { return deadlineZeroEndpoint{} },
		NewReceiver: func(contract.Config) contract.Endpoint { return deadlineZeroEndpoint{} },
	}
	result := Evaluate(15, config, factory)
	if result.Verdict != "fail" {
		t.Fatalf("deadline-zero endpoint was not failed: %+v", result)
	}
	if !hasViolation(result, "timer deadline is not in the future") {
		t.Fatalf("missing future-deadline violation: %+v", result.Violations)
	}
	if result.Metrics.CompletionTicks != 64 {
		t.Fatalf("virtual clock did not advance monotonically to the stall limit: %+v", result.Metrics)
	}
}

type panickingReceiver struct{}

func (panickingReceiver) OnAppData([]byte, bool) []contract.Packet { return nil }
func (panickingReceiver) OnPacket(contract.Packet, uint64) []contract.Packet {
	panic("contestant receiver panic")
}
func (panickingReceiver) OnTimer(uint64, uint64) []contract.Packet { return nil }
func (panickingReceiver) ReadDelivered(int) []byte                 { return nil }
func (panickingReceiver) NextTimers() []contract.Timer             { return nil }
func (panickingReceiver) StateBytes() uint64                       { return 0 }
func (panickingReceiver) Done() bool                               { return false }

func TestPanickingEndpointYieldsViolation(t *testing.T) {
	config := PublicConfig{
		StreamBytes:     64,
		MaxPayloadBytes: 64,
		RetransmitTicks: 8,
		Fault:           FaultClean,
	}
	factory := contract.Factory{
		NewSender: starter.NewSender,
		NewReceiver: func(contract.Config) contract.Endpoint {
			return panickingReceiver{}
		},
	}
	result := Evaluate(16, config, factory)
	if result.Verdict != "fail" {
		t.Fatalf("panicking endpoint was not failed: %+v", result)
	}
	if !hasViolation(result, "contestant endpoint panicked") {
		t.Fatalf("missing panic violation: %+v", result.Violations)
	}
}

type mutatingSender struct {
	sent  bool
	acked bool
}

func (s *mutatingSender) OnAppData(data []byte, _ bool) []contract.Packet {
	if s.sent {
		return nil
	}
	s.sent = true
	for index := range data {
		data[index] ^= 0xff
	}
	pkt := contract.Packet{Kind: contract.KindData, Seq: 0, Payload: data}
	pkt.Checksum = contract.Checksum(pkt)
	return []contract.Packet{pkt}
}

func (s *mutatingSender) OnPacket(pkt contract.Packet, _ uint64) []contract.Packet {
	if pkt.Kind == contract.KindAck && pkt.Verify() && len(pkt.Payload) == 0 && pkt.Ack == 0 {
		s.acked = true
	}
	return nil
}

func (s *mutatingSender) OnTimer(uint64, uint64) []contract.Packet { return nil }
func (s *mutatingSender) ReadDelivered(int) []byte                 { return nil }
func (s *mutatingSender) NextTimers() []contract.Timer             { return nil }
func (s *mutatingSender) StateBytes() uint64                       { return 0 }
func (s *mutatingSender) Done() bool                               { return s.acked }

func TestSenderMutationCannotCorruptSourceComparison(t *testing.T) {
	config := PublicConfig{
		StreamBytes:     64,
		MaxPayloadBytes: 64,
		RetransmitTicks: 8,
		Fault:           FaultClean,
	}
	factory := contract.Factory{
		NewSender:   func(contract.Config) contract.Endpoint { return &mutatingSender{} },
		NewReceiver: starter.NewReceiver,
	}
	result := Evaluate(17, config, factory)
	if result.Verdict != "fail" {
		t.Fatalf("mutating sender was not failed: %+v", result)
	}
	if !hasViolation(result, "delivered stream does not match source") {
		t.Fatalf("missing delivered-stream violation: %+v", result.Violations)
	}
}

type scriptedTimerEndpoint struct {
	timers []contract.Timer
	done   bool
}

func (e *scriptedTimerEndpoint) OnAppData([]byte, bool) []contract.Packet { return nil }
func (e *scriptedTimerEndpoint) OnPacket(contract.Packet, uint64) []contract.Packet {
	return nil
}
func (e *scriptedTimerEndpoint) OnTimer(uint64, uint64) []contract.Packet { return nil }
func (e *scriptedTimerEndpoint) ReadDelivered(int) []byte                 { return nil }
func (e *scriptedTimerEndpoint) NextTimers() []contract.Timer             { return e.timers }
func (e *scriptedTimerEndpoint) StateBytes() uint64                       { return 0 }
func (e *scriptedTimerEndpoint) Done() bool                               { return e.done }

func newTimerTestLink(sender, receiver contract.Endpoint) *link {
	return newLink(19, FaultClean, contract.Factory{
		NewSender:   func(contract.Config) contract.Endpoint { return sender },
		NewReceiver: func(contract.Config) contract.Endpoint { return receiver },
	}, contract.Config{
		MaxPayloadBytes: 64,
		RetransmitTicks: 8,
		StreamBytes:     0,
	}, nil)
}

func TestTimerRearmReplacesPendingEvent(t *testing.T) {
	sender := &scriptedTimerEndpoint{timers: []contract.Timer{{TimerID: 1, Deadline: 10}}}
	receiver := &scriptedTimerEndpoint{done: true}
	l := newTimerTestLink(sender, receiver)

	l.scheduleTimers()
	if len(l.pending) != 1 || l.pending[0].tick != 10 {
		t.Fatalf("initial timer not scheduled correctly: %+v", l.pending)
	}

	sender.timers = []contract.Timer{{TimerID: 1, Deadline: 20}}
	l.tick = 1
	l.scheduleTimers()
	if len(l.pending) != 1 || l.pending[0].tick != 20 {
		t.Fatalf("re-armed timer did not replace the pending event: %+v", l.pending)
	}
}

func TestTimerDuplicateIDsFirstWins(t *testing.T) {
	sender := &scriptedTimerEndpoint{timers: []contract.Timer{
		{TimerID: 1, Deadline: 30},
		{TimerID: 1, Deadline: 40},
	}}
	receiver := &scriptedTimerEndpoint{done: true}
	l := newTimerTestLink(sender, receiver)

	l.scheduleTimers()
	if len(l.pending) != 1 || l.pending[0].tick != 30 {
		t.Fatalf("duplicate TimerID did not resolve first-wins: %+v", l.pending)
	}
}

func TestConfigRejectsPayloadBoundAboveWireEncoding(t *testing.T) {
	called := false
	factory := contract.Factory{
		NewSender: func(cfg contract.Config) contract.Endpoint {
			called = true
			return starter.NewSender(cfg)
		},
		NewReceiver: func(cfg contract.Config) contract.Endpoint {
			called = true
			return starter.NewReceiver(cfg)
		},
	}
	config := PublicConfig{
		StreamBytes:     64,
		MaxPayloadBytes: 65536,
		RetransmitTicks: 8,
		Fault:           FaultClean,
	}
	result := Evaluate(18, config, factory)
	if called {
		t.Fatal("contestant factory ran before payload-bound validation")
	}
	if result.Verdict != "fail" {
		t.Fatalf("oversized payload bound was not failed: %+v", result)
	}
	if !hasViolation(result, "invalid public config: payload bound exceeds wire encoding") {
		t.Fatalf("missing payload-bound violation: %+v", result.Violations)
	}
}
