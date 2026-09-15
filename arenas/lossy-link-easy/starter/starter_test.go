package starter

import (
	"bytes"
	"testing"

	"github.com/bytearena/arenas/arenas/lossy-link-easy/contract"
)

func testConfig(streamBytes uint64) contract.Config {
	return contract.Config{
		MaxPayloadBytes: 4,
		RetransmitTicks: 8,
		StreamBytes:     streamBytes,
	}
}

func TestReceiverDeliversExactlyOnceInOrderAndAcksDuplicates(t *testing.T) {
	receiver := NewReceiver(testConfig(8))

	first := contract.Packet{Kind: contract.KindData, Seq: 0, Payload: []byte("abcd")}
	first.Checksum = contract.Checksum(first)
	if acks := receiver.OnPacket(first, 1); len(acks) != 1 || acks[0].Ack != 0 {
		t.Fatalf("unexpected ack for first packet: %+v", acks)
	}

	// A duplicate of the same sequence must be acknowledged, not delivered.
	if acks := receiver.OnPacket(first.Clone(), 2); len(acks) != 1 || acks[0].Ack != 0 {
		t.Fatalf("duplicate was not re-acknowledged: %+v", acks)
	}

	second := contract.Packet{Kind: contract.KindData, Seq: 1, Payload: []byte("efgh")}
	second.Checksum = contract.Checksum(second)
	if acks := receiver.OnPacket(second, 3); len(acks) != 1 || acks[0].Ack != 1 {
		t.Fatalf("unexpected ack for second packet: %+v", acks)
	}

	if got := receiver.ReadDelivered(1024); !bytes.Equal(got, []byte("abcdefgh")) {
		t.Fatalf("delivered bytes = %q, want abcdefgh", got)
	}
	if !receiver.Done() {
		t.Fatal("receiver did not report completion")
	}
}

func TestReceiverRejectsCorruptionAndOversizedPayloads(t *testing.T) {
	receiver := NewReceiver(testConfig(4))

	corrupt := contract.Packet{Kind: contract.KindData, Seq: 0, Payload: []byte("abcd")}
	corrupt.Checksum = contract.Checksum(corrupt)
	corrupt.Payload[0] ^= 0x80
	if acks := receiver.OnPacket(corrupt, 1); len(acks) != 0 {
		t.Fatalf("corrupt packet produced acks: %+v", acks)
	}

	oversized := contract.Packet{Kind: contract.KindData, Seq: 0, Payload: []byte("abcdefghij")}
	oversized.Checksum = contract.Checksum(oversized)
	if acks := receiver.OnPacket(oversized, 2); len(acks) != 0 {
		t.Fatalf("oversized packet produced acks: %+v", acks)
	}

	if got := receiver.ReadDelivered(1024); len(got) != 0 {
		t.Fatalf("receiver delivered rejected bytes: %q", got)
	}
}

func TestSenderStopAndWaitAndRetransmit(t *testing.T) {
	stream := []byte("abcdefgh")
	sender := NewSender(testConfig(uint64(len(stream))))

	sent := sender.OnAppData(stream, true)
	if len(sent) != 1 || !bytes.Equal(sent[0].Payload, []byte("abcd")) {
		t.Fatalf("unexpected first packet: %+v", sent)
	}

	// A timeout must retransmit the same in-flight packet.
	timers := sender.NextTimers()
	if len(timers) != 1 {
		t.Fatalf("expected one pending timer, got %d", len(timers))
	}
	retransmit := sender.OnTimer(timers[0].TimerID, timers[0].Deadline)
	if len(retransmit) != 1 || !bytes.Equal(retransmit[0].Payload, sent[0].Payload) {
		t.Fatalf("retransmit did not repeat in-flight packet: %+v", retransmit)
	}

	// A stale acknowledgement with a valid checksum must be ignored.
	staleAck := contract.Packet{Kind: contract.KindAck, Ack: 1}
	staleAck.Checksum = contract.Checksum(staleAck)
	if out := sender.OnPacket(staleAck, 9); len(out) != 0 {
		t.Fatalf("stale ack advanced the sender: %+v", out)
	}

	// The matching acknowledgement advances to the second packet.
	ack := contract.Packet{Kind: contract.KindAck, Ack: 0}
	ack.Checksum = contract.Checksum(ack)
	second := sender.OnPacket(ack, 10)
	if len(second) != 1 || !bytes.Equal(second[0].Payload, []byte("efgh")) {
		t.Fatalf("unexpected second packet: %+v", second)
	}

	ack2 := contract.Packet{Kind: contract.KindAck, Ack: 1}
	ack2.Checksum = contract.Checksum(ack2)
	if out := sender.OnPacket(ack2, 11); len(out) != 0 {
		t.Fatalf("final ack produced output: %+v", out)
	}
	if !sender.Done() {
		t.Fatal("sender did not report completion")
	}
	if len(sender.NextTimers()) != 0 {
		t.Fatal("sender still had pending timers after completion")
	}
}

func TestSenderRejectsChecksumInvalidAck(t *testing.T) {
	var sender contract.Endpoint = NewSender(testConfig(8))
	sender.OnAppData([]byte("abcdefgh"), true)

	invalidAck := contract.Packet{Kind: contract.KindAck, Ack: 0}
	if out := sender.OnPacket(invalidAck, 1); len(out) != 0 {
		t.Fatalf("checksum-invalid ack advanced the sender: %+v", out)
	}
	if sender.Done() {
		t.Fatal("checksum-invalid ack completed the sender")
	}
}

func TestSenderRejectsAckWithPayload(t *testing.T) {
	var sender contract.Endpoint = NewSender(testConfig(8))
	sent := sender.OnAppData([]byte("abcdefgh"), true)
	if len(sent) != 1 {
		t.Fatalf("unexpected first packet: %+v", sent)
	}

	ackWithPayload := contract.Packet{Kind: contract.KindAck, Ack: 0, Payload: []byte("x")}
	ackWithPayload.Checksum = contract.Checksum(ackWithPayload)
	if out := sender.OnPacket(ackWithPayload, 1); len(out) != 0 {
		t.Fatalf("payload-carrying ack advanced the sender: %+v", out)
	}
	if sender.Done() {
		t.Fatal("payload-carrying ack completed the sender")
	}
	if timers := sender.NextTimers(); len(timers) != 1 {
		t.Fatalf("sender did not keep its in-flight timer: %+v", timers)
	}
}

func TestSenderIgnoresStaleTimer(t *testing.T) {
	var sender contract.Endpoint = NewSender(testConfig(8))
	sender.OnAppData([]byte("abcdefgh"), true)

	// The first packet is in flight with a timer armed for its deadline.
	ack0 := contract.Packet{Kind: contract.KindAck, Ack: 0}
	ack0.Checksum = contract.Checksum(ack0)
	second := sender.OnPacket(ack0, 0)
	if len(second) != 1 || second[0].Seq != 1 {
		t.Fatalf("unexpected second packet: %+v", second)
	}

	// A timer armed for the first packet fires after the sender advanced; it
	// must not retransmit the second packet early.
	if out := sender.OnTimer(dataTimerID, 1); len(out) != 0 {
		t.Fatalf("stale timer retransmitted the in-flight packet: %+v", out)
	}
}

func TestSenderHandlesEmptyStream(t *testing.T) {
	sender := NewSender(testConfig(0))
	if out := sender.OnAppData(nil, true); len(out) != 0 {
		t.Fatalf("empty stream produced packets: %+v", out)
	}
	if !sender.Done() {
		t.Fatal("empty-stream sender did not report completion")
	}
}

func TestSenderNeverExceedsMaxPacketSize(t *testing.T) {
	stream := bytes.Repeat([]byte("x"), 100)
	sender := NewSender(testConfig(uint64(len(stream))))

	sent := sender.OnAppData(stream, true)
	maxPayload := testConfig(100).MaxPayload()
	if len(sent[0].Payload) > maxPayload {
		t.Fatalf("first packet payload %d exceeds max %d", len(sent[0].Payload), maxPayload)
	}
}
