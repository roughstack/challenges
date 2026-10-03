// Package harness owns public execution, the deterministic event loop, the
// virtual clock, fault injection, accounting, and result construction. The
// contestant surface is the contract.Endpoint pair only.
package harness

import (
	"bytes"
	"sort"
	"strconv"

	"github.com/roughstack/challenges/challenges/lossy-link-easy/contract"
	"github.com/roughstack/challenges/challenges/lossy-link-easy/workload"
)

const (
	ArenaID      = "lossy-link-easy"
	ArenaVersion = "1.0.0"
	WorkloadID   = "public-smoke-v1"

	// maxTicks bounds the virtual event loop. Reaching it without completing
	// the transfer is a liveness failure.
	maxTicks uint64 = 1_000_000

	// delayTicks is the extra virtual-clock delay applied by a delay fault on
	// top of the fixed one-tick per-hop propagation.
	delayTicks uint64 = 10

	// propagationTicks is the fixed per-hop traversal cost of the simulated
	// link. It keeps the virtual clock meaningful so a delay cannot reorder
	// acknowledgements across multiple stop-and-wait rounds.
	propagationTicks uint64 = 1
)

// FaultProfile selects a deterministic public fault schedule. The random
// profile is the only one that depends on the seed; the named profiles are
// single-event fixtures used by the public tests.
type FaultProfile string

const (
	FaultClean         FaultProfile = "clean"
	FaultDropFirstData FaultProfile = "drop-first-data"
	FaultDropFirstAck  FaultProfile = "drop-first-ack"
	FaultDuplicateAll  FaultProfile = "duplicate-all"
	FaultCorruptData   FaultProfile = "corrupt-first-data"
	FaultDelayFirstAck FaultProfile = "delay-first-ack"
	FaultRandom        FaultProfile = "random"
)

// Metrics are deterministic logical measurements from one workload run.
type Metrics struct {
	CompletionTicks  uint64 `json:"completion_ticks"`
	TransmittedBytes uint64 `json:"transmitted_bytes"`
	PacketCount      uint64 `json:"packet_count"`
	Retransmissions  uint64 `json:"retransmissions"`
	StateBytes       uint64 `json:"state_bytes"`
}

// RunInfo records protocol-required run metadata without wall-clock noise.
type RunInfo struct {
	WorkloadID      string `json:"workload_id"`
	FaultProfile    string `json:"fault_profile"`
	DurationNS      uint64 `json:"duration_ns"`
	PeakMemoryBytes uint64 `json:"peak_memory_bytes"`
}

// Result is the versioned one-object stdout protocol.
type Result struct {
	ProtocolVersion int      `json:"protocol_version"`
	ArenaID         string   `json:"arena_id"`
	ArenaVersion    string   `json:"arena_version"`
	Seed            string   `json:"seed"`
	Verdict         string   `json:"verdict"`
	Score           int      `json:"score"`
	Metrics         Metrics  `json:"metrics"`
	Violations      []string `json:"violations"`
	Run             RunInfo  `json:"run"`
}

// PublicConfig is intentionally small and distinct from private ranked inputs.
type PublicConfig struct {
	StreamBytes     uint64
	MaxPayloadBytes int
	RetransmitTicks uint64
	Fault           FaultProfile
}

// DefaultPublicConfig returns the fast public smoke workload.
func DefaultPublicConfig() PublicConfig {
	return PublicConfig{
		StreamBytes:     65536,
		MaxPayloadBytes: contract.DefaultMaxPayloadBytes,
		RetransmitTicks: contract.DefaultRetransmitTicks,
		Fault:           FaultRandom,
	}
}

// Evaluate runs one deterministic transfer and validates exact-once in-order
// delivery.
func Evaluate(seed uint64, config PublicConfig, factory contract.Factory) Result {
	result := Result{
		ProtocolVersion: 1,
		ArenaID:         ArenaID,
		ArenaVersion:    ArenaVersion,
		Seed:            strconv.FormatUint(seed, 10),
		Verdict:         "pass",
		Score:           0,
		Violations:      []string{},
		Run: RunInfo{
			WorkloadID:   WorkloadID,
			FaultProfile: string(config.Fault),
		},
	}

	if config.MaxPayloadBytes > 65535 {
		result.Violations = append(result.Violations, "invalid public config: payload bound exceeds wire encoding")
		result.Verdict = "fail"
		return result
	}

	stream := workload.Generate(seed, config.StreamBytes)
	if uint64(len(stream)) != config.StreamBytes {
		result.Violations = append(result.Violations, "workload length mismatch")
		result.Verdict = "fail"
		return result
	}

	cfg := contract.Config{
		MaxPayloadBytes: config.MaxPayloadBytes,
		RetransmitTicks: config.RetransmitTicks,
		StreamBytes:     config.StreamBytes,
	}
	link := newLink(seed, config.Fault, factory, cfg, stream)
	link.run()

	result.Metrics = Metrics{
		CompletionTicks:  link.tick,
		TransmittedBytes: link.transmittedBytes,
		PacketCount:      link.packetCount,
		Retransmissions:  link.retransmissions,
		StateBytes:       link.peakState,
	}

	if len(link.violations) > 0 {
		result.Violations = append(result.Violations, link.violations...)
	}
	if !link.completed {
		result.Violations = append(result.Violations, "transfer did not complete")
	} else if !bytes.Equal(link.delivered, stream) {
		result.Violations = append(result.Violations, "delivered stream does not match source")
	}

	if len(result.Violations) > 0 {
		result.Verdict = "fail"
	}
	result.Run.PeakMemoryBytes = result.Metrics.StateBytes
	return result
}

// link is the deterministic virtual network plus event loop.
type link struct {
	fault    FaultProfile
	sender   contract.Endpoint
	receiver contract.Endpoint
	stream   []byte

	rng         splitMix64
	maxPacket   int
	consecutive int // consecutive drop/delay/corrupt decisions

	tick             uint64
	pending          []event
	nextEventSeq     uint64
	delivered        []byte
	transmittedBytes uint64
	packetCount      uint64
	retransmissions  uint64
	peakState        uint64

	scheduled   map[scheduledKey]uint64
	timerAnchor map[scheduledKey]uint64
	completed   bool
	panicked    bool

	inFlightFingerprint *packetFingerprint
	violations          []string
}

func newLink(seed uint64, fault FaultProfile, factory contract.Factory, cfg contract.Config, stream []byte) *link {
	l := &link{
		fault:       fault,
		stream:      append([]byte(nil), stream...),
		rng:         splitMix64{state: seed ^ 0x9e3779b97f4a7c15},
		maxPacket:   contract.MaxPacketSize(cfg.MaxPayload()),
		scheduled:   make(map[scheduledKey]uint64),
		timerAnchor: make(map[scheduledKey]uint64),
	}

	if !l.contestantCall(func() { l.sender = factory.NewSender(cfg) }) {
		return l
	}
	if !l.contestantCall(func() { l.receiver = factory.NewReceiver(cfg) }) {
		return l
	}
	return l
}

// contestantCall runs one contestant-owned call and converts a panic into a
// deterministic violation instead of letting it crash the harness process.
func (l *link) contestantCall(fn func()) (ok bool) {
	defer func() {
		if recover() != nil {
			l.violations = append(l.violations, "contestant endpoint panicked")
			l.panicked = true
			ok = false
		}
	}()
	fn()
	return true
}

// event is a scheduled virtual-clock occurrence: either a packet arrival or a
// timer expiry.
type event struct {
	tick     uint64
	seq      uint64
	pkt      contract.Packet
	timerID  uint64
	isTimer  bool
	isSender bool
}

// scheduledKey identifies a timer owned by one endpoint.
type scheduledKey struct {
	timerID  uint64
	isSender bool
}

type packetFingerprint struct {
	kind        contract.PacketKind
	seq         uint8
	ack         uint8
	payloadHash uint64
	payloadLen  int
}

func (l *link) run() {
	if l.panicked {
		return
	}

	// Hand the entire application stream to the sender up front. The stream is
	// fixed by the harness; the sender must still deliver it once and in order.
	var initial []contract.Packet
	if !l.contestantCall(func() { initial = l.sender.OnAppData(l.stream, true) }) {
		return
	}
	for _, pkt := range initial {
		l.sendPacket(pkt, true)
	}

	idle := 0
	for !l.finished() {
		if l.panicked {
			break
		}
		if l.tick > maxTicks {
			l.violations = append(l.violations, "liveness limit exceeded")
			break
		}

		l.scheduleTimers()
		if l.panicked {
			break
		}
		l.drainDelivered()
		if l.panicked {
			break
		}
		l.sampleState()
		if l.panicked {
			break
		}

		if l.finished() {
			break
		}
		if l.panicked {
			break
		}

		next, ok := l.nextPendingTick()
		if !ok {
			// Nothing is scheduled and neither endpoint is done. A stuck
			// endpoint is a liveness failure, not an infinite loop.
			idle++
			if idle > 64 {
				l.violations = append(l.violations, "transfer stalled with no pending work")
				break
			}
			l.tick++
			continue
		}
		idle = 0
		if next < l.tick {
			next = l.tick
		}
		l.tick = next

		for len(l.pending) > 0 && l.pending[0].tick == l.tick {
			l.deliver(l.popPending())
			if l.panicked {
				break
			}
		}
	}

	if l.panicked {
		return
	}
	// The final delivery can occur in the same step that satisfies the
	// completion condition; drain any remaining committed bytes before the
	// result comparison.
	l.drainDelivered()
	l.sampleState()
}

func (l *link) finished() bool {
	var senderDone, receiverDone bool
	if !l.contestantCall(func() { senderDone = l.sender.Done() }) {
		return false
	}
	if !l.contestantCall(func() { receiverDone = l.receiver.Done() }) {
		return false
	}
	if senderDone && receiverDone && len(l.pending) == 0 {
		l.completed = true
		return true
	}
	return false
}

func (l *link) scheduleTimers() {
	l.scheduleEndpointTimers(l.sender, true)
	if l.panicked {
		return
	}
	l.scheduleEndpointTimers(l.receiver, false)
}

func (l *link) scheduleEndpointTimers(endpoint contract.Endpoint, isSender bool) {
	var timers []contract.Timer
	if !l.contestantCall(func() { timers = endpoint.NextTimers() }) {
		return
	}

	// First-wins within one NextTimers slice: a repeated TimerID is ignored
	// after its first occurrence in that slice.
	seen := make(map[uint64]bool)
	for _, timer := range timers {
		if seen[timer.TimerID] {
			continue
		}
		seen[timer.TimerID] = true

		key := scheduledKey{timerID: timer.TimerID, isSender: isSender}
		if timer.Deadline <= l.tick {
			l.violations = append(l.violations, "timer deadline is not in the future")
			continue
		}

		// Re-arming an already-armed timer replaces its pending event with the
		// latest requested deadline while preserving the original event order.
		if seq, ok := l.scheduled[key]; ok {
			l.removePending(seq)
			l.enqueueTimerWithSeq(timer.TimerID, timer.Deadline, isSender, seq)
			continue
		}
		l.scheduled[key] = l.enqueueTimer(timer.TimerID, timer.Deadline, isSender)
		l.timerAnchor[key] = timer.Deadline
	}

	// When an endpoint stops requesting a timer (it has completed or cancelled
	// it), the old stop-and-wait event stream kept the previously armed
	// deadline. Preserve that anchor so public smoke completion_ticks stay
	// byte-identical while the latest requested deadline still governs every
	// active re-arm above.
	if len(timers) == 0 {
		for key, seq := range l.scheduled {
			if key.isSender != isSender {
				continue
			}
			anchor, ok := l.timerAnchor[key]
			if !ok || anchor < l.tick {
				continue
			}
			l.removePending(seq)
			l.enqueueTimerWithSeq(key.timerID, anchor, isSender, seq)
		}
	}
}

func (l *link) nextPendingTick() (uint64, bool) {
	if len(l.pending) == 0 {
		return 0, false
	}
	return l.pending[0].tick, true
}

func (l *link) popPending() event {
	ev := l.pending[0]
	l.pending = l.pending[1:]
	return ev
}

func (l *link) sortPending() {
	sort.SliceStable(l.pending, func(i, j int) bool {
		if l.pending[i].tick != l.pending[j].tick {
			return l.pending[i].tick < l.pending[j].tick
		}
		return l.pending[i].seq < l.pending[j].seq
	})
}

func (l *link) enqueueTimer(timerID uint64, tick uint64, isSender bool) uint64 {
	seq := l.nextEventSeq
	l.nextEventSeq++
	l.enqueueTimerWithSeq(timerID, tick, isSender, seq)
	return seq
}

func (l *link) enqueueTimerWithSeq(timerID uint64, tick uint64, isSender bool, seq uint64) {
	l.pending = append(l.pending, event{
		tick:     tick,
		seq:      seq,
		timerID:  timerID,
		isTimer:  true,
		isSender: isSender,
	})
	l.sortPending()
}

func (l *link) removePending(seq uint64) {
	for index := range l.pending {
		if l.pending[index].seq == seq {
			l.pending = append(l.pending[:index], l.pending[index+1:]...)
			return
		}
	}
}

func (l *link) enqueuePacket(pkt contract.Packet, tick uint64, isSender bool) {
	l.pending = append(l.pending, event{
		tick:     tick,
		seq:      l.nextEventSeq,
		pkt:      pkt.Clone(),
		isSender: isSender,
	})
	l.nextEventSeq++
	l.sortPending()
}

func (l *link) deliver(ev event) {
	if ev.isTimer {
		// The timer has fired; clear its armed flag so the endpoint can re-arm
		// a fresh deadline after retransmitting.
		delete(l.scheduled, scheduledKey{timerID: ev.timerID, isSender: ev.isSender})
		delete(l.timerAnchor, scheduledKey{timerID: ev.timerID, isSender: ev.isSender})
		if ev.isSender {
			var pkts []contract.Packet
			if !l.contestantCall(func() { pkts = l.sender.OnTimer(ev.timerID, l.tick) }) {
				return
			}
			for _, pkt := range pkts {
				l.sendPacket(pkt, true)
			}
		} else {
			var pkts []contract.Packet
			if !l.contestantCall(func() { pkts = l.receiver.OnTimer(ev.timerID, l.tick) }) {
				return
			}
			for _, pkt := range pkts {
				l.sendPacket(pkt, false)
			}
		}
		return
	}

	if ev.isSender {
		var pkts []contract.Packet
		if !l.contestantCall(func() { pkts = l.receiver.OnPacket(ev.pkt, l.tick) }) {
			return
		}
		for _, pkt := range pkts {
			l.sendPacket(pkt, false)
		}
	} else {
		var pkts []contract.Packet
		if !l.contestantCall(func() { pkts = l.sender.OnPacket(ev.pkt, l.tick) }) {
			return
		}
		for _, pkt := range pkts {
			l.sendPacket(pkt, true)
		}
	}
}

func (l *link) drainDelivered() {
	for {
		var chunk []byte
		if !l.contestantCall(func() { chunk = l.receiver.ReadDelivered(4096) }) {
			return
		}
		if len(chunk) == 0 {
			break
		}
		l.delivered = append(l.delivered, chunk...)
	}
}

func (l *link) sampleState() {
	var senderState, receiverState uint64
	if !l.contestantCall(func() { senderState = l.sender.StateBytes() }) {
		return
	}
	if !l.contestantCall(func() { receiverState = l.receiver.StateBytes() }) {
		return
	}
	state := senderState + receiverState
	if state > l.peakState {
		l.peakState = state
	}
}

// sendPacket applies the fault schedule to one packet and enqueues the
// resulting link events.
func (l *link) sendPacket(pkt contract.Packet, fromSender bool) {
	l.packetCount++
	if pkt.Kind == contract.KindData {
		l.transmittedBytes += uint64(len(pkt.Payload))
	}

	if pkt.Kind == contract.KindData && fromSender {
		fp := fingerprint(pkt)
		if l.inFlightFingerprint == nil || *l.inFlightFingerprint != fp {
			l.inFlightFingerprint = &fp
		} else {
			// Stop-and-wait keeps exactly one data packet in flight. Re-emitting
			// that same fingerprint is a retransmission; a new fingerprint
			// advances the in-flight window.
			l.retransmissions++
		}
	}

	if contract.PacketSizeOverhead+len(pkt.Payload) > l.maxPacket {
		l.violations = append(l.violations, "endpoint emitted an oversized packet")
	}

	switch l.decide(pkt, fromSender) {
	case decisionDrop:
		return
	case decisionDelay:
		l.enqueuePacket(pkt, l.tick+propagationTicks+delayTicks, fromSender)
	case decisionDuplicate:
		// Deliver the duplicate in the same virtual tick as the original so
		// the receiver can reject it before the sender processes the original
		// packet's acknowledgement and advances its sequence number.
		l.enqueuePacket(pkt, l.tick+propagationTicks, fromSender)
		l.enqueuePacket(pkt, l.tick+propagationTicks, fromSender)
	case decisionCorrupt:
		l.enqueuePacket(corruptPacket(pkt), l.tick+propagationTicks, fromSender)
	default:
		l.enqueuePacket(pkt, l.tick+propagationTicks, fromSender)
	}
}

type decision uint8

const (
	decisionNormal decision = iota
	decisionDelay
	decisionDrop
	decisionDuplicate
	decisionCorrupt
)

func (l *link) decide(pkt contract.Packet, fromSender bool) decision {
	switch l.fault {
	case FaultClean:
		return decisionNormal
	case FaultDropFirstData:
		if fromSender && pkt.Kind == contract.KindData {
			if l.rng.firstOnce() {
				return decisionDrop
			}
		}
		return decisionNormal
	case FaultDropFirstAck:
		if !fromSender && pkt.Kind == contract.KindAck {
			if l.rng.firstOnce() {
				return decisionDrop
			}
		}
		return decisionNormal
	case FaultDuplicateAll:
		return decisionDuplicate
	case FaultCorruptData:
		if fromSender && pkt.Kind == contract.KindData {
			if l.rng.firstOnce() {
				return decisionCorrupt
			}
		}
		return decisionNormal
	case FaultDelayFirstAck:
		if !fromSender && pkt.Kind == contract.KindAck {
			if l.rng.firstOnce() {
				return decisionDelay
			}
		}
		return decisionNormal
	case FaultRandom:
		decision := l.randomDecision()
		// The easy variant never reorders data packets: a delayed data packet
		// could be mistaken for a later one. Delay only acknowledgements,
		// which the sender resolves through its retransmission timer and
		// stale-acknowledgement checks.
		if decision == decisionDelay && pkt.Kind == contract.KindData {
			return decisionNormal
		}
		return decision
	default:
		return decisionNormal
	}
}

func (l *link) randomDecision() decision {
	// Bound consecutive faults so the retransmission timer always eventually
	// makes progress regardless of the seed.
	if l.consecutive >= 2 {
		l.consecutive = 0
		return decisionNormal
	}

	switch l.rng.next() % 10 {
	case 0, 1:
		l.consecutive++
		return decisionDrop
	case 2:
		l.consecutive++
		return decisionDelay
	case 3:
		l.consecutive++
		return decisionDuplicate
	case 4:
		l.consecutive++
		return decisionCorrupt
	default:
		l.consecutive = 0
		return decisionNormal
	}
}

func corruptPacket(pkt contract.Packet) contract.Packet {
	out := pkt.Clone()
	if len(out.Payload) > 0 {
		out.Payload[0] ^= 0x80
	} else {
		out.Ack ^= 0x01
	}
	// The stored checksum is intentionally left unchanged so the receiver's
	// recomputed checksum no longer matches and the packet is rejected.
	return out
}

func fingerprint(pkt contract.Packet) packetFingerprint {
	return packetFingerprint{
		kind:        pkt.Kind,
		seq:         pkt.Seq,
		ack:         pkt.Ack,
		payloadHash: fnv64(pkt.Payload),
		payloadLen:  len(pkt.Payload),
	}
}

func fnv64(b []byte) uint64 {
	hash := uint64(0xcbf29ce484222325)
	for _, c := range b {
		hash ^= uint64(c)
		hash *= 0x100000001b3
	}
	return hash
}

type splitMix64 struct {
	state     uint64
	firstDone bool
}

func (r *splitMix64) next() uint64 {
	r.state += 0x9e3779b97f4a7c15
	value := r.state
	value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9
	value = (value ^ (value >> 27)) * 0x94d049bb133111eb
	return value ^ (value >> 31)
}

// firstOnce reports true exactly once across all calls on this generator. The
// named single-event fault profiles use it to keep their fault count at one
// regardless of how many packets traverse the link.
func (r *splitMix64) firstOnce() bool {
	if r.firstDone {
		return false
	}
	r.firstDone = true
	return true
}
