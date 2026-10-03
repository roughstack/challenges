// Package harness owns public execution, the deterministic device, fault
// injection, crash simulation, accounting, correctness gates, and result
// construction for the journal-recovery-easy arena. The contestant surface is
// the contract.Journal interface only.
package harness

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"strconv"

	"github.com/roughstack/challenges/challenges/journal-recovery-easy/contract"
	"github.com/roughstack/challenges/challenges/journal-recovery-easy/workload"
)

const (
	// ArenaID is the stable public identifier for this variant.
	ArenaID = "journal-recovery-easy"
	// ArenaVersion is the immutable public version for this variant.
	ArenaVersion = "1.0.0"
	// WorkloadID identifies the deterministic public smoke workload.
	WorkloadID = "public-smoke-v1"
)

// FaultProfile selects a deterministic public crash/fault applied between the
// append and recovery phases.
type FaultProfile string

const (
	FaultClean           FaultProfile = "clean"
	FaultEmpty           FaultProfile = "empty"
	FaultPartialHeader   FaultProfile = "partial-header"
	FaultTornTail        FaultProfile = "torn-tail"
	FaultTailGarbage     FaultProfile = "tail-garbage"
	FaultInteriorBitFlip FaultProfile = "interior-bit-flip"
	FaultLengthBomb      FaultProfile = "length-bomb"
	FaultSequenceGap     FaultProfile = "sequence-gap"
	FaultDuplicateLSN    FaultProfile = "duplicate-lsn"
)

// errApplyFail is the deterministic error the harness injects into the
// recovery apply callback when config.FailApplyAt requests it.
var errApplyFail = errors.New("harness: apply callback failed")

// Metrics are deterministic logical measurements from one workload run.
type Metrics struct {
	AppendThroughput  uint64 `json:"append_throughput"`
	RecoveryBytesRead uint64 `json:"recovery_bytes_read"`
	WriteOverhead     uint64 `json:"write_overhead"`
	Allocations       uint64 `json:"allocations"`
	DiskBytes         uint64 `json:"disk_bytes"`
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
	Records     int
	MaxPayload  int
	SyncEvery   int // >0: harness acknowledges durability every N records
	Fault       FaultProfile
	FailApplyAt int // >0: recovery apply callback fails on this many-th record
}

// DefaultPublicConfig returns the fast public smoke workload.
func DefaultPublicConfig() PublicConfig {
	return PublicConfig{
		Records:    96,
		MaxPayload: 1024,
		SyncEvery:  1,
		Fault:      FaultTornTail,
	}
}

// Evaluate runs the deterministic public workload against a contestant journal.
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

	r := &runner{result: &result}
	normalize(&config)
	result.Run.FaultProfile = string(config.Fault)

	if config.MaxPayload > contract.MaxPayloadBytes {
		r.violate("invalid public config: payload bound exceeds frame encoding")
		return result
	}

	records := workload.Generate(workload.Config{
		Seed:       seed,
		Records:    config.Records,
		MaxPayload: config.MaxPayload,
	})
	expected := cloneRecords(records)

	device := &memDevice{}
	var journal contract.Journal
	if !r.call(func() {
		var err error
		journal, err = factory("", device)
		if err != nil {
			r.violate("OpenJournal failed: " + err.Error())
		} else if journal == nil {
			r.violate("OpenJournal returned a nil journal")
		}
	}) {
		return result
	}

	expectedBytes := make([]byte, 0)
	appended := 0
appendPhase:
	for index := range records {
		lsn := uint64(0)
		var appendErr error
		if !r.call(func() {
			lsn, appendErr = journal.Append(records[index])
		}) {
			break appendPhase
		}
		if appendErr != nil {
			r.violate("Append failed: " + appendErr.Error())
			break appendPhase
		}
		if lsn != uint64(index+1) {
			r.violate("Append returned LSN " + strconv.FormatUint(lsn, 10) + ", want " + strconv.FormatUint(uint64(index+1), 10))
		}
		appended++

		// Prove Append copied the payload before returning.
		if len(records[index].Payload) > 0 {
			records[index].Payload[0] ^= 0xff
		}

		// Enforce the per-Append device write: the live device must already
		// contain exactly the cumulative frames for every appended record.
		expectedBytes = append(expectedBytes, contract.EncodeRecord(expected[index])...)
		if len(device.live) != len(expectedBytes) || !bytes.Equal(device.live, expectedBytes) {
			r.violate("Append did not write record " + strconv.Itoa(index) + " to the device")
			break appendPhase
		}

		if config.SyncEvery > 0 && (index+1)%config.SyncEvery == 0 {
			if !r.call(func() {
				if err := journal.Sync(lsn); err != nil {
					r.violate("Sync failed: " + err.Error())
				}
			}) {
				break appendPhase
			}
		}
	}
	if r.panicked {
		return result
	}
	if appended > 0 {
		// Acknowledge every appended record so the append-phase device scan is
		// deterministic regardless of the SyncEvery batching policy.
		if !r.call(func() {
			if err := journal.Sync(uint64(appended)); err != nil {
				r.violate("final Sync failed: " + err.Error())
			}
		}) {
			return result
		}
	}

	// The append phase must have produced exactly the requested clean frames.
	if !r.panicked {
		onDevice, status, err := referenceScan(device.live)
		if err != nil || status != contract.StatusClean || !recordsEqual(onDevice, expected) {
			r.violate("append phase produced incorrect journal bytes")
		}
	}

	appendWrites := device.writes
	appendSyncs := device.syncs
	appendTruncates := device.truncates
	appendWriteBytes := device.writeBytes
	appendReadBytes := device.readBytes

	// Simulate the crash: discard writes that were never made durable, then
	// apply the deterministic fault to the committed bytes.
	device.crash()
	layouts := frameLayouts(expected)
	applyFault(device, config.Fault, layouts)

	refRecords, refStatus, refErr := referenceScan(device.live)

	var recovered []contract.Record
	var info contract.RecoveryInfo
	var recoverErr error
	if !r.call(func() {
		recoveredJournal, openErr := factory("", device)
		if openErr != nil {
			r.violate("reopen OpenJournal failed: " + openErr.Error())
			return
		}
		if recoveredJournal == nil {
			r.violate("reopen OpenJournal returned a nil journal")
			return
		}
		info, recoverErr = recoveredJournal.Recover(func(record contract.Record) error {
			if config.FailApplyAt > 0 && len(recovered)+1 == config.FailApplyAt {
				return errApplyFail
			}
			recovered = append(recovered, record)
			return nil
		})
		_ = recoveredJournal.Close()
	}) {
		return result
	}

	recoveryReadBytes := device.readBytes - appendReadBytes

	validateRecovery(r, recovered, info, recoverErr, refRecords, refStatus, refErr, config)

	ops := appendWrites + appendSyncs
	if ops == 0 {
		ops = 1
	}
	result.Metrics = Metrics{
		AppendThroughput:  uint64(appended) * 1000 / ops,
		RecoveryBytesRead: recoveryReadBytes,
		WriteOverhead:     overhead(appendWrites, appendSyncs, appendTruncates, uint64(appended)),
		Allocations:       device.reads + device.writes,
		DiskBytes:         appendWriteBytes,
	}
	result.Run.PeakMemoryBytes = uint64(len(device.live))

	if len(result.Violations) > 0 {
		result.Verdict = "fail"
	}
	return result
}

// runner carries panic isolation and violation recording through Evaluate.
type runner struct {
	result   *Result
	panicked bool
}

func (r *runner) violate(message string) {
	r.result.Violations = append(r.result.Violations, message)
	r.result.Verdict = "fail"
}

// call runs one contestant-owned operation and converts a panic into a
// deterministic violation instead of crashing the harness process.
func (r *runner) call(fn func()) (ok bool) {
	if r.panicked {
		return false
	}
	defer func() {
		if recover() != nil {
			r.panicked = true
			r.result.Violations = append(r.result.Violations, "contestant journal panicked")
			r.result.Verdict = "fail"
		}
	}()
	fn()
	return !r.panicked
}

func normalize(config *PublicConfig) {
	if config.Records <= 0 {
		config.Records = DefaultPublicConfig().Records
	}
	if config.MaxPayload <= 0 {
		config.MaxPayload = DefaultPublicConfig().MaxPayload
	}
	if config.SyncEvery <= 0 {
		config.SyncEvery = 1
	}
	if config.Fault == "" {
		config.Fault = FaultClean
	}
}

func overhead(writes, syncs, truncates, appended uint64) uint64 {
	if writes+syncs+truncates <= appended {
		return 0
	}
	return writes + syncs + truncates - appended
}

func cloneRecords(records []contract.Record) []contract.Record {
	out := make([]contract.Record, len(records))
	for index, record := range records {
		out[index] = contract.Record{
			LSN:     record.LSN,
			Payload: append([]byte(nil), record.Payload...),
		}
	}
	return out
}

func recordsEqual(a, b []contract.Record) bool {
	if len(a) != len(b) {
		return false
	}
	for index := range a {
		if a[index].LSN != b[index].LSN || !bytes.Equal(a[index].Payload, b[index].Payload) {
			return false
		}
	}
	return true
}

// frameLayout is the expected byte range of one record's frame.
type frameLayout struct {
	start int
	end   int
}

func frameLayouts(records []contract.Record) []frameLayout {
	layouts := make([]frameLayout, len(records))
	off := 0
	for index, record := range records {
		size := contract.FrameOverhead + len(record.Payload)
		layouts[index] = frameLayout{start: off, end: off + size}
		off += size
	}
	return layouts
}

// memDevice is the harness-owned deterministic device with explicit durability
// and fault hooks.
type memDevice struct {
	durable []byte
	live    []byte

	reads      uint64
	readBytes  uint64
	writes     uint64
	writeBytes uint64
	syncs      uint64
	truncates  uint64
	renames    uint64
}

func (d *memDevice) Size() (uint64, error) { return uint64(len(d.live)), nil }

func (d *memDevice) Read(offset uint64, length uint64) ([]byte, error) {
	if offset > uint64(len(d.live)) || length > uint64(len(d.live))-offset {
		return nil, errors.New("device: read out of range")
	}
	d.reads++
	d.readBytes += length
	return append([]byte(nil), d.live[offset:offset+length]...), nil
}

func (d *memDevice) Write(offset uint64, data []byte) error {
	end := offset + uint64(len(data))
	if end > uint64(len(d.live)) {
		grown := make([]byte, end)
		copy(grown, d.live)
		d.live = grown
	}
	copy(d.live[offset:], data)
	d.writes++
	d.writeBytes += uint64(len(data))
	return nil
}

func (d *memDevice) Sync() error {
	d.syncs++
	d.durable = append(d.durable[:0], d.live...)
	return nil
}

func (d *memDevice) Rename(_, _ string) error {
	d.renames++
	return nil
}

func (d *memDevice) Truncate(size uint64) error {
	if size > uint64(len(d.live)) {
		grown := make([]byte, size)
		copy(grown, d.live)
		d.live = grown
	} else {
		d.live = d.live[:size]
	}
	d.truncates++
	return nil
}

// crash discards every write that was never made durable.
func (d *memDevice) crash() {
	d.live = append(d.live[:0], d.durable...)
}

// applyFault mutates the committed device bytes according to the fault profile.
func applyFault(device *memDevice, fault FaultProfile, layouts []frameLayout) {
	switch fault {
	case FaultEmpty:
		device.durable = device.durable[:0]
	case FaultPartialHeader:
		if n := len(layouts); n > 0 {
			cut := layouts[n-1].start + contract.FrameHeaderSize - 1
			if cut > len(device.durable) {
				cut = len(device.durable)
			}
			device.durable = device.durable[:cut]
		}
	case FaultTornTail:
		if n := len(layouts); n > 0 {
			last := layouts[n-1]
			cut := last.start + contract.FrameHeaderSize + (last.end-last.start-contract.FrameOverhead)/2
			if cut > len(device.durable) {
				cut = len(device.durable)
			}
			if cut < last.start {
				cut = len(device.durable)
			}
			device.durable = device.durable[:cut]
		}
	case FaultTailGarbage:
		device.durable = append(device.durable, bytes.Repeat([]byte{0xff}, contract.FrameOverhead)...)
	case FaultInteriorBitFlip:
		if len(layouts) >= 3 {
			second := layouts[1]
			at := second.start + contract.FrameHeaderSize
			if at < second.end && at < len(device.durable) {
				device.durable[at] ^= 0x01
			}
		}
	case FaultLengthBomb:
		if len(layouts) >= 2 {
			second := layouts[1]
			if second.start+9 <= len(device.durable) {
				binary.BigEndian.PutUint32(device.durable[second.start+5:second.start+9], contract.MaxPayloadBytes+1)
			}
		}
	case FaultSequenceGap:
		if len(layouts) >= 2 {
			second := layouts[1]
			mutateSequence(device.durable, second, uint64(len(layouts)+10))
		}
	case FaultDuplicateLSN:
		if len(layouts) >= 2 {
			second := layouts[1]
			mutateSequence(device.durable, second, 1)
		}
	}

	// The fault is committed: the visible device matches the damaged durable
	// bytes after the crash.
	device.live = append(device.live[:0], device.durable...)
}

// mutateSequence rewrites a frame's sequence number and fixes its checksum so
// the frame is complete and checksum-valid but has a wrong sequence.
func mutateSequence(data []byte, layout frameLayout, seq uint64) {
	if layout.start+contract.FrameHeaderSize > len(data) || layout.end > len(data) {
		return
	}
	frame := data[layout.start:layout.end]
	binary.BigEndian.PutUint64(frame[9:17], seq)
	fixChecksum(frame)
}

func fixChecksum(frame []byte) {
	sum := crc32.ChecksumIEEE(frame[:len(frame)-contract.ChecksumSize])
	binary.BigEndian.PutUint32(frame[len(frame)-contract.ChecksumSize:], sum)
}

// referenceScan implements the public recovery semantics that the harness uses
// as its ground-truth model. It mirrors the documented contract exactly.
func referenceScan(data []byte) (records []contract.Record, status contract.RecoveryStatus, err error) {
	if len(data) == 0 {
		return nil, contract.StatusEmpty, nil
	}

	off := 0
	expectedSeq := uint64(1)
	for {
		if off == len(data) {
			return records, contract.StatusClean, nil
		}
		if len(data)-off < contract.FrameHeaderSize {
			return records, contract.StatusTornTail, nil
		}

		header := data[off : off+contract.FrameHeaderSize]
		length, seq, ok := contract.ParseHeader(header)
		if !ok {
			if findNextValidFrame(data, off+1) >= 0 {
				return records, contract.StatusCorrupt, contract.ErrCorrupt
			}
			return records, contract.StatusTailGarbage, nil
		}
		if uint64(length) > contract.MaxPayloadBytes {
			return records, contract.StatusCorrupt, contract.ErrLengthBomb
		}
		frameEnd := off + contract.FrameHeaderSize + int(length) + contract.ChecksumSize
		if frameEnd > len(data) {
			return records, contract.StatusTornTail, nil
		}
		frame := data[off:frameEnd]
		if !contract.VerifyFrame(frame) {
			return records, contract.StatusCorrupt, contract.ErrCorrupt
		}
		if seq != expectedSeq {
			return records, contract.StatusCorrupt, contract.ErrSequence
		}
		records = append(records, contract.Record{
			LSN:     seq,
			Payload: append([]byte(nil), frame[contract.FrameHeaderSize:len(frame)-contract.ChecksumSize]...),
		})
		expectedSeq++
		off = frameEnd
	}
}

func findNextValidFrame(data []byte, from int) int {
	if from < 0 {
		from = 0
	}
	magic := contract.MagicBytes()
	for offset := from; offset+contract.FrameOverhead <= len(data); offset++ {
		if data[offset] != magic[0] {
			continue
		}
		if validCompleteFrameAt(data, offset) {
			return offset
		}
	}
	return -1
}

func validCompleteFrameAt(data []byte, off int) bool {
	if off+contract.FrameOverhead > len(data) {
		return false
	}
	length, _, ok := contract.ParseHeader(data[off : off+contract.FrameHeaderSize])
	if !ok {
		return false
	}
	if uint64(length) > contract.MaxPayloadBytes {
		return false
	}
	end := off + contract.FrameHeaderSize + int(length) + contract.ChecksumSize
	if end > len(data) {
		return false
	}
	return contract.VerifyFrame(data[off:end])
}

// validateRecovery compares the contestant's recovery result with the harness
// reference model and records violations on any mismatch.
func validateRecovery(r *runner, recovered []contract.Record, info contract.RecoveryInfo, recoverErr error, refRecords []contract.Record, refStatus contract.RecoveryStatus, refErr error, config PublicConfig) {
	if config.FailApplyAt > 0 {
		wantApplied := len(refRecords)
		if config.FailApplyAt-1 < wantApplied {
			wantApplied = config.FailApplyAt - 1
		}
		shouldFail := config.FailApplyAt <= len(refRecords)
		if shouldFail {
			if recoverErr == nil {
				r.violate("recovery swallowed the apply callback error")
			} else if !errors.Is(recoverErr, errApplyFail) {
				r.violate("recovery did not propagate the apply callback error")
			}
		} else if recoverErr != nil {
			if refErr == nil {
				r.violate("recovery returned an unexpected apply callback error")
			} else if !errors.Is(recoverErr, refErr) {
				r.violate("recovery returned the wrong error")
			}
		}
		if len(recovered) != wantApplied {
			r.violate("recovery applied the wrong number of records before the callback failure")
		}
		if !recordsEqual(recovered, refRecords[:wantApplied]) {
			r.violate("recovery applied records that do not match the valid prefix")
		}
		return
	}

	if (recoverErr == nil) != (refErr == nil) {
		r.violate("recovery error presence does not match the journal state")
	}
	if refErr != nil && recoverErr != nil && !errors.Is(recoverErr, refErr) {
		r.violate("recovery returned the wrong error")
	}
	if refErr != nil && info.Status != contract.StatusCorrupt {
		r.violate("recovery did not report a corrupt journal")
	}
	if refErr == nil && info.Status != refStatus {
		r.violate("recovery reported status " + info.Status.String() + ", want " + refStatus.String())
	}
	if uint64(len(recovered)) != info.RecordsApplied {
		r.violate("recovery applied-count mismatch")
	}
	if !recordsEqual(recovered, refRecords) {
		r.violate("recovery applied records that do not match the valid prefix")
	}
	if len(refRecords) > 0 && info.LastLSN != refRecords[len(refRecords)-1].LSN {
		r.violate("recovery reported the wrong last LSN")
	}
}
