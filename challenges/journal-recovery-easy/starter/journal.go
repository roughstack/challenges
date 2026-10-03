// Package starter contains a deliberately simple, correct length-delimited
// record journal over the injected Device.
//
// The implementation is intentionally straightforward: Append writes one full
// frame and then Syncs immediately, so every append is durable before it
// returns. Recover reads the whole device in one pass and then scans it. This
// is correct but non-optimal: a tuned implementation can batch durability
// behind the explicit Sync calls and read the journal incrementally. That
// leaves the central storage optimization to the contestant.
package starter

import (
	"errors"

	"github.com/roughstack/challenges/challenges/journal-recovery-easy/contract"
)

type journal struct {
	device  contract.Device
	closed  bool
	lastLSN uint64
}

// OpenJournal returns a single-file in-simulation journal over device.
// dir is reserved for persistent tiers and is ignored here.
func OpenJournal(_ string, device contract.Device) (contract.Journal, error) {
	if device == nil {
		return nil, errors.New("journal: nil device")
	}
	return &journal{device: device}, nil
}

func (j *journal) Append(record contract.Record) (uint64, error) {
	if j.closed {
		return 0, contract.ErrClosed
	}

	lsn := j.lastLSN + 1
	frame := contract.EncodeRecord(contract.Record{LSN: lsn, Payload: record.Payload})

	size, err := j.device.Size()
	if err != nil {
		return 0, err
	}
	if err := j.device.Write(size, frame); err != nil {
		return 0, err
	}
	if err := j.device.Sync(); err != nil {
		return 0, err
	}
	j.lastLSN = lsn
	return lsn, nil
}

func (j *journal) Sync(upto uint64) error {
	if j.closed {
		return contract.ErrClosed
	}
	if upto > j.lastLSN {
		return contract.ErrUnknownLSN
	}
	// Every Append already synced its own frame, so there is nothing left to
	// flush for any acknowledged LSN.
	return nil
}

func (j *journal) Recover(apply func(contract.Record) error) (contract.RecoveryInfo, error) {
	info := contract.RecoveryInfo{Status: contract.StatusEmpty}
	if j.closed {
		return info, contract.ErrClosed
	}

	size, err := j.device.Size()
	if err != nil {
		return info, err
	}
	if size == 0 {
		return info, nil
	}

	data, err := j.device.Read(0, size)
	if err != nil {
		return info, err
	}
	if uint64(len(data)) != size {
		return info, errors.New("journal: device returned a short read")
	}

	records, status, scanErr := scanJournal(data)
	applied := uint64(0)
	last := uint64(0)
	for _, record := range records {
		if apply != nil {
			if err := apply(record); err != nil {
				info.Status = status
				info.RecordsApplied = applied
				info.LastLSN = last
				return info, err
			}
		}
		applied++
		last = record.LSN
	}

	info.Status = status
	info.RecordsApplied = applied
	info.LastLSN = last
	return info, scanErr
}

// scanJournal validates the framed byte stream and returns the longest valid
// prefix plus the status of the region that ended the scan. scanErr is non-nil
// only for interior corruption, sequence discontinuities, and length bombs.
func scanJournal(data []byte) (records []contract.Record, status contract.RecoveryStatus, scanErr error) {
	if len(data) == 0 {
		return nil, contract.StatusEmpty, nil
	}

	off := 0
	expectedSeq := uint64(1)
	for {
		if off == len(data) {
			return records, contract.StatusClean, nil
		}
		remaining := len(data) - off
		if remaining < contract.FrameHeaderSize {
			return records, contract.StatusTornTail, nil
		}

		header := data[off : off+contract.FrameHeaderSize]
		length, seq, ok := contract.ParseHeader(header)
		if !ok {
			// A malformed header is tail garbage unless a valid frame follows
			// it somewhere later in the file, in which case the malformed
			// region is interior corruption.
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

func (j *journal) Close() error {
	if j.closed {
		return nil
	}
	j.closed = true
	return nil
}

// findNextValidFrame returns the offset of the next complete, valid frame at or
// after from, or -1 when none exists.
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
