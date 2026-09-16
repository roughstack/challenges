package starter

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/bytearena/arenas/arenas/journal-recovery-easy/contract"
)

// byteDevice is a minimal deterministic Device for the public starter tests.
// The production device with fault hooks lives in the harness package.
type byteDevice struct {
	data []byte
}

func (d *byteDevice) Size() (uint64, error) { return uint64(len(d.data)), nil }

func (d *byteDevice) Read(offset uint64, length uint64) ([]byte, error) {
	if offset > uint64(len(d.data)) || length > uint64(len(d.data))-offset {
		return nil, errors.New("byteDevice: read out of range")
	}
	return append([]byte(nil), d.data[offset:offset+length]...), nil
}

func (d *byteDevice) Write(offset uint64, p []byte) error {
	end := offset + uint64(len(p))
	if end > uint64(len(d.data)) {
		grown := make([]byte, end)
		copy(grown, d.data)
		d.data = grown
	}
	copy(d.data[offset:], p)
	return nil
}

func (d *byteDevice) Sync() error { return nil }

func (d *byteDevice) Truncate(size uint64) error {
	if size > uint64(len(d.data)) {
		grown := make([]byte, size)
		copy(grown, d.data)
		d.data = grown
		return nil
	}
	d.data = d.data[:size]
	return nil
}

func openTestJournal(device contract.Device) contract.Journal {
	journal, err := OpenJournal("", device)
	if err != nil {
		panic(err)
	}
	return journal
}

func appendThree(t *testing.T, device contract.Device) []contract.Record {
	t.Helper()
	journal := openTestJournal(device)
	records := []contract.Record{
		{LSN: 1, Payload: []byte("first-record")},
		{LSN: 2, Payload: bytes.Repeat([]byte{0x5a}, 64)},
		{LSN: 3, Payload: []byte("third")},
	}
	for index, record := range records {
		lsn, err := journal.Append(record)
		if err != nil {
			t.Fatalf("append %d failed: %v", index, err)
		}
		if lsn != uint64(index+1) {
			t.Fatalf("append %d returned LSN %d, want %d", index, lsn, index+1)
		}
	}
	if err := journal.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}
	return records
}

func recoverAll(t *testing.T, device contract.Device) ([]contract.Record, contract.RecoveryInfo, error) {
	t.Helper()
	journal := openTestJournal(device)
	var applied []contract.Record
	info, err := journal.Recover(func(record contract.Record) error {
		applied = append(applied, record)
		return nil
	})
	_ = journal.Close()
	return applied, info, err
}

func TestRoundTripThreeRecordsAfterReopen(t *testing.T) {
	device := &byteDevice{}
	want := appendThree(t, device)

	applied, info, err := recoverAll(t, device)
	if err != nil {
		t.Fatalf("recover failed: %v", err)
	}
	if info.Status != contract.StatusClean {
		t.Fatalf("status = %v, want clean", info.Status)
	}
	if info.RecordsApplied != 3 || info.LastLSN != 3 {
		t.Fatalf("info = %+v, want 3 applied with last LSN 3", info)
	}
	if len(applied) != 3 {
		t.Fatalf("recovered %d records, want 3", len(applied))
	}
	for index := range want {
		if applied[index].LSN != want[index].LSN || !bytes.Equal(applied[index].Payload, want[index].Payload) {
			t.Fatalf("record %d mismatch: got %+v want %+v", index, applied[index], want[index])
		}
	}
}

func TestAppendCopiesPayloadBeforeReturn(t *testing.T) {
	device := &byteDevice{}
	journal := openTestJournal(device)

	payload := []byte("mutable")
	if _, err := journal.Append(contract.Record{LSN: 1, Payload: payload}); err != nil {
		t.Fatal(err)
	}
	payload[0] = 'X'

	applied, _, err := recoverAll(t, device)
	if err != nil {
		t.Fatal(err)
	}
	if len(applied) != 1 || string(applied[0].Payload) != "mutable" {
		t.Fatalf("Append retained a mutable caller alias: %q", applied)
	}
}

func TestSyncRejectsUnknownLSN(t *testing.T) {
	journal := openTestJournal(&byteDevice{})
	if _, err := journal.Append(contract.Record{LSN: 1, Payload: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	if err := journal.Sync(1); err != nil {
		t.Fatalf("Sync(1) = %v, want nil", err)
	}
	if err := journal.Sync(2); !errors.Is(err, contract.ErrUnknownLSN) {
		t.Fatalf("Sync(2) = %v, want ErrUnknownLSN", err)
	}
}

func TestCloseIsIdempotentAndTerminal(t *testing.T) {
	journal := openTestJournal(&byteDevice{})
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	if err := journal.Close(); err != nil {
		t.Fatalf("Close is not idempotent: %v", err)
	}
	if _, err := journal.Append(contract.Record{LSN: 1, Payload: []byte("x")}); !errors.Is(err, contract.ErrClosed) {
		t.Fatalf("Append after Close = %v, want ErrClosed", err)
	}
	if err := journal.Sync(0); !errors.Is(err, contract.ErrClosed) {
		t.Fatalf("Sync after Close = %v, want ErrClosed", err)
	}
	if _, err := journal.Recover(nil); !errors.Is(err, contract.ErrClosed) {
		t.Fatalf("Recover after Close = %v, want ErrClosed", err)
	}
}

func TestEmptyJournalRecoversEmpty(t *testing.T) {
	applied, info, err := recoverAll(t, &byteDevice{})
	if err != nil {
		t.Fatalf("recover failed: %v", err)
	}
	if info.Status != contract.StatusEmpty || info.RecordsApplied != 0 || len(applied) != 0 {
		t.Fatalf("empty recovery = %+v with %d records", info, len(applied))
	}
}

func TestPerByteTailTruncationRecoversPrefix(t *testing.T) {
	device := &byteDevice{}
	appendThree(t, device)
	full := append([]byte(nil), device.data...)

	// Find the start of the final frame so we can truncate each byte of it.
	lastStart := len(full) - len(contract.EncodeRecord(contract.Record{LSN: 3, Payload: []byte("third")}))
	lastFrameLen := len(full) - lastStart

	for cut := 1; cut < lastFrameLen; cut++ {
		truncated := &byteDevice{data: append([]byte(nil), full[:len(full)-cut]...)}
		applied, info, err := recoverAll(t, truncated)
		if err != nil {
			t.Fatalf("cut %d: recover failed: %v", cut, err)
		}
		if info.Status != contract.StatusTornTail {
			t.Fatalf("cut %d: status = %v, want torn-tail", cut, info.Status)
		}
		if len(applied) != 2 {
			t.Fatalf("cut %d: recovered %d records, want 2", cut, len(applied))
		}
		if applied[0].LSN != 1 || applied[1].LSN != 2 {
			t.Fatalf("cut %d: recovered LSNs = %d,%d, want 1,2", cut, applied[0].LSN, applied[1].LSN)
		}
	}

	// Removing the entire last frame is a clean prefix, not a torn tail.
	clean := &byteDevice{data: append([]byte(nil), full[:lastStart]...)}
	applied, info, err := recoverAll(t, clean)
	if err != nil {
		t.Fatal(err)
	}
	if info.Status != contract.StatusClean || len(applied) != 2 {
		t.Fatalf("whole-frame cut = status %v with %d records, want clean with 2", info.Status, len(applied))
	}
}

func TestTailGarbageRecoversValidPrefix(t *testing.T) {
	device := &byteDevice{}
	appendThree(t, device)

	// A garbage region long enough to contain a header but with a bad magic
	// byte is reported as tail garbage, not interior corruption.
	garbage := bytes.Repeat([]byte{0xff}, contract.FrameOverhead)
	garbageDevice := &byteDevice{data: append(append([]byte(nil), device.data...), garbage...)}
	applied, info, err := recoverAll(t, garbageDevice)
	if err != nil {
		t.Fatalf("recover failed: %v", err)
	}
	if info.Status != contract.StatusTailGarbage {
		t.Fatalf("status = %v, want tail-garbage", info.Status)
	}
	if len(applied) != 3 {
		t.Fatalf("recovered %d records, want 3", len(applied))
	}

	// A garbage region shorter than a header is a torn tail.
	short := &byteDevice{data: append(append([]byte(nil), device.data...), 0xff, 0xfe, 0xfd)}
	applied, info, err = recoverAll(t, short)
	if err != nil {
		t.Fatalf("short garbage recover failed: %v", err)
	}
	if info.Status != contract.StatusTornTail || len(applied) != 3 {
		t.Fatalf("short garbage = status %v with %d records, want torn-tail with 3", info.Status, len(applied))
	}
}

func TestInteriorBitFlipIsReportedAndStopsRecovery(t *testing.T) {
	device := &byteDevice{}
	appendThree(t, device)

	// Flip one payload bit inside the second of three full records.
	firstLen := len(contract.EncodeRecord(contract.Record{LSN: 1, Payload: []byte("first-record")}))
	flipAt := firstLen + contract.FrameHeaderSize + 3
	corrupt := &byteDevice{data: append([]byte(nil), device.data...)}
	corrupt.data[flipAt] ^= 0x01

	var applied []contract.Record
	journal := openTestJournal(corrupt)
	info, err := journal.Recover(func(record contract.Record) error {
		applied = append(applied, record)
		return nil
	})
	if !errors.Is(err, contract.ErrCorrupt) {
		t.Fatalf("recover error = %v, want ErrCorrupt", err)
	}
	if info.Status != contract.StatusCorrupt {
		t.Fatalf("status = %v, want corrupt", info.Status)
	}
	if info.RecordsApplied != 1 || info.LastLSN != 1 {
		t.Fatalf("info = %+v, want one applied record", info)
	}
	if len(applied) != 1 || applied[0].LSN != 1 {
		t.Fatalf("applied records = %+v, want only record 1", applied)
	}
}

func TestLengthBombIsRejectedWithBoundedAllocation(t *testing.T) {
	device := &byteDevice{}
	journal := openTestJournal(device)
	if _, err := journal.Append(contract.Record{LSN: 1, Payload: []byte("ok")}); err != nil {
		t.Fatal(err)
	}
	_ = journal.Close()

	// Append a frame whose header declares a payload far above the bound.
	bomb := contract.EncodeRecord(contract.Record{LSN: 2, Payload: []byte("short")})
	binary.BigEndian.PutUint32(bomb[5:9], contract.MaxPayloadBytes+1)
	bombDevice := &byteDevice{data: append(append([]byte(nil), device.data...), bomb...)}

	var applied []contract.Record
	recovered := openTestJournal(bombDevice)
	info, err := recovered.Recover(func(record contract.Record) error {
		applied = append(applied, record)
		return nil
	})
	if !errors.Is(err, contract.ErrLengthBomb) {
		t.Fatalf("recover error = %v, want ErrLengthBomb", err)
	}
	if info.Status != contract.StatusCorrupt {
		t.Fatalf("status = %v, want corrupt", info.Status)
	}
	if info.RecordsApplied != 1 || len(applied) != 1 {
		t.Fatalf("length bomb applied %d records, want only the valid prefix", len(applied))
	}
}

func TestPartialHeaderRecoversPrefix(t *testing.T) {
	device := &byteDevice{}
	appendThree(t, device)
	truncated := &byteDevice{data: append([]byte(nil), device.data[:len(device.data)-len("third")-contract.ChecksumSize-3]...)}

	applied, info, err := recoverAll(t, truncated)
	if err != nil {
		t.Fatalf("recover failed: %v", err)
	}
	if info.Status != contract.StatusTornTail {
		t.Fatalf("status = %v, want torn-tail", info.Status)
	}
	if len(applied) != 2 {
		t.Fatalf("recovered %d records, want 2", len(applied))
	}
}
