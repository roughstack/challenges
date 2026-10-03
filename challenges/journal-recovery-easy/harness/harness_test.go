package harness

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/roughstack/challenges/challenges/journal-recovery-easy/contract"
	"github.com/roughstack/challenges/challenges/journal-recovery-easy/starter"
)

func TestEvaluateIsDeterministicAndRecoversTornTail(t *testing.T) {
	config := DefaultPublicConfig()
	first := Evaluate(^uint64(0), config, starter.OpenJournal)
	second := Evaluate(^uint64(0), config, starter.OpenJournal)

	if !reflect.DeepEqual(first, second) {
		t.Fatalf("same seed produced different results:\n%+v\n%+v", first, second)
	}
	if first.Verdict != "pass" || len(first.Violations) != 0 {
		t.Fatalf("unexpected verdict: %+v", first)
	}
	if first.Metrics.AppendThroughput == 0 {
		t.Fatal("append throughput was not measured")
	}
	if first.Metrics.RecoveryBytesRead == 0 {
		t.Fatal("recovery bytes read were not measured")
	}
	if first.Metrics.WriteOverhead == 0 {
		t.Fatal("write overhead was not measured")
	}
	if first.Metrics.Allocations == 0 {
		t.Fatal("allocations were not measured")
	}
	if first.Metrics.DiskBytes == 0 {
		t.Fatal("disk bytes were not measured")
	}
}

func TestStarterPassesEveryPublicFaultProfile(t *testing.T) {
	profiles := []FaultProfile{
		FaultClean,
		FaultEmpty,
		FaultPartialHeader,
		FaultTornTail,
		FaultTailGarbage,
		FaultInteriorBitFlip,
		FaultLengthBomb,
		FaultSequenceGap,
		FaultDuplicateLSN,
	}
	for _, fault := range profiles {
		config := DefaultPublicConfig()
		config.Fault = fault
		result := Evaluate(7, config, starter.OpenJournal)
		if result.Verdict != "pass" {
			t.Fatalf("fault %s failed: %+v", fault, result.Violations)
		}
	}
}

func TestAllocationsCountsAllDeviceReadAndWriteCalls(t *testing.T) {
	result := Evaluate(16, DefaultPublicConfig(), starter.OpenJournal)
	if result.Metrics.Allocations != 97 {
		t.Fatalf("allocations = %d, want 97 (96 append writes + 1 recovery read)", result.Metrics.Allocations)
	}
}

func TestResultEnvelopeContainsEveryPublicMetric(t *testing.T) {
	result := Evaluate(7, DefaultPublicConfig(), starter.OpenJournal)
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
	for _, metric := range []string{"append_throughput", "recovery_bytes_read", "write_overhead", "allocations", "disk_bytes"} {
		if _, ok := metrics[metric]; !ok {
			t.Fatalf("result is missing metric %q", metric)
		}
	}
	for _, field := range []string{"protocol_version", "arena_id", "arena_version", "seed", "verdict", "score", "run"} {
		if _, ok := envelope[field]; !ok {
			t.Fatalf("result is missing field %q", field)
		}
	}
	if envelope["seed"] != "7" {
		t.Fatalf("seed not preserved as a decimal string: %v", envelope["seed"])
	}
}

// delegatingRecover keeps the starter's append path but replaces Recover.
type delegatingRecover struct {
	inner   contract.Journal
	recover func(apply func(contract.Record) error) (contract.RecoveryInfo, error)
}

func (d *delegatingRecover) Append(record contract.Record) (uint64, error) {
	return d.inner.Append(record)
}
func (d *delegatingRecover) Sync(upto uint64) error { return d.inner.Sync(upto) }
func (d *delegatingRecover) Close() error           { return d.inner.Close() }
func (d *delegatingRecover) Recover(apply func(contract.Record) error) (contract.RecoveryInfo, error) {
	return d.recover(apply)
}

func fixedRecoverFactory(info contract.RecoveryInfo, err error) contract.Factory {
	return func(_ string, device contract.Device) (contract.Journal, error) {
		inner, openErr := starter.OpenJournal("", device)
		if openErr != nil {
			return nil, openErr
		}
		return &delegatingRecover{inner: inner, recover: func(func(contract.Record) error) (contract.RecoveryInfo, error) {
			return info, err
		}}, nil
	}
}

// wrongRecoverErrorFactory keeps the starter's real recovery behavior but
// replaces its returned error, so the harness can gate error identity without
// any other recovery mismatch.
func wrongRecoverErrorFactory(wrong error) contract.Factory {
	return func(_ string, device contract.Device) (contract.Journal, error) {
		inner, openErr := starter.OpenJournal("", device)
		if openErr != nil {
			return nil, openErr
		}
		return &delegatingRecover{inner: inner, recover: func(apply func(contract.Record) error) (contract.RecoveryInfo, error) {
			info, _ := inner.Recover(apply)
			return info, wrong
		}}, nil
	}
}

func cleanInfo() contract.RecoveryInfo {
	return contract.RecoveryInfo{Status: contract.StatusClean}
}

func TestEmptyFileRecoveryIsGated(t *testing.T) {
	config := DefaultPublicConfig()
	config.Fault = FaultEmpty
	result := Evaluate(1, config, fixedRecoverFactory(cleanInfo(), nil))
	if result.Verdict != "fail" {
		t.Fatalf("verdict = %q, want fail; violations: %v", result.Verdict, result.Violations)
	}
	if !containsViolation(result.Violations, "recovery reported status clean, want empty") {
		t.Fatalf("violations = %v, want the status mismatch", result.Violations)
	}
}

func TestPartialHeaderRecoveryIsGated(t *testing.T) {
	config := DefaultPublicConfig()
	config.Fault = FaultPartialHeader
	result := Evaluate(2, config, fixedRecoverFactory(cleanInfo(), nil))
	if result.Verdict != "fail" {
		t.Fatalf("verdict = %q, want fail; violations: %v", result.Verdict, result.Violations)
	}
	if !containsViolation(result.Violations, "recovery reported status clean, want torn-tail") {
		t.Fatalf("violations = %v, want the torn-tail status mismatch", result.Violations)
	}
}

func TestSequenceGapRecoveryIsGated(t *testing.T) {
	config := DefaultPublicConfig()
	config.Fault = FaultSequenceGap
	result := Evaluate(3, config, fixedRecoverFactory(cleanInfo(), nil))
	if result.Verdict != "fail" {
		t.Fatalf("verdict = %q, want fail; violations: %v", result.Verdict, result.Violations)
	}
	if !containsViolation(result.Violations, "recovery did not report a corrupt journal") {
		t.Fatalf("violations = %v, want the corrupt-status violation", result.Violations)
	}
}

func TestDuplicateLSNRecoveryIsGated(t *testing.T) {
	config := DefaultPublicConfig()
	config.Fault = FaultDuplicateLSN
	result := Evaluate(4, config, fixedRecoverFactory(cleanInfo(), nil))
	if result.Verdict != "fail" {
		t.Fatalf("verdict = %q, want fail; violations: %v", result.Verdict, result.Violations)
	}
	if !containsViolation(result.Violations, "recovery error presence does not match the journal state") {
		t.Fatalf("violations = %v, want the error-presence violation", result.Violations)
	}
}

func TestApplyCallbackFailurePropagatesThroughStarter(t *testing.T) {
	config := DefaultPublicConfig()
	config.FailApplyAt = 2
	result := Evaluate(5, config, starter.OpenJournal)
	if result.Verdict != "pass" {
		t.Fatalf("starter did not propagate the callback failure: %+v", result.Violations)
	}
}

func TestInteriorBitFlipWithFailApplyAtPastPrefixPasses(t *testing.T) {
	config := DefaultPublicConfig()
	config.Fault = FaultInteriorBitFlip
	config.FailApplyAt = 2
	result := Evaluate(11, config, starter.OpenJournal)
	if result.Verdict != "pass" || len(result.Violations) != 0 {
		t.Fatalf("interior bit flip with FailApplyAt past the valid prefix false-failed: %+v", result)
	}
}

func swallowCallbackFactory() contract.Factory {
	return func(_ string, device contract.Device) (contract.Journal, error) {
		inner, openErr := starter.OpenJournal("", device)
		if openErr != nil {
			return nil, openErr
		}
		return &delegatingRecover{inner: inner, recover: func(apply func(contract.Record) error) (contract.RecoveryInfo, error) {
			info, _ := inner.Recover(func(record contract.Record) error {
				if apply != nil {
					_ = apply(record)
				}
				return nil
			})
			return info, nil
		}}, nil
	}
}

func TestSwallowedApplyCallbackFailureIsGated(t *testing.T) {
	config := DefaultPublicConfig()
	config.FailApplyAt = 2
	result := Evaluate(6, config, swallowCallbackFactory())
	if result.Verdict != "fail" {
		t.Fatalf("verdict = %q, want fail; violations: %v", result.Verdict, result.Violations)
	}
	if !containsViolation(result.Violations, "recovery swallowed the apply callback error") {
		t.Fatalf("violations = %v, want the swallowed-callback violation", result.Violations)
	}
}

func TestWrongApplyCallbackErrorIsGated(t *testing.T) {
	config := DefaultPublicConfig()
	config.FailApplyAt = 2
	wrong := errors.New("hostile: different apply error")
	result := Evaluate(12, config, wrongRecoverErrorFactory(wrong))
	if result.Verdict != "fail" {
		t.Fatalf("verdict = %q, want fail; violations: %v", result.Verdict, result.Violations)
	}
	if !containsViolation(result.Violations, "recovery did not propagate the apply callback error") {
		t.Fatalf("violations = %v, want the wrong-apply-error violation", result.Violations)
	}
}

func TestWrongCorruptionErrorIsGated(t *testing.T) {
	config := DefaultPublicConfig()
	config.Fault = FaultInteriorBitFlip
	wrong := errors.New("hostile: different corruption error")
	result := Evaluate(13, config, wrongRecoverErrorFactory(wrong))
	if result.Verdict != "fail" {
		t.Fatalf("verdict = %q, want fail; violations: %v", result.Verdict, result.Violations)
	}
	if !containsViolation(result.Violations, "recovery returned the wrong error") {
		t.Fatalf("violations = %v, want the wrong-corruption-error violation", result.Violations)
	}
}

func TestWrongCorruptionErrorWithFailApplyAtPastPrefixIsGated(t *testing.T) {
	config := DefaultPublicConfig()
	config.Fault = FaultInteriorBitFlip
	config.FailApplyAt = 2
	wrong := errors.New("hostile: different corruption error")
	result := Evaluate(14, config, wrongRecoverErrorFactory(wrong))
	if result.Verdict != "fail" {
		t.Fatalf("verdict = %q, want fail; violations: %v", result.Verdict, result.Violations)
	}
	if !containsViolation(result.Violations, "recovery returned the wrong error") {
		t.Fatalf("violations = %v, want the wrong-corruption-error violation", result.Violations)
	}
}

type panickingJournal struct{}

func (panickingJournal) Append(contract.Record) (uint64, error) { panic("boom") }
func (panickingJournal) Sync(uint64) error                      { return nil }
func (panickingJournal) Recover(func(contract.Record) error) (contract.RecoveryInfo, error) {
	return contract.RecoveryInfo{}, nil
}
func (panickingJournal) Close() error { return nil }

// deferringAppendJournal returns correct LSNs but buffers every frame in
// memory, writing them to the device only during Sync. Append is therefore in
// violation of the per-Append device write contract.
type deferringAppendJournal struct {
	device  contract.Device
	nextLSN uint64
	pending [][]byte
}

func (j *deferringAppendJournal) Append(record contract.Record) (uint64, error) {
	j.nextLSN++
	j.pending = append(j.pending, contract.EncodeRecord(contract.Record{LSN: j.nextLSN, Payload: record.Payload}))
	return j.nextLSN, nil
}

func (j *deferringAppendJournal) Sync(upto uint64) error {
	if upto > j.nextLSN {
		return contract.ErrUnknownLSN
	}
	size, err := j.device.Size()
	if err != nil {
		return err
	}
	for i := uint64(0); i < upto; i++ {
		if err := j.device.Write(size, j.pending[i]); err != nil {
			return err
		}
		size += uint64(len(j.pending[i]))
	}
	if err := j.device.Sync(); err != nil {
		return err
	}
	j.pending = append([][]byte(nil), j.pending[upto:]...)
	return nil
}

func (j *deferringAppendJournal) Recover(apply func(contract.Record) error) (contract.RecoveryInfo, error) {
	inner, err := starter.OpenJournal("", j.device)
	if err != nil {
		return contract.RecoveryInfo{}, err
	}
	return inner.Recover(apply)
}

func (j *deferringAppendJournal) Close() error { return nil }

func deferringAppendFactory() contract.Factory {
	return func(_ string, device contract.Device) (contract.Journal, error) {
		return &deferringAppendJournal{device: device}, nil
	}
}

func TestAppendMustWriteBeforeReturn(t *testing.T) {
	result := Evaluate(15, DefaultPublicConfig(), deferringAppendFactory())
	if result.Verdict != "fail" {
		t.Fatalf("verdict = %q, want fail; violations: %v", result.Verdict, result.Violations)
	}
	if !containsViolation(result.Violations, "Append did not write record 0 to the device") {
		t.Fatalf("violations = %v, want the per-Append device write violation", result.Violations)
	}
}

func TestPanickingJournalIsIsolated(t *testing.T) {
	factory := func(_ string, _ contract.Device) (contract.Journal, error) {
		return panickingJournal{}, nil
	}
	result := Evaluate(8, DefaultPublicConfig(), factory)
	if result.Verdict != "fail" {
		t.Fatalf("verdict = %q, want fail; violations: %v", result.Verdict, result.Violations)
	}
	if !containsViolation(result.Violations, "contestant journal panicked") {
		t.Fatalf("violations = %v, want the panic violation", result.Violations)
	}
	if result.ProtocolVersion != 1 || result.ArenaID != ArenaID {
		t.Fatalf("result envelope was not returned intact: %+v", result)
	}
}

func TestNilJournalIsRejected(t *testing.T) {
	factory := func(_ string, _ contract.Device) (contract.Journal, error) {
		return nil, nil
	}
	result := Evaluate(9, DefaultPublicConfig(), factory)
	if result.Verdict != "fail" {
		t.Fatalf("verdict = %q, want fail; violations: %v", result.Verdict, result.Violations)
	}
	if !containsViolation(result.Violations, "OpenJournal returned a nil journal") {
		t.Fatalf("violations = %v, want the nil-journal violation", result.Violations)
	}
}

func containsViolation(violations []string, want string) bool {
	for _, violation := range violations {
		if violation == want {
			return true
		}
	}
	return false
}
