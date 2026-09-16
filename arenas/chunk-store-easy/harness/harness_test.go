package harness

import (
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"testing"

	"github.com/bytearena/arenas/arenas/chunk-store-easy/contract"
	"github.com/bytearena/arenas/arenas/chunk-store-easy/starter"
)

func TestEvaluateIsDeterministicAndPasses(t *testing.T) {
	config := DefaultPublicConfig()
	first := Evaluate(^uint64(0), config, starter.OpenStore)
	second := Evaluate(^uint64(0), config, starter.OpenStore)

	if !reflect.DeepEqual(first, second) {
		t.Fatalf("same seed produced different results:\n%+v\n%+v", first, second)
	}
	if first.Verdict != "pass" || len(first.Violations) != 0 {
		t.Fatalf("unexpected verdict: %+v", first)
	}
	if first.Metrics.StoredBytes == 0 {
		t.Fatal("stored bytes were not measured")
	}
	if first.Metrics.RandomReadLogicalBytes == 0 {
		t.Fatal("random read logical bytes were not measured")
	}
	if first.Metrics.WriteCPUWork == 0 {
		t.Fatal("write CPU work was not measured")
	}
	if first.Metrics.ReadCPUWork == 0 {
		t.Fatal("read CPU work was not measured")
	}
	if first.Metrics.IndexMemoryBytes == 0 {
		t.Fatal("index memory was not measured")
	}
}

func TestResultEnvelopeContainsEveryPublicMetric(t *testing.T) {
	result := Evaluate(7, DefaultPublicConfig(), starter.OpenStore)
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
	for _, metric := range []string{"stored_bytes", "random_read_logical_bytes", "write_cpu_work", "read_cpu_work", "index_memory_bytes"} {
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

func TestChecksumCorruptionIsGated(t *testing.T) {
	config := DefaultPublicConfig()
	config.Fault = FaultChecksumCorruption
	result := Evaluate(11, config, hostileSwallowCorruptionFactory())
	if result.Verdict != "fail" {
		t.Fatalf("verdict = %q, want fail; violations: %v", result.Verdict, result.Violations)
	}
	if !containsViolation(result.Violations, "read of a corrupt blob did not return a corruption error") {
		t.Fatalf("violations = %v, want the corrupt-read violation", result.Violations)
	}
}

func TestCorruptLengthIsGated(t *testing.T) {
	config := DefaultPublicConfig()
	config.Fault = FaultCorruptLength
	result := Evaluate(12, config, hostileSwallowCorruptionFactory())
	if result.Verdict != "fail" {
		t.Fatalf("verdict = %q, want fail; violations: %v", result.Verdict, result.Violations)
	}
	if !containsViolation(result.Violations, "read of a corrupt blob did not return a corruption error") {
		t.Fatalf("violations = %v, want the corrupt-read violation", result.Violations)
	}
}

type hostileRangeStore struct {
	contract.Store
	sizes map[[2]uint64]int
}

func (h *hostileRangeStore) Put(id, version uint64, data io.Reader, size uint64) error {
	err := h.Store.Put(id, version, data, size)
	if err == nil {
		h.sizes[[2]uint64{id, version}] = int(size)
	}
	return err
}

func (h *hostileRangeStore) ReadAt(id, version uint64, p []byte, off int64) (int, error) {
	key := [2]uint64{id, version}
	if size, ok := h.sizes[key]; ok && off >= int64(size) {
		for index := range p {
			p[index] = 0xaa
		}
		return len(p), nil
	}
	return h.Store.ReadAt(id, version, p, off)
}

func hostileRangeFactory() contract.Factory {
	return func(_ string, device contract.Device, memoryLimit uint64) (contract.Store, error) {
		inner, err := starter.OpenStore("", device, memoryLimit)
		if err != nil {
			return nil, err
		}
		return &hostileRangeStore{Store: inner, sizes: make(map[[2]uint64]int)}, nil
	}
}

func TestOutOfRangeReadIsGated(t *testing.T) {
	result := Evaluate(13, DefaultPublicConfig(), hostileRangeFactory())
	if result.Verdict != "fail" {
		t.Fatalf("verdict = %q, want fail; violations: %v", result.Verdict, result.Violations)
	}
	if !containsViolation(result.Violations, "out-of-range read did not return io.EOF") {
		t.Fatalf("violations = %v, want the out-of-range violation", result.Violations)
	}
}

type hostileReadAllStore struct {
	contract.Store
	device contract.Device
}

func (h *hostileReadAllStore) ReadAt(id, version uint64, p []byte, off int64) (int, error) {
	n, err := h.Store.ReadAt(id, version, p, off)
	if size, sizeErr := h.device.Size(); sizeErr == nil && size > 0 {
		_, _ = h.device.Read(0, size)
	}
	return n, err
}

func hostileReadAllFactory() contract.Factory {
	return func(_ string, device contract.Device, memoryLimit uint64) (contract.Store, error) {
		inner, err := starter.OpenStore("", device, memoryLimit)
		if err != nil {
			return nil, err
		}
		return &hostileReadAllStore{Store: inner, device: device}, nil
	}
}

func TestReadingUnrelatedBlobsIsGated(t *testing.T) {
	result := Evaluate(14, DefaultPublicConfig(), hostileReadAllFactory())
	if result.Verdict != "fail" {
		t.Fatalf("verdict = %q, want fail; violations: %v", result.Verdict, result.Violations)
	}
	if !containsViolation(result.Violations, "read decoded unrelated blobs") {
		t.Fatalf("violations = %v, want the unrelated-blob violation", result.Violations)
	}
}

var allocSink []byte

type hostileAllocStore struct {
	contract.Store
}

func (h *hostileAllocStore) ReadAt(id, version uint64, p []byte, off int64) (int, error) {
	n, err := h.Store.ReadAt(id, version, p, off)
	if errors.Is(err, contract.ErrCorrupt) || errors.Is(err, contract.ErrLengthBomb) {
		allocSink = make([]byte, 8<<20)
	}
	return n, err
}

func hostileAllocFactory() contract.Factory {
	return func(_ string, device contract.Device, memoryLimit uint64) (contract.Store, error) {
		inner, err := starter.OpenStore("", device, memoryLimit)
		if err != nil {
			return nil, err
		}
		return &hostileAllocStore{Store: inner}, nil
	}
}

func TestOverAllocationOnCorruptInputIsGated(t *testing.T) {
	config := DefaultPublicConfig()
	config.Fault = FaultCorruptLength
	result := Evaluate(15, config, hostileAllocFactory())
	if result.Verdict != "fail" {
		t.Fatalf("verdict = %q, want fail; violations: %v", result.Verdict, result.Violations)
	}
	if !containsViolation(result.Violations, "read over-allocated on corrupt input") {
		t.Fatalf("violations = %v, want the over-allocation violation", result.Violations)
	}
}

type hostileSwallowCorruptionStore struct {
	contract.Store
}

func (h *hostileSwallowCorruptionStore) ReadAt(id, version uint64, p []byte, off int64) (int, error) {
	n, err := h.Store.ReadAt(id, version, p, off)
	if errors.Is(err, contract.ErrCorrupt) || errors.Is(err, contract.ErrLengthBomb) {
		for index := range p {
			p[index] = 0xcc
		}
		return len(p), nil
	}
	return n, err
}

func hostileSwallowCorruptionFactory() contract.Factory {
	return func(_ string, device contract.Device, memoryLimit uint64) (contract.Store, error) {
		inner, err := starter.OpenStore("", device, memoryLimit)
		if err != nil {
			return nil, err
		}
		return &hostileSwallowCorruptionStore{Store: inner}, nil
	}
}

type hostileTruncatingDeleteStore struct {
	contract.Store
	device     contract.Device
	beforeSize uint64
}

func (h *hostileTruncatingDeleteStore) Delete(id, version uint64) error {
	if h.beforeSize == 0 {
		if size, err := h.device.Size(); err == nil {
			h.beforeSize = size
		}
	}
	_ = h.device.Truncate(1)
	return h.Store.Delete(id, version)
}

func TestDeleteReclaimsDeviceBytesIsGated(t *testing.T) {
	var hostile *hostileTruncatingDeleteStore
	factory := func(_ string, device contract.Device, memoryLimit uint64) (contract.Store, error) {
		inner, err := starter.OpenStore("", device, memoryLimit)
		if err != nil {
			return nil, err
		}
		hostile = &hostileTruncatingDeleteStore{Store: inner, device: device}
		return hostile, nil
	}

	result := Evaluate(16, DefaultPublicConfig(), factory)
	if result.Verdict != "fail" {
		t.Fatalf("verdict = %q, want fail; violations: %v", result.Verdict, result.Violations)
	}
	if !containsViolation(result.Violations, "Delete reclaimed device bytes") {
		t.Fatalf("violations = %v, want the delete-reclaim violation", result.Violations)
	}
	if hostile == nil || hostile.beforeSize == 0 {
		t.Fatal("hostile delete store never ran")
	}
	if result.Metrics.StoredBytes != hostile.beforeSize {
		t.Fatalf("StoredBytes = %d, want post-write size %d (not lowered by truncating Delete)", result.Metrics.StoredBytes, hostile.beforeSize)
	}
}

func TestScanDeviceRejectsNonUniformBlockFraming(t *testing.T) {
	size := contract.BlockSize + 1
	rawLens := []uint32{uint32(contract.BlockSize - 1), 2}
	data := contract.EncodeBlobHeader(1, 1, size, 2)
	for _, rawLen := range rawLens {
		payload := make([]byte, rawLen)
		data = append(data, contract.EncodeBlockRecord(rawLen, rawLen, payload)...)
	}

	if _, err := scanDevice(&memDevice{data: data}); !errors.Is(err, contract.ErrCorrupt) {
		t.Fatalf("scanDevice error = %v, want ErrCorrupt", err)
	}
}

func TestUnknownFaultProfileRunsClean(t *testing.T) {
	config := DefaultPublicConfig()
	config.Fault = FaultProfile("typo-profile")
	result := Evaluate(21, config, starter.OpenStore)
	if result.Verdict != "pass" || len(result.Violations) != 0 {
		t.Fatalf("verdict = %q, violations = %v, want clean pass", result.Verdict, result.Violations)
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
