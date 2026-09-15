package workload

import (
	"reflect"
	"testing"

	"github.com/bytearena/arenas/arenas/cache-pressure-medium/contract"
)

func TestGenerateIsDeterministicAndCoversPhases(t *testing.T) {
	config := Config{Seed: ^uint64(0), Operations: 300, HotKeys: 8, ScanKeys: 24, CapacityBytes: 24 * 128}
	first := Generate(config)
	second := Generate(config)
	if !reflect.DeepEqual(first, second) {
		t.Fatal("same uint64 seed produced different operations")
	}

	seen := map[Kind]bool{}
	for _, operation := range first {
		seen[operation.Kind] = true
	}
	for _, kind := range []Kind{Read, Write, Delete} {
		if !seen[kind] {
			t.Fatalf("operation kind %d was not generated", kind)
		}
	}
}

func TestGenerateContainsCacheSizedScan(t *testing.T) {
	scanEntryBytes := ScanValueBytes + contract.PerEntryMetadataBytes
	config := Config{Seed: 7, Operations: 400, HotKeys: 8, ScanKeys: 64, CapacityBytes: 64 * scanEntryBytes}
	operations := Generate(config)

	scanStart := (config.Operations - int(config.ScanKeys)) / 2
	scanEnd := scanStart + int(config.ScanKeys)

	seen := map[uint64]bool{}
	scanBytes := uint64(0)
	for index := scanStart; index < scanEnd; index++ {
		op := operations[index]
		if op.Kind != Read {
			t.Fatalf("scan op %d has kind %d, want Read", index, op.Kind)
		}
		if op.Key < config.HotKeys || op.Key >= config.HotKeys+config.ScanKeys {
			t.Fatalf("scan op %d key %d is not a cold scan key", index, op.Key)
		}
		if seen[op.Key] {
			t.Fatalf("scan key %d repeated within one pass", op.Key)
		}
		seen[op.Key] = true
		scanBytes += op.SizeBytes + contract.PerEntryMetadataBytes
	}
	if scanBytes < config.CapacityBytes {
		t.Fatalf("scan accounted bytes = %d, want >= capacity %d", scanBytes, config.CapacityBytes)
	}
}

func TestGenerateDerivedScanSizingCoversCapacityWithSmallestCount(t *testing.T) {
	scanEntryBytes := ScanValueBytes + contract.PerEntryMetadataBytes
	capacity := 10*scanEntryBytes + 1
	config := Config{Seed: 11, Operations: 100, HotKeys: 8, ScanKeys: 0, CapacityBytes: capacity}
	operations := Generate(config)

	derived := (capacity + scanEntryBytes - 1) / scanEntryBytes
	if (derived-1)*scanEntryBytes >= capacity {
		t.Fatalf("derived scan count %d is not the smallest covering count for capacity %d", derived, capacity)
	}

	scanStart := (config.Operations - int(derived)) / 2
	scanEnd := scanStart + int(derived)
	scanBytes := uint64(0)
	seen := map[uint64]bool{}
	for index := scanStart; index < scanEnd; index++ {
		op := operations[index]
		if op.Kind != Read {
			t.Fatalf("scan op %d has kind %d, want Read", index, op.Kind)
		}
		if op.Key < config.HotKeys || op.Key >= config.HotKeys+derived {
			t.Fatalf("scan op %d key %d is not a cold scan key", index, op.Key)
		}
		if seen[op.Key] {
			t.Fatalf("scan key %d repeated within one pass", op.Key)
		}
		seen[op.Key] = true
		scanBytes += op.SizeBytes + contract.PerEntryMetadataBytes
	}
	if scanBytes < capacity {
		t.Fatalf("derived scan accounted bytes = %d, want >= capacity %d", scanBytes, capacity)
	}
}

func TestGenerateExplicitScanKeysOverrideWins(t *testing.T) {
	scanEntryBytes := ScanValueBytes + contract.PerEntryMetadataBytes
	capacity := 1000 * scanEntryBytes
	config := Config{Seed: 13, Operations: 100, HotKeys: 8, ScanKeys: 3, CapacityBytes: capacity}
	operations := Generate(config)

	scanStart := (config.Operations - int(config.ScanKeys)) / 2
	scanEnd := scanStart + int(config.ScanKeys)
	scanBytes := uint64(0)
	for index := scanStart; index < scanEnd; index++ {
		op := operations[index]
		if op.Kind != Read {
			t.Fatalf("scan op %d has kind %d, want Read", index, op.Kind)
		}
		scanBytes += op.SizeBytes + contract.PerEntryMetadataBytes
	}
	if scanBytes >= capacity {
		t.Fatalf("explicit ScanKeys override should not be replaced by capacity-derived sizing: %d bytes >= capacity %d", scanBytes, capacity)
	}
	if scanBytes != config.ScanKeys*scanEntryBytes {
		t.Fatalf("explicit ScanKeys accounted bytes = %d, want %d", scanBytes, config.ScanKeys*scanEntryBytes)
	}
}

func TestGenerateHandlesEmptyConfig(t *testing.T) {
	if operations := Generate(Config{}); operations != nil {
		t.Fatalf("empty config produced %d operations", len(operations))
	}
}

func TestKeyMixIsDeterministicAndSaltSensitive(t *testing.T) {
	a := KeyMix(1, 2, 3)
	if b := KeyMix(1, 2, 3); a != b {
		t.Fatal("KeyMix is not deterministic")
	}
	if KeyMix(1, 2, 3) == KeyMix(1, 2, 4) {
		t.Fatal("KeyMix is not salt-sensitive")
	}
}
