package workload

import (
	"reflect"
	"testing"
)

func TestGenerateIsDeterministic(t *testing.T) {
	cfg := Config{Seed: ^uint64(0), Operations: 600, KeySpace: 64, ScanLength: 8}
	first := Generate(cfg)
	second := Generate(cfg)
	if !reflect.DeepEqual(first, second) {
		t.Fatal("same uint64 seed produced different operations")
	}

	other := Generate(Config{Seed: 99, Operations: 600, KeySpace: 64, ScanLength: 8})
	if reflect.DeepEqual(first, other) {
		t.Fatal("different seeds produced identical operations")
	}
}

func TestGenerateCoversEveryPublicKind(t *testing.T) {
	operations := Generate(Config{Seed: 7, Operations: 700, KeySpace: 64, ScanLength: 8})
	seen := map[Kind]bool{}
	for _, operation := range operations {
		seen[operation.Kind] = true
	}
	for _, kind := range []Kind{Write, Read, Delete, Scan, Snapshot} {
		if !seen[kind] {
			t.Fatalf("operation kind %d was not generated", kind)
		}
	}
}

func TestGenerateKeepsKeysAndScansInBounds(t *testing.T) {
	operations := Generate(Config{Seed: 123, Operations: 900, KeySpace: 128, ScanLength: 32})
	for _, operation := range operations {
		if operation.Key >= 128 {
			t.Fatalf("key %d outside configured key space", operation.Key)
		}
		if operation.Kind == Scan {
			if operation.Length < 0 {
				t.Fatalf("negative scan length: %+v", operation)
			}
			if uint64(operation.Key)+uint64(operation.Length) > 128 {
				t.Fatalf("scan exceeds key space: %+v", operation)
			}
		}
	}
}

func TestGenerateHandlesEmptyConfig(t *testing.T) {
	if operations := Generate(Config{}); operations != nil {
		t.Fatalf("empty config produced %d operations", len(operations))
	}
}
