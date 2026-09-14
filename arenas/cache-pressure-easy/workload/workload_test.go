package workload

import (
	"reflect"
	"testing"
)

func TestGenerateIsDeterministicAndCoversPhases(t *testing.T) {
	config := Config{Seed: ^uint64(0), Operations: 300, HotKeys: 8, ScanKeys: 24}
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

func TestGenerateHandlesEmptyConfig(t *testing.T) {
	if operations := Generate(Config{}); operations != nil {
		t.Fatalf("empty config produced %d operations", len(operations))
	}
}
