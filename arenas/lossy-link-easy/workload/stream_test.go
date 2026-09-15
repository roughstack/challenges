package workload

import (
	"bytes"
	"testing"
)

func TestGenerateIsDeterministicAndLengthBounded(t *testing.T) {
	first := Generate(^uint64(0), 4096)
	second := Generate(^uint64(0), 4096)
	if !bytes.Equal(first, second) {
		t.Fatal("same uint64 seed produced different streams")
	}
	if len(first) != 4096 {
		t.Fatalf("stream length = %d, want 4096", len(first))
	}

	other := Generate(1, 4096)
	if bytes.Equal(first, other) {
		t.Fatal("different seeds produced identical streams")
	}
}

func TestGenerateHandlesEmptyStream(t *testing.T) {
	if stream := Generate(42, 0); len(stream) != 0 {
		t.Fatalf("empty request produced %d bytes", len(stream))
	}
}
