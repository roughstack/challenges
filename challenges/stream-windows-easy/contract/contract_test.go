package contract

import "testing"

func TestConfigWindowSizeEffective(t *testing.T) {
	if got := (Config{WindowSize: 0}).WindowSizeEffective(); got != DefaultWindowSize {
		t.Fatalf("zero window size resolved to %d, want %d", got, DefaultWindowSize)
	}
	if got := (Config{WindowSize: 77}).WindowSizeEffective(); got != 77 {
		t.Fatalf("custom window size resolved to %d, want 77", got)
	}
}

func TestMaxLegalTickKeepsWindowEndInUint64(t *testing.T) {
	const windowSize uint64 = 10
	max := MaxLegalTick(windowSize)
	if max != ^uint64(0)-(^uint64(0)%windowSize)-1 {
		t.Fatalf("MaxLegalTick(%d) = %d, want formula value", windowSize, max)
	}

	start := (max / windowSize) * windowSize
	if start > ^uint64(0)-windowSize {
		t.Fatalf("window containing MaxLegalTick overflows uint64")
	}

	// The tick one past the maximum legal tick must belong to a window whose
	// end no longer fits in uint64.
	next := max + 1
	nextStart := (next / windowSize) * windowSize
	if nextStart <= ^uint64(0)-windowSize {
		t.Fatalf("tick after MaxLegalTick unexpectedly fits")
	}
}
