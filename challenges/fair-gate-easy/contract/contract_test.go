package contract

import "testing"

func TestConfigNormalizedDefaultsRateDen(t *testing.T) {
	cfg := Config{RateNum: 2, RateDen: 0, Burst: 5}
	normalized := cfg.Normalized()
	if normalized.RateDen != DefaultRateDen {
		t.Fatalf("normalized RateDen = %d, want %d", normalized.RateDen, DefaultRateDen)
	}
	if cfg.RateDen != 0 {
		t.Fatal("Normalized mutated the receiver")
	}
	if got := cfg.RateDenEffective(); got != DefaultRateDen {
		t.Fatalf("RateDenEffective = %d, want %d", got, DefaultRateDen)
	}
	if got := (Config{RateNum: 1, RateDen: 7, Burst: 5}).RateDenEffective(); got != 7 {
		t.Fatalf("RateDenEffective = %d, want 7", got)
	}
}

func TestCapacityUnits(t *testing.T) {
	if units, ok := (Config{RateNum: 1, RateDen: 4, Burst: 5}).CapacityUnits(); !ok || units != 20 {
		t.Fatalf("CapacityUnits = %d, %v; want 20, true", units, ok)
	}
	if units, ok := (Config{RateNum: 1, RateDen: 1, Burst: 0}).CapacityUnits(); !ok || units != 0 {
		t.Fatalf("zero burst CapacityUnits = %d, %v; want 0, true", units, ok)
	}
	if _, ok := (Config{RateNum: 1, RateDen: ^uint64(0), Burst: 2}).CapacityUnits(); ok {
		t.Fatal("overflowing capacity product reported ok")
	}
}
