package contract

import "testing"

func TestExpiryTickSaturatesOnOverflow(t *testing.T) {
	max := ^uint64(0)
	if got := ExpiryTick(max-5, 10); got != max {
		t.Fatalf("ExpiryTick(max-5, 10) = %d, want %d", got, max)
	}
	if got := ExpiryTick(max, max); got != max {
		t.Fatalf("ExpiryTick(max, max) = %d, want %d", got, max)
	}
}

func TestExpiryTickZeroTTLNeverExpires(t *testing.T) {
	max := ^uint64(0)
	if got := ExpiryTick(0, 0); got != max {
		t.Fatalf("ExpiryTick(0, 0) = %d, want %d", got, max)
	}
	if got := ExpiryTick(10, 0); got != max {
		t.Fatalf("ExpiryTick(10, 0) = %d, want %d", got, max)
	}
}

func TestExpiryTickExactBoundary(t *testing.T) {
	if got := ExpiryTick(0, 20); got != 20 {
		t.Fatalf("ExpiryTick(0, 20) = %d, want 20", got)
	}
	if got := ExpiryTick(19, 1); got != 20 {
		t.Fatalf("ExpiryTick(19, 1) = %d, want 20", got)
	}
}

func TestAccountedBytesIncludesMetadata(t *testing.T) {
	if got := AccountedBytes([]byte("abc")); got != 3+PerEntryMetadataBytes {
		t.Fatalf("AccountedBytes(abc) = %d, want %d", got, 3+PerEntryMetadataBytes)
	}
}
