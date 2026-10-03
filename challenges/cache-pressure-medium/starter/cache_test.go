package starter

import (
	"bytes"
	"math"
	"testing"

	"github.com/roughstack/challenges/challenges/cache-pressure-medium/contract"
)

func TestByteCapacityEvictsToStayWithinBudget(t *testing.T) {
	small := []byte("123456")  // 6 bytes, accounted 38
	smaller := []byte("12345") // 5 bytes, accounted 37
	capacity := contract.AccountedBytes(small)

	cache := NewCache(capacity)
	cache.Put(1, small, contract.RequestMeta{NowTick: 1})
	cache.Put(2, smaller, contract.RequestMeta{NowTick: 2})

	if used := cache.UsedBytes(); used > capacity {
		t.Fatalf("used bytes = %d exceeds capacity %d", used, capacity)
	}

	resident := 0
	if _, ok := cache.Get(1, contract.RequestMeta{NowTick: 3}); ok {
		resident++
	}
	if _, ok := cache.Get(2, contract.RequestMeta{NowTick: 3}); ok {
		resident++
	}
	if resident != 1 {
		t.Fatalf("expected exactly one resident entry after eviction, got %d", resident)
	}
}

func TestOversizedObjectRejectedWithoutEviction(t *testing.T) {
	cache := NewCache(40)
	cache.Put(1, []byte("hello"), contract.RequestMeta{NowTick: 1}) // accounted 37

	cache.Put(2, make([]byte, 20), contract.RequestMeta{NowTick: 2}) // accounted 52 > 40

	if _, ok := cache.Get(2, contract.RequestMeta{NowTick: 3}); ok {
		t.Fatal("oversized object was stored")
	}
	if value, ok := cache.Get(1, contract.RequestMeta{NowTick: 3}); !ok || string(value) != "hello" {
		t.Fatal("oversized insert evicted or altered an existing entry")
	}
	if used := cache.UsedBytes(); used != contract.AccountedBytes([]byte("hello")) {
		t.Fatalf("used bytes = %d, want %d", used, contract.AccountedBytes([]byte("hello")))
	}
}

func TestOversizedReplacementLeavesOldValue(t *testing.T) {
	cache := NewCache(40)
	cache.Put(1, []byte("hello"), contract.RequestMeta{NowTick: 1}) // accounted 37

	cache.Put(1, make([]byte, 20), contract.RequestMeta{NowTick: 2}) // accounted 52 > 40

	value, ok := cache.Get(1, contract.RequestMeta{NowTick: 3})
	if !ok || string(value) != "hello" {
		t.Fatal("oversized replacement should leave the existing entry unchanged")
	}
	if used := cache.UsedBytes(); used != contract.AccountedBytes([]byte("hello")) {
		t.Fatalf("oversized replacement changed accounted bytes: %d", used)
	}
}

func TestTTLBoundaryExpiresExactlyAtExpiryTick(t *testing.T) {
	cache := NewCache(1024)
	cache.Put(1, []byte("value"), contract.RequestMeta{NowTick: 0, TTL: 20})

	if _, ok := cache.Get(1, contract.RequestMeta{NowTick: 19}); !ok {
		t.Fatal("expected a hit one tick before expiry")
	}
	if _, ok := cache.Get(1, contract.RequestMeta{NowTick: 20}); ok {
		t.Fatal("expected a miss exactly at the expiry tick")
	}

	// Lazy reclamation means the expired entry may still be accounted here.
	cache.Put(1, []byte("next"), contract.RequestMeta{NowTick: 21})
	value, ok := cache.Get(1, contract.RequestMeta{NowTick: 22})
	if !ok || string(value) != "next" {
		t.Fatalf("replacement after expiry mismatch: hit=%v value=%q", ok, value)
	}
	if used := cache.UsedBytes(); used != contract.AccountedBytes([]byte("next")) {
		t.Fatalf("replacement accounted bytes = %d, want %d", used, contract.AccountedBytes([]byte("next")))
	}
}

func TestExpiryOverflowSafeComparison(t *testing.T) {
	cache := NewCache(1024)
	start := ^uint64(0) - 5
	cache.Put(1, []byte("value"), contract.RequestMeta{NowTick: start, TTL: 10})

	if _, ok := cache.Get(1, contract.RequestMeta{NowTick: ^uint64(0) - 1}); !ok {
		t.Fatal("saturating expiry should remain live before the maximum tick")
	}
	if _, ok := cache.Get(1, contract.RequestMeta{NowTick: ^uint64(0)}); ok {
		t.Fatal("saturating expiry should expire at the maximum tick")
	}
}

func TestZeroTTLNeverExpiresAtMaximumTick(t *testing.T) {
	cache := NewCache(1024)
	cache.Put(1, []byte("value"), contract.RequestMeta{NowTick: 0, TTL: 0})

	value, ok := cache.Get(1, contract.RequestMeta{NowTick: math.MaxUint64})
	if !ok || string(value) != "value" {
		t.Fatalf("zero-TTL entry was not served at the maximum tick: hit=%v value=%q", ok, value)
	}
}

func TestResizeReplaceEvictsAsNeeded(t *testing.T) {
	// capacity holds key1(3 bytes) + key2(3 bytes) = 2*35 = 70, but not
	// key1(8 bytes) + key2(3 bytes) = 40 + 35 = 75.
	cache := NewCache(70)
	cache.Put(1, []byte("abc"), contract.RequestMeta{NowTick: 1})
	cache.Put(2, []byte("abc"), contract.RequestMeta{NowTick: 2})

	cache.Put(1, []byte("12345678"), contract.RequestMeta{NowTick: 3})

	if used := cache.UsedBytes(); used > 70 {
		t.Fatalf("used bytes = %d exceeds capacity", used)
	}

	// The resize-replace must evict enough to fit the budget, but the policy
	// chooses which sibling is evicted. Any surviving key must return its exact
	// bytes; a miss is also acceptable.
	for key, want := range map[uint64][]byte{
		1: []byte("12345678"),
		2: []byte("abc"),
	} {
		if value, ok := cache.Get(key, contract.RequestMeta{NowTick: 4}); ok && !bytes.Equal(value, want) {
			t.Fatalf("surviving key %d returned the wrong value %q, want %q", key, value, want)
		}
	}
}

func TestReplaceRefreshesWithoutDoubleCharging(t *testing.T) {
	cache := NewCache(1024)
	cache.Put(1, []byte("old"), contract.RequestMeta{NowTick: 1})
	cache.Put(1, []byte("new-value"), contract.RequestMeta{NowTick: 2})

	value, ok := cache.Get(1, contract.RequestMeta{NowTick: 3})
	if !ok || !bytes.Equal(value, []byte("new-value")) {
		t.Fatalf("replacement mismatch: hit=%v value=%q", ok, value)
	}
	if want := contract.AccountedBytes([]byte("new-value")); cache.UsedBytes() != want {
		t.Fatalf("used bytes = %d, want %d", cache.UsedBytes(), want)
	}
}

func TestDeleteZeroCapacityAndAliasing(t *testing.T) {
	zero := NewCache(0)
	zero.Put(1, []byte("value"), contract.RequestMeta{NowTick: 1})
	if _, ok := zero.Get(1, contract.RequestMeta{NowTick: 2}); ok || zero.UsedBytes() != 0 {
		t.Fatal("zero-capacity cache stored an entry")
	}

	cache := NewCache(1024)
	original := []byte("value")
	cache.Put(1, original, contract.RequestMeta{NowTick: 1})
	original[0] = 'X'
	returned, ok := cache.Get(1, contract.RequestMeta{NowTick: 2})
	if !ok || string(returned) != "value" {
		t.Fatal("Put retained a mutable caller alias")
	}
	returned[0] = 'Y'
	again, _ := cache.Get(1, contract.RequestMeta{NowTick: 3})
	if string(again) != "value" {
		t.Fatal("Get returned mutable internal storage")
	}

	cache.Delete(1)
	if _, ok := cache.Get(1, contract.RequestMeta{NowTick: 4}); ok || cache.UsedBytes() != 0 {
		t.Fatal("Delete did not remove the entry")
	}
}
