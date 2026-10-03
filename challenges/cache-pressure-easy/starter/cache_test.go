package starter

import (
	"bytes"
	"testing"

	"github.com/roughstack/challenges/challenges/cache-pressure-easy/contract"
)

func TestLRUOrder(t *testing.T) {
	value := []byte("value")
	capacity := contract.AccountedBytes(value) * 2
	cache := NewCache(capacity)

	cache.Put(1, value, contract.RequestMeta{NowTick: 1})
	cache.Put(2, value, contract.RequestMeta{NowTick: 2})
	cache.Get(1, contract.RequestMeta{NowTick: 3})
	cache.Put(3, value, contract.RequestMeta{NowTick: 4})

	if _, ok := cache.Get(2, contract.RequestMeta{}); ok {
		t.Fatal("least-recent key 2 should be evicted")
	}
	for _, key := range []uint64{1, 3} {
		if _, ok := cache.Get(key, contract.RequestMeta{}); !ok {
			t.Fatalf("key %d should remain resident", key)
		}
	}
}

func TestReplaceRefreshesWithoutDoubleCharging(t *testing.T) {
	cache := NewCache(1024)
	cache.Put(1, []byte("old"), contract.RequestMeta{})
	cache.Put(1, []byte("new-value"), contract.RequestMeta{})

	value, ok := cache.Get(1, contract.RequestMeta{})
	if !ok || !bytes.Equal(value, []byte("new-value")) {
		t.Fatalf("replacement mismatch: hit=%v value=%q", ok, value)
	}
	if want := contract.AccountedBytes([]byte("new-value")); cache.UsedBytes() != want {
		t.Fatalf("used bytes = %d, want %d", cache.UsedBytes(), want)
	}
}

func TestDeleteZeroCapacityAndAliasing(t *testing.T) {
	zero := NewCache(0)
	zero.Put(1, []byte("value"), contract.RequestMeta{})
	if _, ok := zero.Get(1, contract.RequestMeta{}); ok || zero.UsedBytes() != 0 {
		t.Fatal("zero-capacity cache stored an entry")
	}

	cache := NewCache(1024)
	original := []byte("value")
	cache.Put(1, original, contract.RequestMeta{})
	original[0] = 'X'
	returned, ok := cache.Get(1, contract.RequestMeta{})
	if !ok || string(returned) != "value" {
		t.Fatal("Put retained a mutable caller alias")
	}
	returned[0] = 'Y'
	again, _ := cache.Get(1, contract.RequestMeta{})
	if string(again) != "value" {
		t.Fatal("Get returned mutable internal storage")
	}

	cache.Delete(1)
	if _, ok := cache.Get(1, contract.RequestMeta{}); ok || cache.UsedBytes() != 0 {
		t.Fatal("Delete did not remove the entry")
	}
}

func TestOversizedReplacementLeavesExistingEntry(t *testing.T) {
	existing := []byte("small")
	cache := NewCache(contract.AccountedBytes(existing))
	cache.Put(1, existing, contract.RequestMeta{})
	cache.Put(1, make([]byte, len(existing)+1), contract.RequestMeta{})

	value, ok := cache.Get(1, contract.RequestMeta{})
	if !ok || !bytes.Equal(value, existing) {
		t.Fatal("oversized replacement should leave the existing entry unchanged")
	}
}
