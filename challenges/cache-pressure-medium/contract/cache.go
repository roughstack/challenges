// Package contract defines the contestant-owned cache surface for the
// cache-pressure-medium arena.
package contract

// PerEntryMetadataBytes is the fixed accounting charge for every resident key.
const PerEntryMetadataBytes uint64 = 32

// RequestMeta is deterministic request context supplied by the harness.
//
// NowTick is the injected virtual tick of the request. TTL is the entry's
// lifetime in ticks measured from the Put tick; a TTL of 0 means the entry
// never expires. Cost is the backing-store cost of a miss for this request; it
// does not influence cache accounting.
type RequestMeta struct {
	NowTick uint64
	Cost    uint32
	TTL     uint64
}

// Cache is the complete contestant-owned surface for this arena.
//
// Expiry: Put records expiry tick E = ExpiryTick(meta.NowTick, meta.TTL). An
// entry is expired, and must be treated as absent, at the first operation whose
// meta.NowTick >= E. Get never extends expiry; Put (including replacement)
// resets it. Expired entries must not be returned by Get.
//
// Values returned by Get must not alias mutable internal storage, and values
// passed to Put must not remain aliased to the caller.
type Cache interface {
	Get(key uint64, meta RequestMeta) ([]byte, bool)
	Put(key uint64, value []byte, meta RequestMeta)
	Delete(key uint64)
	UsedBytes() uint64
}

// Factory constructs a fresh cache for one isolated workload run.
type Factory func(capacityBytes uint64) Cache

// AccountedBytes returns the value and fixed metadata charge for one entry.
func AccountedBytes(value []byte) uint64 {
	return uint64(len(value)) + PerEntryMetadataBytes
}

// ExpiryTick returns the tick at which an entry written with the given TTL at
// the given tick expires, saturating on overflow. A TTL of 0 means the entry
// never expires. An entry is expired once
// NowTick >= ExpiryTick(NowTickAtPut, TTL).
func ExpiryTick(now, ttl uint64) uint64 {
	const max = ^uint64(0)
	if ttl == 0 {
		return max
	}
	if now > max-ttl {
		return max
	}
	return now + ttl
}
