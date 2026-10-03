package contract

// PerEntryMetadataBytes is the fixed accounting charge for every resident key.
const PerEntryMetadataBytes uint64 = 32

// RequestMeta is deterministic request context supplied by the harness.
type RequestMeta struct {
	NowTick uint64
	Cost    uint32
	TTL     uint64
}

// Cache is the complete contestant-owned surface for this arena.
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
