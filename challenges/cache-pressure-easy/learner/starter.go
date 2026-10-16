// Package starter is the learner-facing scaffold. The verified public
// implementation lives at the manifest entrypoint and is not prefilled here.
package starter

import "github.com/roughstack/challenges/challenges/cache-pressure-easy/contract"

type cache struct {
	capacity uint64
	// TODO: track stored values, their accounted sizes, and recency.
}

func NewCache(capacityBytes uint64) contract.Cache {
	return &cache{capacity: capacityBytes}
}

func (c *cache) Get(key uint64, meta contract.RequestMeta) ([]byte, bool) {
	panic("TODO: return a private copy and update recency on a hit")
}

func (c *cache) Put(key uint64, value []byte, meta contract.RequestMeta) {
	panic("TODO: enforce the exact byte budget and LRU eviction")
}

func (c *cache) Delete(key uint64) {
	panic("TODO: remove the entry and its accounting")
}

func (c *cache) UsedBytes() uint64 {
	panic("TODO: report the charged bytes, including metadata")
}
