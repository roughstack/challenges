// Package starter is the learner-facing scaffold for the TTL cache.
package starter

import "github.com/roughstack/challenges/challenges/cache-pressure-medium/contract"

type cache struct {
	capacity uint64
	// TODO: track values, expiry ticks, recency, and byte accounting.
}

func NewCache(capacityBytes uint64) contract.Cache {
	return &cache{capacity: capacityBytes}
}

func (c *cache) Get(key uint64, meta contract.RequestMeta) ([]byte, bool) {
	panic("TODO: expire at the exact tick and update recency")
}

func (c *cache) Put(key uint64, value []byte, meta contract.RequestMeta) {
	panic("TODO: handle expiry, replacement, admission, and eviction")
}

func (c *cache) Delete(key uint64) {
	panic("TODO: remove an entry without leaking its byte charge")
}

func (c *cache) UsedBytes() uint64 {
	panic("TODO: report current accounted bytes")
}
