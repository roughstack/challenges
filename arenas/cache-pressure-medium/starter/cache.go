// Package starter contains a deliberately simple, correct cache implementation.
//
// The starter uses a single-segment LRU with byte accounting, lazy expiry, and
// bounded expired-entry cleanup. It is semantically correct on the public
// contract but intentionally non-optimal: recency maintenance is linear time,
// and the single segment offers no protection against scan pollution.
package starter

import "github.com/bytearena/arenas/arenas/cache-pressure-medium/contract"

type entry struct {
	key          uint64
	value        []byte
	expiry       uint64
	accounted    uint64
	neverExpires bool
}

type cache struct {
	capacity uint64
	used     uint64
	entries  map[uint64]*entry
	order    []uint64 // least recent to most recent
}

// NewCache returns a byte-accounted LRU cache with lazy expiry.
func NewCache(capacityBytes uint64) contract.Cache {
	return &cache{
		capacity: capacityBytes,
		entries:  make(map[uint64]*entry),
	}
}

func (c *cache) Get(key uint64, meta contract.RequestMeta) ([]byte, bool) {
	e, ok := c.entries[key]
	if !ok {
		return nil, false
	}
	if !e.neverExpires && meta.NowTick >= e.expiry {
		c.remove(key)
		return nil, false
	}

	c.touch(key)
	return clone(e.value), true
}

func (c *cache) Put(key uint64, value []byte, meta contract.RequestMeta) {
	accounted := contract.AccountedBytes(value)
	if accounted > c.capacity {
		// Oversized values are rejected without touching resident data.
		return
	}

	expiry := contract.ExpiryTick(meta.NowTick, meta.TTL)
	if e, ok := c.entries[key]; ok && (e.neverExpires || meta.NowTick < e.expiry) {
		// Replace a live entry without changing the item count.
		c.used -= e.accounted
		c.entries[key] = &entry{
			key:          key,
			value:        clone(value),
			expiry:       expiry,
			accounted:    accounted,
			neverExpires: meta.TTL == 0,
		}
		c.used += accounted
		c.touch(key)
		c.reclaim(meta.NowTick)
		return
	}

	if _, ok := c.entries[key]; ok {
		c.remove(key)
	}

	c.entries[key] = &entry{
		key:          key,
		value:        clone(value),
		expiry:       expiry,
		accounted:    accounted,
		neverExpires: meta.TTL == 0,
	}
	c.order = append(c.order, key)
	c.used += accounted
	c.reclaim(meta.NowTick)
}

func (c *cache) Delete(key uint64) {
	if _, ok := c.entries[key]; ok {
		c.remove(key)
	}
}

func (c *cache) UsedBytes() uint64 {
	return c.used
}

// reclaim performs bounded expired-entry cleanup, then evicts the least-recent
// entries until the byte budget is satisfied.
func (c *cache) reclaim(now uint64) {
	c.sweepExpired(now)
	for c.used > c.capacity {
		c.evictOldest()
	}
}

// sweepExpired removes expired entries among a bounded window of the oldest
// entries. Remaining expired entries are reclaimed lazily on access.
func (c *cache) sweepExpired(now uint64) {
	const maxSweep = 32

	var expired []uint64
	for index, key := range c.order {
		if index >= maxSweep {
			break
		}
		if e, ok := c.entries[key]; ok && !e.neverExpires && now >= e.expiry {
			expired = append(expired, key)
		}
	}
	for _, key := range expired {
		c.remove(key)
	}
}

func (c *cache) evictOldest() {
	if len(c.order) == 0 {
		return
	}
	c.remove(c.order[0])
}

func (c *cache) remove(key uint64) {
	e, ok := c.entries[key]
	if !ok {
		return
	}
	c.used -= e.accounted
	delete(c.entries, key)
	c.removeFromOrder(key)
}

func (c *cache) touch(key uint64) {
	c.removeFromOrder(key)
	c.order = append(c.order, key)
}

func (c *cache) removeFromOrder(key uint64) {
	for index, candidate := range c.order {
		if candidate == key {
			copy(c.order[index:], c.order[index+1:])
			c.order = c.order[:len(c.order)-1]
			return
		}
	}
}

func clone(value []byte) []byte {
	copyOfValue := make([]byte, len(value))
	copy(copyOfValue, value)
	return copyOfValue
}
