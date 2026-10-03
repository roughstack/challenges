// Package starter contains a deliberately simple, correct cache implementation.
package starter

import "github.com/roughstack/challenges/challenges/cache-pressure-easy/contract"

type cache struct {
	capacity uint64
	used     uint64
	values   map[uint64][]byte
	recency  []uint64 // least recent to most recent
}

// NewCache returns an exact-capacity LRU cache with linear-time recency updates.
func NewCache(capacityBytes uint64) contract.Cache {
	return &cache{
		capacity: capacityBytes,
		values:   make(map[uint64][]byte),
	}
}

func (c *cache) Get(key uint64, _ contract.RequestMeta) ([]byte, bool) {
	value, ok := c.values[key]
	if !ok {
		return nil, false
	}

	c.touch(key)
	return clone(value), true
}

func (c *cache) Put(key uint64, value []byte, _ contract.RequestMeta) {
	size := contract.AccountedBytes(value)
	if size > c.capacity {
		return
	}

	if previous, ok := c.values[key]; ok {
		c.used -= contract.AccountedBytes(previous)
		c.values[key] = clone(value)
		c.used += size
		c.touch(key)
	} else {
		c.values[key] = clone(value)
		c.recency = append(c.recency, key)
		c.used += size
	}

	for c.used > c.capacity {
		c.evictOldest()
	}
}

func (c *cache) Delete(key uint64) {
	value, ok := c.values[key]
	if !ok {
		return
	}

	c.used -= contract.AccountedBytes(value)
	delete(c.values, key)
	c.removeFromRecency(key)
}

func (c *cache) UsedBytes() uint64 {
	return c.used
}

func (c *cache) touch(key uint64) {
	c.removeFromRecency(key)
	c.recency = append(c.recency, key)
}

func (c *cache) evictOldest() {
	if len(c.recency) == 0 {
		return
	}

	key := c.recency[0]
	c.recency = c.recency[1:]
	value := c.values[key]
	c.used -= contract.AccountedBytes(value)
	delete(c.values, key)
}

func (c *cache) removeFromRecency(key uint64) {
	for index, candidate := range c.recency {
		if candidate == key {
			copy(c.recency[index:], c.recency[index+1:])
			c.recency = c.recency[:len(c.recency)-1]
			return
		}
	}
}

func clone(value []byte) []byte {
	copyOfValue := make([]byte, len(value))
	copy(copyOfValue, value)
	return copyOfValue
}
