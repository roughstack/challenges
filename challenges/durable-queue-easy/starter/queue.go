// Package starter contains a deliberately simple, correct in-memory queue.
//
// The implementation is intentionally straightforward: one mutex protects the
// entire state and a single broadcast channel wakes every waiter on any state
// change. Waiters re-check their predicate and go back to sleep, so the
// thundering herd is correct but not optimal under contention. Expiry is a
// linear scan of outstanding leases. This leaves the central concurrency
// optimization (targeted waiter wakeups and a deadline heap) to the contestant.
package starter

import (
	"context"
	"sort"
	"sync"

	"github.com/roughstack/challenges/challenges/durable-queue-easy/contract"
)

type entry struct {
	job      contract.Job
	seq      uint64 // monotonic enqueue order, ties broken by this value
	attempt  uint32 // number of times this job has been leased
	deadline uint64
	token    uint64
}

type queue struct {
	mu        sync.Mutex
	notify    chan struct{}
	closed    bool
	capacity  uint64
	resident  uint64
	nextSeq   uint64
	nextToken uint64
	available []*entry
	leased    map[uint64]*entry // lease token -> entry
}

// OpenQueue returns an in-memory queue bounded by capacityBytes. dir is ignored.
func OpenQueue(_ string, capacityBytes uint64) (contract.Queue, error) {
	return &queue{
		capacity: capacityBytes,
		notify:   make(chan struct{}),
		leased:   make(map[uint64]*entry),
	}, nil
}

func (q *queue) Enqueue(ctx context.Context, job contract.Job) error {
	if ctx == nil {
		ctx = context.Background()
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return contract.ErrClosed
	}
	size := uint64(len(job.Payload))
	if size > q.capacity {
		return contract.ErrJobTooLarge
	}
	payload := clone(job.Payload)
	for {
		if q.closed {
			return contract.ErrClosed
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if addSat(q.resident, size) <= q.capacity {
			q.available = append(q.available, &entry{
				job: contract.Job{ID: job.ID, Payload: payload},
				seq: q.nextSeq,
			})
			q.nextSeq++
			q.resident += size
			q.signal()
			return nil
		}
		if err := q.wait(ctx); err != nil {
			return err
		}
	}
}

func (q *queue) Lease(ctx context.Context, nowTick uint64, visibility uint64) (contract.Lease, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	for {
		if q.closed {
			return contract.Lease{}, contract.ErrClosed
		}
		if ctx.Err() != nil {
			return contract.Lease{}, ctx.Err()
		}
		q.expire(nowTick)
		if len(q.available) > 0 {
			return q.pop(nowTick, visibility), nil
		}
		if err := q.wait(ctx); err != nil {
			return contract.Lease{}, err
		}
	}
}

func (q *queue) Ack(_ context.Context, token uint64) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return contract.ErrClosed
	}
	entry, ok := q.leased[token]
	if !ok {
		return contract.ErrStaleToken
	}
	delete(q.leased, token)
	q.resident -= uint64(len(entry.job.Payload))
	q.signal()
	return nil
}

func (q *queue) Nack(_ context.Context, token uint64) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return contract.ErrClosed
	}
	entry, ok := q.leased[token]
	if !ok {
		return contract.ErrStaleToken
	}
	delete(q.leased, token)
	q.available = append(q.available, entry)
	q.signal()
	return nil
}

func (q *queue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil
	}
	q.closed = true
	q.available = nil
	q.leased = nil
	q.signal()
	return nil
}

// expire moves every lease whose deadline has been reached back to the
// available queue in ascending deadline order (ties broken by enqueue order).
// It invalidates the old tokens so late Ack/Nack calls are rejected.
func (q *queue) expire(nowTick uint64) {
	if len(q.leased) == 0 {
		return
	}
	expired := make([]*entry, 0, len(q.leased))
	for _, entry := range q.leased {
		if entry.deadline <= nowTick {
			expired = append(expired, entry)
		}
	}
	if len(expired) == 0 {
		return
	}
	sort.Slice(expired, func(i, j int) bool {
		if expired[i].deadline != expired[j].deadline {
			return expired[i].deadline < expired[j].deadline
		}
		return expired[i].seq < expired[j].seq
	})
	for _, entry := range expired {
		delete(q.leased, entry.token)
		q.available = append(q.available, entry)
	}
	q.signal()
}

func (q *queue) pop(nowTick uint64, visibility uint64) contract.Lease {
	entry := q.available[0]
	q.available = q.available[1:]
	entry.attempt++
	entry.deadline = addSat(nowTick, visibility)
	entry.token = q.nextToken
	q.nextToken++
	q.leased[entry.token] = entry
	return contract.Lease{
		Job: contract.Job{
			ID:      entry.job.ID,
			Payload: clone(entry.job.Payload),
		},
		Token:    entry.token,
		Deadline: entry.deadline,
		Attempt:  entry.attempt,
	}
}

// signal wakes every waiter without holding any per-waiter state. Callers hold
// q.mu; waiters re-check their predicate after waking.
func (q *queue) signal() {
	close(q.notify)
	q.notify = make(chan struct{})
}

// wait releases the lock, blocks until a state change or context
// cancellation, and re-acquires the lock before returning.
func (q *queue) wait(ctx context.Context) error {
	notify := q.notify
	q.mu.Unlock()
	var err error
	select {
	case <-notify:
	case <-ctx.Done():
		err = ctx.Err()
	}
	q.mu.Lock()
	return err
}

func clone(payload []byte) []byte {
	copyOfPayload := make([]byte, len(payload))
	copy(copyOfPayload, payload)
	return copyOfPayload
}

func addSat(a, b uint64) uint64 {
	if a > ^uint64(0)-b {
		return ^uint64(0)
	}
	return a + b
}
