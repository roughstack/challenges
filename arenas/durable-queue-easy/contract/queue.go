// Package contract defines the contestant-owned surface for the
// durable-queue-easy arena.
package contract

import (
	"context"
	"errors"
)

// Job is a unit of work with a unique ID and an opaque payload.
//
// IDs are unique at enqueue time for a single queue instance. The queue copies
// the payload at the enqueue ownership boundary and returns an independent copy
// from Lease, so the queue never aliases caller-owned memory.
type Job struct {
	ID      uint64
	Payload []byte
}

// Lease is a visibility-scoped claim over a Job.
type Lease struct {
	Job      Job
	Token    uint64
	Deadline uint64
	Attempt  uint32
}

// Queue is the complete contestant-owned surface for this arena.
//
// Semantics:
//
//   - Enqueue adds a job in FIFO order and blocks while the resident payload
//     bytes (enqueued or leased but not acknowledged) would exceed
//     capacityBytes. It returns ErrJobTooLarge when a single payload can never
//     fit. It returns ctx.Err() when the context is canceled before the job is
//     admitted.
//   - Lease returns the oldest available job, or blocks until a job becomes
//     available. nowTick is injected virtual time: a previously leased job
//     whose Deadline has been reached or passed becomes available again and is
//     redelivered with an incremented Attempt and a fresh token. visibility is
//     added to nowTick to form the returned Deadline.
//   - Ack permanently removes a leased job. It must never be delivered again.
//   - Nack returns a leased job to the back of the available queue without
//     changing its Attempt.
//   - Ack and Nack reject unknown or already-used tokens with ErrStaleToken.
//   - Close is terminal and idempotent. After Close every operation returns
//     ErrClosed, and any blocked producer or consumer is woken with ErrClosed.
//     Accepted-but-unacknowledged jobs are discarded: the easy tier is
//     in-memory and performs no persistence.
//
// Available jobs are delivered in FIFO order of availability: enqueue appends,
// Nack appends, and visibility expiry appends in ascending Deadline order
// (ties broken by enqueue order).
type Queue interface {
	Enqueue(ctx context.Context, job Job) error
	Lease(ctx context.Context, nowTick uint64, visibility uint64) (Lease, error)
	Ack(ctx context.Context, token uint64) error
	Nack(ctx context.Context, token uint64) error
	Close() error
}

// Factory constructs a fresh queue for one isolated workload run.
//
// dir is reserved for the persistent tiers; the easy tier ignores it.
type Factory func(dir string, capacityBytes uint64) (Queue, error)

// Documented errors returned by the queue.
var (
	// ErrClosed is returned by every operation after Close has been called.
	ErrClosed = errors.New("durable queue: closed")
	// ErrJobTooLarge is returned by Enqueue when a payload can never fit.
	ErrJobTooLarge = errors.New("durable queue: job payload exceeds capacity")
	// ErrStaleToken is returned by Ack and Nack for unknown or used tokens.
	ErrStaleToken = errors.New("durable queue: stale or unknown lease token")
)
