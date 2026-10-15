// Package starter is the learner-facing scaffold for a lease-based queue.
package starter

import (
	"context"

	"github.com/roughstack/challenges/challenges/durable-queue-easy/contract"
)

type queue struct {
	capacity uint64
	// TODO: protect queued, leased, and closed state under concurrency.
}

func OpenQueue(_ string, capacityBytes uint64) (contract.Queue, error) {
	return &queue{capacity: capacityBytes}, nil
}

func (q *queue) Enqueue(ctx context.Context, job contract.Job) error {
	panic("TODO: copy the payload and apply capacity backpressure")
}

func (q *queue) Lease(ctx context.Context, nowTick, visibility uint64) (contract.Lease, error) {
	panic("TODO: issue a lease or wait for work")
}

func (q *queue) Ack(ctx context.Context, token uint64) error {
	panic("TODO: permanently remove the leased job")
}

func (q *queue) Nack(ctx context.Context, token uint64) error {
	panic("TODO: make the job visible again")
}

func (q *queue) Close() error {
	panic("TODO: wake waiters and close idempotently")
}
