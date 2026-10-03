# Durable Work Queue

Producers submit jobs and workers lease them. Build a bounded, in-memory,
single-process queue for concurrent producers and consumers. Preserve FIFO
order among available jobs, block or cancel cleanly under backpressure, and
implement Ack/Nack without persistence.

## Contract

Implement `contract.Queue` and expose this constructor:

```go
func OpenQueue(dir string, capacityBytes uint64) (contract.Queue, error)
```

`dir` is reserved for the persistent tiers and is ignored here. The queue keeps
no filesystem state and adds no external dependencies.

- `Enqueue` copies the payload, admits jobs in FIFO order, and blocks while the
  resident payload bytes (enqueued or leased but not acknowledged) would exceed
  `capacityBytes`. It returns `ErrJobTooLarge` for a payload that can never fit,
  and `ctx.Err()` when canceled before admission.
- `Lease(ctx, nowTick, visibility)` returns the oldest available job with a
  unique token, `Deadline = nowTick + visibility`, and an incremented `Attempt`.
  `nowTick` is injected virtual time: a lease whose deadline has been reached is
  redelivered with a fresh token. A job is never silently lost.
- `Ack` permanently removes a leased job. `Nack` returns it to the back of the
  available queue with its `Attempt` unchanged. Both reject unknown or
  already-used tokens with `ErrStaleToken`.
- `Close` is terminal and idempotent: every later operation returns `ErrClosed`
  and any blocked producer or consumer is woken with `ErrClosed`. Accepted but
  unacknowledged jobs are discarded because the easy tier is in-memory.

Available jobs are delivered in FIFO order of availability: enqueue appends,
Nack appends, and visibility expiry appends in ascending deadline order (ties
broken by enqueue order).

The starter uses one mutex plus a single broadcast channel that wakes every
waiter on any state change, and a linear scan for expiry. It is semantically
correct but thundering-herd under contention; the intended optimization is
targeted waiter wakeups and a deadline heap.

## Metrics

- `completed_jobs`: jobs acknowledged by the end of the workload.
- `logical_queue_wait`: p99 of enqueue-to-ack wait in logical ticks.
- `sync_work`: deterministic count of synchronized queue operations charged by
  the public harness.
- `peak_bytes`: maximum resident payload bytes observed by the harness.

Correctness gates scoring. Public smoke output is reproducible but unofficial;
normalization and calibration belong to the platform and private judge.

## Verify

From the repository root:

```sh
go test ./...
go run ./arenas/durable-queue-easy/cmd/smoke --seed 1844674407370955161
```

The smoke command writes exactly one `bytearena.result/v1` JSON object to stdout.
It does not use wall time, network access, environment secrets, or random seeds.
