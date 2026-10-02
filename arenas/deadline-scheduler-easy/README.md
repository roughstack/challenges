# Deadline Scheduler

A worker service receives jobs with arrival ticks and estimated work. Schedule
non-preemptive jobs on one worker to reduce mean and tail flow time while
guaranteeing that every waiting job eventually runs. Runtime estimates are
exact in this tier, and all execution happens on a deterministic virtual clock
owned by the harness.

## Scenario

Jobs arrive at deterministic virtual ticks and each carries an exact
`EstimatedWork`. A job started at tick `s` runs until tick `s + EstimatedWork`;
it can never be preempted and no second job can run while it is busy. The
scheduler sees arrivals, per-tick progress, and completions, and may request
that a waiting job be started. The harness validates every request and owns all
accounting and result construction.

## Non-goals

- No multiple workers, worker placement, preemption, or admission control.
  Those belong to the medium and hard variants.
- No variable runtimes: estimates are exact here, so progress is deterministic.
- No wall-clock timers, network access, environment secrets, or goroutines.
- No hidden workloads, ranked seeds, or reference implementations in this
  repository.

## Contract

Implement `contract.Scheduler` and expose this constructor:

```go
func NewScheduler(cluster contract.Cluster) contract.Scheduler
```

The full surface lives in `contract/contract.go`:

- `OnArrival(job Job, nowTick uint64) []Action` — called once per job at its
  arrival tick. Return an `ActionRun` when the worker is idle.
- `OnProgress(progress Progress, nowTick uint64) []Action` — called once per
  running tick with the running job's remaining work. It is also called once
  with a zero `Progress` when the worker is idle at the end of a tick, after
  every same-tick arrival and completion has been delivered, so the scheduler
  can choose with the full batch visible.
- `OnComplete(completion Completion, nowTick uint64) []Action` — called when the
  running job finishes. Return an `ActionRun` for the next waiting job.
- `StateBytes() uint64` — report the scheduler's current logical state size for
  the deterministic memory metric.

Only `ID`, `Arrival`, and `EstimatedWork` are used in this tier. The remaining
job fields are reserved and always zero in public workloads.

The harness rejects any action that runs a job before its arrival, double-places
a running job, preempts the worker, runs a completed job, references an unknown
job, or overflows virtual-clock arithmetic. The harness may deliver several
`OnArrival` calls for the same tick before applying returned actions, so a
scheduler that returns an `ActionRun` should record that job as running
immediately. Every waiting job must eventually run; a scheduler that stalls
with waiting jobs fails the correctness gate.

## Metrics

- `mean_flow_ticks` — mean completion-minus-arrival time in virtual ticks
  (minimize).
- `p95_flow_ticks` — 95th percentile flow time in virtual ticks (minimize).
- `starvation_penalty` — total ticks beyond `contract.MaxWaitTicks` that any job
  waited before starting (minimize).
- `scheduler_work` — deterministic count of scheduler callbacks and returned
  actions charged by the harness (minimize).
- `peak_state_bytes` — peak logical state bytes reported by `StateBytes`
  (minimize).

Correctness gates scoring. Public smoke output is reproducible but unofficial;
normalization and calibration belong to the platform and private judge.

The starter is a deliberately simple FIFO scheduler. It is correct — every
waiting job eventually runs and no invalid action is emitted — but it is
intentionally non-optimal on the short-before-long and aging workload shapes.

## Verify

From the repository root:

```sh
go fmt ./arenas/deadline-scheduler-easy/...
go test ./arenas/deadline-scheduler-easy/...
go vet ./arenas/deadline-scheduler-easy/...
go test ./...
go run ./arenas/deadline-scheduler-easy/cmd/smoke --seed 1844674407370955161
```

Run the smoke command twice with the same seed; the logical counters must be
identical. The command writes exactly one `bytearena.result/v1` JSON object to
stdout and uses no wall time, network access, environment secrets, or random
seeds.
