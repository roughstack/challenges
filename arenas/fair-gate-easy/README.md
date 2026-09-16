# Fair Gate — Easy

An API gateway must protect a shared downstream service. Implement a
single-tenant token bucket rate limiter with a configured rate and burst
capacity. The harness owns a monotonic virtual clock and asks the limiter,
one request at a time, whether the request is admitted and when a rejected
request may retry.

## Scenario

The limiter starts with a full bucket of `Burst` tokens. Each request has a
whole-token `Cost` and an arrival tick `NowTick` on the harness-owned virtual
clock. The bucket refills lazily at a rational rate of `RateNum` tokens per
`RateDen` ticks, clamped to `Burst`. All arithmetic is deterministic integer
fixed-point arithmetic; there is no floating point, no wall clock, and no
network access.

A request is admitted only when the bucket holds at least its cost after the
lazy refill. When a request is rejected, the limiter reports `RetryAt`: the
earliest tick strictly greater than `NowTick` at which an identical retry would
be admitted assuming no intervening requests, or `RetryNever` (zero) when no
finite tick can satisfy it. Allowed decisions always return a zero `RetryAt`.

The harness owns the clock, traffic generation, correctness gates, accounting,
and result construction. The contestant owns only the `Decide` call.

## Non-goals

- No multi-tenant budgets, global caps, clustering, real networking, or shared
  packages. `Tenant` is reserved for higher tiers and ignored here.
- No wall-clock timers, goroutines, network access, environment secrets, or
  ranked seeds.
- No hidden workloads, reference implementations, or calibration data in this
  repository.

## Contract

Implement `contract.Limiter` and expose this constructor:

```go
func NewLimiter(cfg contract.Config) contract.Limiter
```

The full surface lives in `contract/contract.go`:

- `Decide(Request) Decision` — the only contestant-owned method. It receives
  one request and returns the admission decision and earliest retry tick.
- `Request` carries `Tenant`, `Cost`, and `NowTick`. `Cost` is in whole tokens.
  `NowTick` is non-decreasing across the requests delivered by the harness.
- `Config` carries `RateNum`, `RateDen`, and `Burst`. `RateDen` zero falls back
  to `1`. A `RateNum` of zero means no refill; a `Burst` of zero means the
  bucket is always empty.

Config and request edge cases are deterministic:

- Zero `RateNum` — only the initial burst is available, then positive-cost
  requests are rejected with `RetryNever`.
- Zero `Burst` — positive-cost requests are rejected with `RetryNever`; a
  zero-cost request is always admitted.
- Cost above `Burst` — rejected with `RetryNever`.
- Same-tick requests — the bucket does not refill between them.
- Non-monotonic `NowTick` — the harness rejects the run with a deterministic
  violation before calling `Decide`.

## Metrics

- `decisions_per_second` — decisions processed per logical second of injected
  virtual time (maximize).
- `arithmetic_work` — deterministic count of `Decide` calls processed during
  the run (minimize).
- `alloc_bytes` — cumulative heap bytes allocated by the limiter factory and
  `Decide` calls, measured deterministically with garbage collection disabled.
  Leaner implementations score better (minimize).

Correctness gates scoring. Public smoke output is reproducible but unofficial;
normalization and calibration belong to the platform and private judge.

The starter is a deliberately simple, correct token bucket. It recomputes the
rational denominator and performs checked arithmetic on every call, so it is
easy to verify but intentionally non-optimal.

## Verify

From the repository root:

```sh
go fmt ./arenas/fair-gate-easy/...
go test ./arenas/fair-gate-easy/...
go test -race ./arenas/fair-gate-easy/...
go vet ./arenas/fair-gate-easy/...
go test ./...
go run ./arenas/fair-gate-easy/cmd/smoke --seed 1844674407370955161
```

Run the smoke command twice with the same seed; the logical counters must be
identical. The command writes exactly one `bytearena.result/v1` JSON object to
stdout and uses no wall time, network access, environment secrets, or random
seeds.
