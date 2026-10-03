# Cache Pressure

A read-heavy service fronts an expensive backing store. Cache memory is strictly
limited, values have variable byte sizes, some entries expire, and a single
sequential scan can exceed the entire cache. The player owns the cache policy;
the harness owns the backing store, request stream, accounting, and memory
validation.

## Scenario

Requests arrive on an injected virtual clock (`NowTick`). Reads that miss are
charged a per-key backing cost and then filled from the backing store. Writes
replace values, and deletes remove them. The workload mixes a hot key set,
exactly one cache-sized scan of cold keys, and a repeat of the hot set, so a
policy that lets the scan evict its working set pays the miss cost twice.

## Non-goals

- No concurrency. This variant is single-threaded.
- No persistence, networking, or wall-clock dependence. Time is the injected
  virtual tick; all counters are deterministic from a `uint64` seed.
- No admission of oversized values. A value whose accounted size exceeds the
  byte budget is rejected and must not evict resident data.

## Contract

Implement `contract.Cache` and expose this constructor:

```go
func NewCache(capacityBytes uint64) contract.Cache
```

Each resident entry is charged `len(value) + contract.PerEntryMetadataBytes`.
`UsedBytes` must never exceed the configured capacity at an operation boundary.

Expiry: `Put` records expiry tick `E = contract.ExpiryTick(meta.NowTick, meta.TTL)`.
An entry is expired, and treated as absent, at the first operation whose
`meta.NowTick >= E`. A `TTL` of 0 means the entry never expires. `Get` never
extends expiry; `Put` (including replacement) resets it. `ExpiryTick` saturates
on overflow, so tick arithmetic is safe across the full `uint64` range.

`Get` returns a copy, and `Put` must not retain a caller alias. Oversized values
(new keys and replacements alike) are rejected without touching resident data.
Zero-capacity caches never store entries.

The starter uses a single-segment LRU with byte accounting, lazy expiry, and
bounded expired-entry cleanup. It is semantically correct but deliberately
non-optimal: recency maintenance is linear time and the single segment offers no
protection against scan pollution.

## Metrics

- `weighted_backing_cost`: sum of the per-key miss costs charged on cache misses.
- `byte_hit_rate`: bytes served from cache divided by bytes requested, in basis
  points (0–10000).
- `request_hit_rate`: cache hits divided by read requests, in basis points.
- `logical_policy_work`: deterministic count of cache-interface and accounting
  operations the harness performed, including integrity checks.
- `peak_memory_headroom`: `capacity - peak accounted bytes`, the unused byte
  budget at peak occupancy. Lower means the cache exploited its budget more fully;
  it is a soft signal alongside the hit-rate and cost metrics.

Correctness gates scoring. Public smoke output is reproducible but unofficial;
normalization and calibration belong to the platform and private judge.

## Verify

From the repository root:

```sh
go test ./...
go run ./challenges/cache-pressure-medium/cmd/smoke --seed 1844674407370955161
```

The smoke command writes exactly one `bytearena.result/v1` JSON object to stdout.
It does not use wall time, network access, environment secrets, or random seeds.
Running it twice with the same seed produces identical counters.
