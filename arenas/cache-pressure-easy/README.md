# Cache Pressure

A read-heavy service fronts an expensive backing store. Implement a
single-threaded, exact-capacity LRU cache that avoids unnecessary backing reads.
All public workload values have the same size.

## Contract

Implement `contract.Cache` and expose this constructor:

```go
func NewCache(capacityBytes uint64) contract.Cache
```

`Get` hits and replacement `Put` calls refresh recency. Values returned from
`Get` must be copies, and values passed to `Put` must not remain aliased to the
caller. The oldest key by recency is evicted when an entry must be admitted.
Zero-capacity caches never store entries.

Each resident entry is charged its value length plus
`contract.PerEntryMetadataBytes`. `UsedBytes` must never exceed the configured
capacity at an operation boundary.

The starter uses a deliberately linear-time recency list. It is semantically
correct, but the intended scalable solution is constant-time lookup and recency
maintenance. This keeps the public repository's test suite green while still
leaving the central data-structure optimization to the contestant.

## Metrics

- `backing_reads`: public requests that missed and loaded from the backing store.
- `metadata_work`: deterministic cache API and accounting work charged by the
  public harness.
- `peak_accounted_bytes`: maximum value-plus-metadata bytes reported by the cache.

Correctness gates scoring. Public smoke output is reproducible but unofficial;
normalization and calibration belong to the platform and private judge.

## Verify

From the repository root:

```sh
go test ./...
go run ./arenas/cache-pressure-easy/cmd/smoke --seed 12345
```

The smoke command writes exactly one `bytearena.result/v1` JSON object to stdout.
It does not use wall time, network access, environment secrets, or random seeds.
