# LSM Compactor

A simulated LSM tree flushes immutable sorted runs into one unordered Level-0
and one sorted Level-1. The contestant owns only the compaction policy; the
harness owns flushes, reads, scans, writes, deletes, snapshots, logical byte
accounting, run-overlap tracking, and result construction.

## Scenario

Writes and deletes enter a memtable. When the memtable reaches its configured
flush size the harness flushes it as a new Level-0 run. Reads search the
memtable first; a memtable hit performs no run reads. Otherwise the harness
searches every overlapping Level-0 run and the sorted Level-1 run set, charging
the logical bytes of every run it searches. A Level-0 hit additionally probes
overlapping Level-1 runs created at or after the hit's run for recency,
charging those probes. A compaction merges selected runs into Level-1; the
merged run keeps the newest entry for each key.

The policy sees a value snapshot of the tree and returns either a compaction
plan or no work. A plan must reference existing runs, target Level-1, include at
least one Level-0 run, and include every Level-1 run that overlaps the selected
Level-0 range. Including a distant, non-overlapping Level-1 run is invalid, as
is a stale or oversized plan. The harness rejects invalid plans instead of
repairing them.

## Non-goals

- No deeper level hierarchies, real filesystem storage, or shared packages.
- No wall time, goroutines, network access, environment secrets, or random
  seeds. Time is a virtual tick owned by the harness.
- No hidden workloads, ranked seeds, or reference implementations in this
  repository.

## Policy contract

Implement `contract.Policy` and expose this constructor:

```go
func NewPolicy(cfg contract.Config) contract.Policy
```

The full surface lives in `contract/contract.go`:

- `Pick(state contract.State) (contract.Plan, bool)` — return a plan plus true
  to compact, or false to do nothing. `state` is a deep-copied value snapshot.
- `RunMeta` describes one immutable run: `ID`, `Level`, `Bytes`, `Entries`,
  `Tombstones`, `MinKey`, `MaxKey`, and `CreatedTick`.
- `Plan` carries `InputRunIDs` and `TargetLevel` (must be `1` for easy).

`Config` carries `Level0TriggerRuns`; zero resolves to
`contract.DefaultLevel0TriggerRuns`.

The starter uses a plain threshold trigger: below the published Level-0 run
count it does nothing, and at or above it compacts all Level-0 runs together
with the overlapping Level-1 runs. It is correct on public semantics but
deliberately simple; the intended policy trades write cost, read cost, and disk
space more carefully as workloads become skewed.

## Metrics

- `read_amplification_bps` — logical bytes read to answer point reads, scans,
  and snapshots, expressed per ten-thousand requested bytes (minimize). The
  easy harness models each scan and snapshot key as a point lookup and charges
  the logical bytes of every run it searches. A memtable hit performs no run
  reads, and a Level-0 hit additionally probes overlapping Level-1 runs created
  at or after the hit's run, charging those probes.
- `bytes_rewritten` — logical entry bytes written by compaction merges
  (minimize).
- `compaction_stall_ticks` — number of compactions executed, each modeled as
  one virtual tick of write-path stall (minimize).
- `peak_disk_bytes` — maximum accounted bytes across all runs (minimize).

Correctness gates scoring. Public smoke output is reproducible but unofficial;
normalization and calibration belong to the platform and private judge.

## Verify

From the repository root:

```sh
go fmt ./arenas/lsm-compactor-easy/...
go test ./arenas/lsm-compactor-easy/...
go test -race ./arenas/lsm-compactor-easy/...
go vet ./arenas/lsm-compactor-easy/...
go test ./...
go run ./arenas/lsm-compactor-easy/cmd/smoke --seed 1844674407370955161
```

Run the smoke command twice with the same seed; the logical counters must be
identical. The command writes exactly one `bytearena.result/v1` JSON object to
stdout and uses no wall time, network access, environment secrets, or random
seeds.
