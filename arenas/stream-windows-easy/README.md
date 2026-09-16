# Stream Windows — Easy

An observability pipeline receives timestamped service events. Implement a
streaming operator that computes an exact per-key count and sum over fixed,
non-overlapping event-time windows. Events arrive in order; a watermark
advances event time and makes older windows final.

## Scenario

The harness owns the event stream, the injected virtual clock, watermark
scheduling, output canonicalization, accounting, and result construction. The
contestant owns only the operator that receives events one at a time and returns
final window records. Loading or buffering the full stream is forbidden.

A window covers `[start, start+WindowSize)` where `start` is the largest
multiple of `WindowSize` at or below an event tick. When a watermark reaches
tick `t`, every window whose end is `<= t` must be emitted exactly once and
then evicted. Repeated equal or greater watermarks must not emit a duplicate.
`Close` finalizes every remaining active window.

## Non-goals

- No out-of-order events, allowed lateness, sliding windows, or session
  windows. Those belong to the medium and hard variants.
- No real time, goroutines, network access, environment secrets, or wall-clock
  timers. Time is a virtual tick owned by the harness.
- No hidden workloads, ranked seeds, or reference implementations in this
  repository.

## Operator contract

Implement `contract.Operator` and expose this constructor:

```go
func NewOperator(cfg contract.Config) contract.Operator
```

The full surface lives in `contract/contract.go`:

- `OnEvent(Event) []Output` — receive one in-order event and return any final
  outputs it produces. The easy tier expects none.
- `OnWatermark(tick uint64) []Output` — advance event time and emit every active
  window whose end is `<= tick`, exactly once.
- `Close() []Output` — terminal call; emit every remaining active window
  exactly once.
- `StateBytes() uint64` — report the current live operator state size in bytes.
  Finalized windows must be evicted, so this must reflect only active windows.

`Output` carries `WindowStart`, `WindowEnd`, `Key`, `Count`, `Sum`, and
`Final`. The harness compares final outputs as canonical records, not by
callback batch boundaries. A malformed, premature, non-final, duplicated, or
missing record fails the run.

`Config` carries `WindowSize` (zero falls back to 1000) and `EmitEmpty`. When
`EmitEmpty` is true the operator also emits empty windows whose end is crossed
by a watermark. When false, empty windows produce no output.

## Metrics

- `events_per_second` — events processed per logical second of injected virtual
  time (maximize).
- `peak_state_bytes` — maximum live state bytes reported by the operator
  (minimize).
- `output_delay` — total logical ticks between each window end and the tick at
  which the operator actually emitted it (minimize).
- `alloc_bytes` — heap bytes allocated by operator calls during the run,
  measured with garbage collection disabled for determinism (minimize).

Correctness gates scoring. Public smoke output is reproducible but unofficial;
normalization and calibration belong to the platform and private judge.

## Verify

From the repository root:

```sh
go fmt ./arenas/stream-windows-easy/...
go test ./arenas/stream-windows-easy/...
go test -race ./arenas/stream-windows-easy/...
go vet ./arenas/stream-windows-easy/...
go test ./...
go run ./arenas/stream-windows-easy/cmd/smoke --seed 1844674407370955161
```

Run the smoke command twice with the same seed; the logical counters must be
identical. The command writes exactly one `bytearena.result/v1` JSON object to
stdout and uses no wall time, network access, environment secrets, or random
seeds.
