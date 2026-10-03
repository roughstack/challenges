# Journal Recovery

A tiny key-value engine writes through a player-owned journal. The harness
injects torn writes, bit flips, and process death, then reopens the journal.
Implement a length-delimited record journal over an injected deterministic
Device and recover the longest valid prefix. Durable state is a protocol over
writes, flushes, ordering, and recovery — not just a file write.

## Scenario

The journal frames every record as:

```
magic(4) | version(1) | length(4, big-endian) | seq(8, big-endian) |
payload(length) | checksum(4, big-endian)
```

The checksum is CRC-32 (IEEE) over every preceding byte of the frame. The
harness owns the Device, its durability boundary, fault injection, crash
simulation, workload, accounting, correctness gates, and the result object.
The contestant owns only `contract.Journal`.

## Non-goals

- No real filesystem persistence, checkpoints, transactions, or shared
  packages. The Device is the only storage API; `dir` is reserved for the
  persistent tiers and ignored here.
- No wall-clock timing, network access, environment secrets, or random seeds.
  Workloads and fault schedules are deterministic from an explicit `uint64`
  seed.
- No hidden workloads, ranked seeds, or reference implementations in this
  repository.

## Contract

Implement `contract.Journal` and expose this constructor:

```go
func OpenJournal(dir string, device contract.Device) (contract.Journal, error)
```

The full surface lives in `contract/contract.go`:

- `Append(record Record) (uint64, error)` — frame the record, write it to the
  device, and return the assigned LSN. LSNs start at 1 and increase by one.
  Append copies the payload before returning; later mutation of the caller's
  slice must not change the journaled bytes. A record is not durable until
  `Sync`.
- `Sync(upto uint64) error` — make every record up to and including `upto`
  durable. Returns `ErrUnknownLSN` for an LSN that was never appended.
- `Recover(apply func(Record) error) (RecoveryInfo, error)` — scan the device
  and apply the longest valid prefix of records to `apply` in order.
- `Close() error` — terminal and idempotent; every later method returns
  `ErrClosed`.

Recovery semantics:

- A zero-length journal reports `StatusEmpty`.
- A journal that ends exactly on a frame boundary reports `StatusClean`.
- A partial header, or a plausible declared length that extends past the end
  of the device, is a torn tail: the valid prefix is recovered with
  `StatusTornTail` and no error.
- A trailing region that does not begin with a valid header and is not followed
  by any valid frame reports `StatusTailGarbage`; the valid prefix is recovered
  with no error.
- A complete frame that fails its checksum, a sequence gap or duplicate, a
  declared length above `MaxPayloadBytes`, or an interior malformed region
  followed by a valid frame, is corruption: recovery stops, reports
  `StatusCorrupt`, and returns an error. The valid prefix is applied before the
  error is returned.
- If `apply` returns an error, `Recover` stops and returns that error unchanged.

The starter uses one write plus one sync per append and reads the whole device
in one pass during recovery. It is semantically correct but non-optimal; the
intended optimization batches durability behind `Sync` and reads the journal
incrementally.

## Metrics

- `append_throughput` — records appended per 1000 device write/sync operations
  during the append phase (maximize).
- `recovery_bytes_read` — bytes read from the device during recovery
  (minimize).
- `write_overhead` — append-phase device operations beyond one write per
  appended record (minimize).
- `allocations` — all device read and write calls, one allocation each, a proxy
  for the buffers the journal must provide (minimize).
- `disk_bytes` — total bytes written to the device (minimize).

Correctness is a gate: a panic, malformed result, wrong LSN, mutated payload,
mismatched recovery, swallowed callback failure, or ignored corruption produces
a failed verdict and a score of zero. Public smoke output is reproducible but
unofficial; normalization and calibration belong to the platform and private
judge.

## Verify

From the repository root:

```sh
go fmt ./arenas/journal-recovery-easy/...
go test ./arenas/journal-recovery-easy/...
go test -race ./arenas/journal-recovery-easy/...
go vet ./arenas/journal-recovery-easy/...
go test ./...
go run ./arenas/journal-recovery-easy/cmd/smoke --seed 1844674407370955161
```

Run the smoke command twice with the same seed; the logical counters must be
identical. The command writes exactly one `bytearena.result/v1` JSON object to
stdout and uses no wall time, network access, environment secrets, or random
seeds.
