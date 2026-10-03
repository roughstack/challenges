# Chunk Store

A blob service stores versioned byte objects on a simulated block device.
Implement a fixed-size chunked, per-block compressed store that serves exact
random reads without decoding unrelated blobs.

## Scenario

The harness owns the injected `contract.Device`, workload generation,
corruption injection, accounting, correctness gates, and result construction.
The contestant owns only `contract.Store`.

Blobs are immutable and are identified by `(id, version)`. A blob is split
into fixed-size blocks. Each block is stored with a per-block header that
carries the raw length, encoded length, and a CRC-32 checksum. A block is
compressed only when the encoded payload is strictly smaller than the raw
payload; otherwise it is stored raw.

## Contract

Implement `contract.Store` and expose this constructor:

```go
func OpenStore(dir string, device contract.Device, memoryLimit uint64) (contract.Store, error)
```

The full surface lives in `contract/contract.go`:

- `Put(id uint64, version uint64, data io.Reader, size uint64) error` — store
  an immutable blob. `size` is validated against `contract.MaxBlobBytes` before
  allocation. A duplicate `(id, version)` returns `contract.ErrDuplicate`.
- `ReadAt(id uint64, version uint64, p []byte, off int64) (int, error)` — read
  exactly the requested logical range without decoding unrelated blobs. A
  negative offset returns `contract.ErrRange`; an offset at or beyond the blob
  returns `io.EOF`; a partial trailing read returns the available bytes plus
  `io.EOF`.
- `Delete(id uint64, version uint64) error` — remove a blob from the index.
  The easy tier does not reclaim device bytes.
- `Compact() error` — reserved for later tiers; the easy tier returns `nil`.
- `Close() error` — terminal and idempotent; every later method returns
  `contract.ErrClosed`.
- `IndexBytes() uint64` — report the deterministic in-memory index size used
  by the store.

Corrupt headers, invalid block lengths, checksum mismatches, and decompression
bombs are rejected before unbounded allocation. A declared block payload length
above `contract.BlockSize` returns `contract.ErrLengthBomb`.

## Metrics

- `stored_bytes` — device bytes occupied after the write phase (minimize).
- `random_read_logical_bytes` — device bytes read by `ReadAt` calls (minimize).
- `write_cpu_work` — deterministic ingest plus device-write work (minimize).
- `read_cpu_work` — deterministic requested-byte plus device-read work
  (minimize).
- `index_memory_bytes` — peak reported `IndexBytes` during the run (minimize).

Correctness gates scoring. Public smoke output is reproducible but unofficial;
normalization and calibration belong to the platform and private judge.

## Verify

From the repository root:

```sh
go fmt ./challenges/chunk-store-easy/...
go test ./challenges/chunk-store-easy/...
go test -race ./challenges/chunk-store-easy/...
go vet ./challenges/chunk-store-easy/...
go test ./...
go run ./challenges/chunk-store-easy/cmd/smoke --seed 1844674407370955161
```

Run the smoke command twice with the same seed; the logical counters must be
identical. The command writes exactly one `bytearena.result/v1` JSON object to
stdout and uses no wall time, network access, environment secrets, or random
seeds.
