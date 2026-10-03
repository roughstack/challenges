# Contributing public arenas

This repository contains only public arena material. Official workloads,
ranked seeds, exploit suites, calibration data, and private reference
implementations must never be proposed or discussed in a public pull request.

## Contribution flow

1. Use the arena-proposal issue template before implementing a new family or
   changing a frozen contestant contract.
2. Implement one arena variant per pull request.
3. Keep the contestant-owned API narrow and keep workload generation, invariant
   checks, accounting, and result construction in trusted harness code.
4. Make every workload and logical metric deterministic from an explicit
   `uint64` seed.
5. Use conventional commits and keep plan/status changes separate from an
   implementation commit.

## Required checks

```bash
sh scripts/check-repository-hygiene.sh
gofmt -w arenas/<variant-id>
go test ./...
go vet ./...
go test -race ./...
```

Also validate every changed `arena.yaml` with the Rough Stack platform validator
and run the variant's smoke command twice with the same full-range seed. The two
canonical result objects must match.

## Public/private boundary

Public tests should explain the contract, boundaries, determinism, and resource
accounting without mirroring an official workload distribution. Do not include
hidden cases, private paths, orchestration logs, generation prompts, secrets,
or material copied from a private judge repository.

By submitting a contribution, you agree that it is licensed under the Apache
License 2.0 in this repository.
