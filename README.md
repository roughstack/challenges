# ByteArena public arenas

This repository contains independently versioned, public systems-engineering
arenas for the ByteArena platform. Each arena owns its scenario, contestant
interface, starter, deterministic simulator, public tests, smoke benchmark,
baseline, and public metric contract.

The ByteArena platform is maintained separately. Arena packages depend on its
versioned contracts; the platform consumes validated, immutable arena bundles and
does not import arena source code.

Official workloads, hidden cases, exploit tests, reference implementations, and
calibration data are intentionally absent from this repository.

## Initial build order

The first public implementation slice is `cache-pressure-easy`. The remaining
families are added one at a time after the preceding arena passes tests,
determinism checks, and the platform manifest validator.

