# Rough Stack public arenas

This repository contains independently versioned, public systems-engineering
arenas for the Rough Stack platform. Each arena owns its scenario, contestant
interface, starter, deterministic simulator, public tests, smoke benchmark,
baseline, and public metric contract.

The Rough Stack platform is maintained separately. Arena packages depend on its
versioned contracts; the platform consumes validated, immutable arena bundles and
does not import arena source code.

Official workloads, hidden cases, exploit tests, reference implementations, and
calibration data are intentionally absent from this repository.

## Verification

```bash
sh scripts/check-repository-hygiene.sh
gofmt -w arenas
go test ./...
go vet ./...
go test -race ./...
```

Changed manifests must also pass the Rough Stack platform validator. Every smoke
benchmark must emit one valid result object and reproduce the same canonical
result when repeated with the same seed.

## Initial build order

The first public implementation slice is `cache-pressure-easy`. The remaining
families are added one at a time after the preceding arena passes tests,
determinism checks, and the platform manifest validator.

## Contributing and security

Read [CONTRIBUTING.md](CONTRIBUTING.md) before proposing an arena. Security
problems must be reported privately as described in [SECURITY.md](SECURITY.md),
not through a public issue containing exploit details.

## License

Public arena code and content are licensed under the
[Apache License 2.0](LICENSE) unless an arena explicitly declares otherwise.
See [NOTICE](NOTICE) for attribution information.
