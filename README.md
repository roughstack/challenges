# Rough Stack challenges

This repository contains independently versioned, public systems-engineering
challenges for the Rough Stack platform. Each challenge owns its scenario, contestant
interface, starter, deterministic simulator, public tests, smoke benchmark,
baseline, and public metric contract.

The Rough Stack platform is maintained separately. Challenge packages depend on
versioned contracts; the platform consumes validated, immutable challenge bundles
and does not import challenge source code.

The current manifest filename and protocol fields retain the `arena` name for
compatibility with the pre-release platform. They will change only through a
versioned contract migration.

Official workloads, hidden cases, exploit tests, reference implementations, and
calibration data are intentionally absent from this repository.

## Verification

```bash
sh scripts/check-repository-hygiene.sh
gofmt -w challenges
go test ./...
go vet ./...
go test -race ./...
```

Changed manifests must also pass the Rough Stack platform validator. Every smoke
benchmark must emit one valid result object and reproduce the same canonical
result when repeated with the same seed.

## Initial build order

The first public implementation slice is `cache-pressure-easy`. The remaining
families are added one at a time after the preceding challenge passes tests,
determinism checks, and the platform manifest validator.

## Contributing and security

Read [CONTRIBUTING.md](CONTRIBUTING.md) before proposing a challenge. Security
problems must be reported privately as described in [SECURITY.md](SECURITY.md),
not through a public issue containing exploit details.

## License

Public challenge code and content are licensed under the
[Apache License 2.0](LICENSE) unless a challenge explicitly declares otherwise.
See [NOTICE](NOTICE) for attribution information.
