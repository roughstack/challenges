# Rough Stack challenges

This repository contains independently versioned, public systems-engineering
challenges for the Rough Stack platform. Each challenge owns its scenario, contestant
interface, learner scaffold, deterministic simulator, public tests, smoke
benchmark, public baseline, and metric contract.

The Rough Stack platform is maintained separately. Challenge packages depend on
versioned contracts. Today, a local platform checkout validates and reads a
separate challenge checkout from disk. Published immutable bundles are a future
distribution format, not a requirement for running or contributing locally.

The current manifest filename and protocol fields retain the `arena` name for
compatibility with the pre-release platform. They will change only through a
versioned contract migration.

Official workloads, hidden cases, exploit tests, private reference
implementations, and calibration data are intentionally absent from this
repository. Public baseline implementations remain visible for reproducibility.

Each package's manifest names two different source files. `submission.starter`
is the incomplete scaffold shown in the browser. `submission.entrypoint` is
the file replaced by submitted source inside a temporary execution workspace;
the repository copy is also the tested public baseline. Keeping these separate
lets learners start with a genuine problem without removing the baseline used
for public smoke and feasibility checks.

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

## Local platform integration

Clone this repository beside `roughstack/web-platform` and follow the
[web-platform local setup](https://github.com/roughstack/web-platform#running-it).
The platform's `BYTEARENA_ARENA_ROOTS` setting should point to this repository's
`challenges/` directory. Its opt-in real-run check submits the easy-cache public
baseline and learner scaffold to the local Docker runner. Public baselines are
practice feasibility witnesses; the generated arenas do not yet provide ranked
scores or multi-language submissions.

## Contributing and security

Read [CONTRIBUTING.md](CONTRIBUTING.md) before proposing a challenge. Security
problems must be reported privately as described in [SECURITY.md](SECURITY.md),
not through a public issue containing exploit details.

## License

Public challenge code and content are licensed under the
[Apache License 2.0](LICENSE) unless a challenge explicitly declares otherwise.
See [NOTICE](NOTICE) for attribution information.
