# Security policy

Report vulnerabilities through GitHub's **Report a vulnerability** function.
Do not publish exploit details in issues, discussions, pull requests, challenge
fixtures, or smoke output.

Relevant reports include harness escapes, invariant bypasses, score forgery,
resource-accounting bypasses, nondeterminism, path traversal, output injection,
denial of service, and leakage of official-evaluation material.

Maintainers aim to acknowledge reports within three business days and provide
an initial assessment within seven business days. Security fixes target the
current `main` branch until versioned releases are published.

Public challenge harnesses are defense-in-depth and are not, by themselves, the
host sandbox. Do not run untrusted submissions outside the platform's isolated
execution environment.
