// Command smoke runs the deterministic public fair-gate-easy workload and
// writes one bytearena.result/v1 JSON object to stdout. Diagnostics go to
// stderr only; no wall time, network access, or environment secrets are used.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/roughstack/challenges/challenges/fair-gate-easy/harness"
	"github.com/roughstack/challenges/challenges/fair-gate-easy/starter"
)

func main() {
	seed := flag.Uint64("seed", 12345, "deterministic public workload seed")
	flag.Parse()

	result := harness.Evaluate(*seed, harness.DefaultPublicConfig(), starter.NewLimiter)
	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		fmt.Fprintln(os.Stderr, "failed to encode result")
		os.Exit(1)
	}
	if result.Verdict != "pass" {
		os.Exit(1)
	}
}
