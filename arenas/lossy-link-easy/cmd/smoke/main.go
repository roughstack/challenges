// Command smoke runs the deterministic public lossy-link-easy workload and
// writes one bytearena.result/v1 JSON object to stdout. Diagnostics go to
// stderr only; no wall time, network access, or environment secrets are used.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/bytearena/arenas/arenas/lossy-link-easy/harness"
	"github.com/bytearena/arenas/arenas/lossy-link-easy/starter"
)

func main() {
	seed := flag.Uint64("seed", 12345, "deterministic public workload seed")
	fault := flag.String("fault", string(harness.FaultRandom), "fault profile: clean, drop-first-data, drop-first-ack, duplicate-all, corrupt-first-data, delay-first-ack, random")
	flag.Parse()

	config := harness.DefaultPublicConfig()
	if *fault != "" {
		config.Fault = harness.FaultProfile(*fault)
	}

	result := harness.Evaluate(*seed, config, starter.Factory)
	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		fmt.Fprintln(os.Stderr, "failed to encode result")
		os.Exit(1)
	}
	if result.Verdict != "pass" {
		os.Exit(1)
	}
}
