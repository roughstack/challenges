// Command smoke runs the deterministic public benchmark and writes one
// bytearena.result/v1 JSON object to stdout.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/bytearena/arenas/arenas/deadline-scheduler-easy/harness"
	"github.com/bytearena/arenas/arenas/deadline-scheduler-easy/starter"
)

func main() {
	seed := flag.Uint64("seed", 12345, "deterministic public workload seed")
	flag.Parse()

	result := harness.Evaluate(*seed, harness.DefaultPublicConfig(), starter.NewScheduler)
	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		fmt.Fprintln(os.Stderr, "failed to encode result")
		os.Exit(1)
	}
	if result.Verdict != "pass" {
		os.Exit(1)
	}
}
