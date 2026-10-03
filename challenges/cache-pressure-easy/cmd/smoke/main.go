package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/roughstack/challenges/challenges/cache-pressure-easy/harness"
	"github.com/roughstack/challenges/challenges/cache-pressure-easy/starter"
)

func main() {
	seed := flag.Uint64("seed", 12345, "deterministic public workload seed")
	flag.Parse()

	result := harness.Evaluate(*seed, harness.DefaultPublicConfig(), starter.NewCache)
	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		fmt.Fprintln(os.Stderr, "failed to encode result")
		os.Exit(1)
	}
	if result.Verdict != "pass" {
		os.Exit(1)
	}
}
