// Package starter is the learner-facing scaffold for a token-bucket gate.
package starter

import "github.com/roughstack/challenges/challenges/fair-gate-easy/contract"

type limiter struct {
	cfg contract.Config
	// TODO: track fractional tokens and the most recent tick.
}

func NewLimiter(cfg contract.Config) contract.Limiter {
	return &limiter{cfg: cfg}
}

func (l *limiter) Decide(req contract.Request) contract.Decision {
	panic("TODO: refill, charge once, and return the exact retry tick")
}
