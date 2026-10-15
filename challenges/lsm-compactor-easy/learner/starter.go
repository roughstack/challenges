// Package starter is the learner-facing scaffold for an LSM merge policy.
package starter

import "github.com/roughstack/challenges/challenges/lsm-compactor-easy/contract"

type policy struct {
	cfg contract.Config
}

func NewPolicy(cfg contract.Config) contract.Policy {
	return &policy{cfg: cfg}
}

func (p *policy) Pick(state contract.State) (contract.Plan, bool) {
	panic("TODO: choose a legal overlap-closed compaction plan")
}
