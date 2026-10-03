// Package starter contains a deliberately simple, correct compaction policy.
// It waits for a plain Level-0 run-count threshold, then compacts every
// Level-0 run together with the Level-1 runs their combined range overlaps.
package starter

import (
	"sort"

	"github.com/roughstack/challenges/challenges/lsm-compactor-easy/contract"
)

// Factory constructs the public policy. It is referenced by the harness and
// smoke command only; the contestant replaces the body behind this constructor.
var Factory = contract.Factory(NewPolicy)

type policy struct {
	trigger uint64
}

// NewPolicy returns a correct threshold-triggered compaction policy.
func NewPolicy(cfg contract.Config) contract.Policy {
	return &policy{trigger: cfg.Level0TriggerRunsEffective()}
}

func (p *policy) Pick(state contract.State) (contract.Plan, bool) {
	if len(state.Levels) == 0 {
		return contract.Plan{}, false
	}
	if uint64(len(state.Levels[0])) < p.trigger {
		return contract.Plan{}, false
	}

	l1 := levelRuns(state, 1)
	ids := make([]uint64, 0, len(state.Levels[0])+len(l1))
	for _, run := range state.Levels[0] {
		ids = append(ids, run.ID)
	}

	minKey, maxKey, hasRange := level0Range(state.Levels[0])
	if hasRange {
		for _, run := range l1 {
			if run.OverlapsRange(minKey, maxKey) {
				ids = append(ids, run.ID)
			}
		}
	}

	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return contract.Plan{InputRunIDs: ids, TargetLevel: 1}, true
}

func levelRuns(state contract.State, level int) []contract.RunMeta {
	if len(state.Levels) <= level {
		return nil
	}
	return state.Levels[level]
}

func level0Range(runs []contract.RunMeta) (uint64, uint64, bool) {
	var minKey, maxKey uint64
	hasRange := false
	for _, run := range runs {
		if run.Entries == 0 {
			continue
		}
		if !hasRange {
			minKey, maxKey = run.MinKey, run.MaxKey
			hasRange = true
			continue
		}
		if run.MinKey < minKey {
			minKey = run.MinKey
		}
		if run.MaxKey > maxKey {
			maxKey = run.MaxKey
		}
	}
	return minKey, maxKey, hasRange
}
