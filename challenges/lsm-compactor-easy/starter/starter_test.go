package starter

import (
	"reflect"
	"testing"

	"github.com/roughstack/challenges/challenges/lsm-compactor-easy/contract"
)

func runMeta(id, level, entries, minKey, maxKey uint64) contract.RunMeta {
	return contract.RunMeta{
		ID:          id,
		Level:       level,
		Bytes:       entries * 80,
		Entries:     entries,
		MinKey:      minKey,
		MaxKey:      maxKey,
		CreatedTick: id,
	}
}

func tombstoneRun(id, level, minKey, maxKey uint64) contract.RunMeta {
	meta := runMeta(id, level, 3, minKey, maxKey)
	meta.Tombstones = meta.Entries
	return meta
}

func emptyRun(id, level uint64) contract.RunMeta {
	return contract.RunMeta{ID: id, Level: level, CreatedTick: id}
}

func stateWith(level0, level1 []contract.RunMeta) contract.State {
	return contract.State{
		Levels:      [][]contract.RunMeta{level0, level1},
		DiskBytes:   1024,
		BudgetBytes: 4096,
		NowTick:     100,
	}
}

func policyWithTrigger(trigger uint64) contract.Policy {
	return NewPolicy(contract.Config{Level0TriggerRuns: trigger})
}

func TestBelowThresholdProducesNoPlan(t *testing.T) {
	state := stateWith(
		[]contract.RunMeta{runMeta(1, 0, 2, 10, 20), runMeta(2, 0, 2, 30, 40)},
		nil,
	)
	_, ok := policyWithTrigger(4).Pick(state)
	if ok {
		t.Fatal("two small Level-0 runs should not produce a mandatory compaction")
	}
}

func TestTriggerSelectsAValidPlan(t *testing.T) {
	state := stateWith(
		[]contract.RunMeta{
			runMeta(1, 0, 2, 10, 20),
			runMeta(2, 0, 2, 30, 40),
			runMeta(3, 0, 2, 50, 60),
			runMeta(4, 0, 2, 70, 80),
		},
		nil,
	)
	plan, ok := policyWithTrigger(4).Pick(state)
	if !ok {
		t.Fatal("exceeding the published Level-0 threshold did not select a plan")
	}
	if plan.TargetLevel != 1 {
		t.Fatalf("target level = %d, want 1", plan.TargetLevel)
	}
	if len(plan.InputRunIDs) != 4 {
		t.Fatalf("input runs = %v, want all four Level-0 runs", plan.InputRunIDs)
	}
}

func TestRequiredOverlapIncludesBothLevel1Runs(t *testing.T) {
	state := stateWith(
		[]contract.RunMeta{runMeta(10, 0, 3, 10, 60)},
		[]contract.RunMeta{
			runMeta(20, 1, 2, 5, 15),
			runMeta(21, 1, 2, 50, 65),
			runMeta(22, 1, 2, 100, 120),
		},
	)
	plan, ok := policyWithTrigger(1).Pick(state)
	if !ok {
		t.Fatal("overlapping Level-0 run did not produce a plan")
	}
	for _, id := range []uint64{10, 20, 21} {
		if !contains(plan.InputRunIDs, id) {
			t.Fatalf("plan %v is missing required run %d", plan.InputRunIDs, id)
		}
	}
}

func TestNonOverlapExcludesDistantLevel1Run(t *testing.T) {
	state := stateWith(
		[]contract.RunMeta{runMeta(10, 0, 3, 10, 20)},
		[]contract.RunMeta{runMeta(20, 1, 2, 15, 25), runMeta(30, 1, 2, 100, 120)},
	)
	plan, ok := policyWithTrigger(1).Pick(state)
	if !ok {
		t.Fatal("overlapping Level-0 run did not produce a plan")
	}
	if contains(plan.InputRunIDs, 30) {
		t.Fatalf("distant Level-1 run was unnecessarily included: %v", plan.InputRunIDs)
	}
	if !contains(plan.InputRunIDs, 20) {
		t.Fatalf("overlapping Level-1 run was not included: %v", plan.InputRunIDs)
	}
}

func TestIdenticalRangesAreIncluded(t *testing.T) {
	state := stateWith(
		[]contract.RunMeta{runMeta(10, 0, 3, 50, 80)},
		[]contract.RunMeta{runMeta(20, 1, 2, 50, 80)},
	)
	plan, ok := policyWithTrigger(1).Pick(state)
	if !ok {
		t.Fatal("identical-range Level-0 run did not produce a plan")
	}
	if !contains(plan.InputRunIDs, 20) {
		t.Fatalf("identical-range Level-1 run was not included: %v", plan.InputRunIDs)
	}
}

func TestEmptyRunsAreHandledWithoutPanic(t *testing.T) {
	state := stateWith(
		[]contract.RunMeta{emptyRun(1, 0), emptyRun(2, 0), emptyRun(3, 0), emptyRun(4, 0)},
		[]contract.RunMeta{emptyRun(5, 1)},
	)
	plan, ok := policyWithTrigger(4).Pick(state)
	if !ok {
		t.Fatal("empty Level-0 runs did not produce a plan")
	}
	if plan.TargetLevel != 1 {
		t.Fatalf("target level = %d, want 1", plan.TargetLevel)
	}
}

func TestTombstoneOnlyRunsAreHandled(t *testing.T) {
	state := stateWith(
		[]contract.RunMeta{tombstoneRun(1, 0, 10, 20)},
		[]contract.RunMeta{tombstoneRun(2, 1, 5, 25)},
	)
	plan, ok := policyWithTrigger(1).Pick(state)
	if !ok {
		t.Fatal("tombstone-only run did not produce a plan")
	}
	if !contains(plan.InputRunIDs, 1) || !contains(plan.InputRunIDs, 2) {
		t.Fatalf("tombstone-only overlap was not included: %v", plan.InputRunIDs)
	}
}

func TestSkewedRunSizesAreHandled(t *testing.T) {
	small := runMeta(1, 0, 2, 10, 12)
	small.Bytes = 2 * 80
	giant := runMeta(2, 0, 10000, 13, 500)
	giant.Bytes = 10000 * 80

	state := stateWith([]contract.RunMeta{small, giant}, nil)
	plan, ok := policyWithTrigger(2).Pick(state)
	if !ok {
		t.Fatal("skewed Level-0 runs did not produce a plan")
	}
	if len(plan.InputRunIDs) != 2 {
		t.Fatalf("input runs = %v, want both Level-0 runs", plan.InputRunIDs)
	}
}

func TestMaxUint64KeysAreHandled(t *testing.T) {
	state := stateWith(
		[]contract.RunMeta{runMeta(1, 0, 2, ^uint64(0)-10, ^uint64(0))},
		[]contract.RunMeta{runMeta(2, 1, 2, ^uint64(0)-5, ^uint64(0))},
	)
	plan, ok := policyWithTrigger(1).Pick(state)
	if !ok {
		t.Fatal("maximum uint64 keys did not produce a plan")
	}
	if !contains(plan.InputRunIDs, 2) {
		t.Fatalf("maximum-key overlap was not included: %v", plan.InputRunIDs)
	}
}

func TestPickIsDeterministic(t *testing.T) {
	state := stateWith(
		[]contract.RunMeta{runMeta(1, 0, 2, 10, 20), runMeta(2, 0, 2, 15, 25)},
		[]contract.RunMeta{runMeta(3, 1, 2, 12, 22)},
	)
	first, firstOK := policyWithTrigger(1).Pick(state)
	second, secondOK := policyWithTrigger(1).Pick(state)
	if firstOK != secondOK || !reflect.DeepEqual(first, second) {
		t.Fatalf("same state produced different plans:\n%+v\n%+v", first, second)
	}
}

func contains(ids []uint64, want uint64) bool {
	for _, id := range ids {
		if id == want {
			return true
		}
	}
	return false
}
