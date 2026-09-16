package harness

import (
	"encoding/json"
	"reflect"
	"sort"
	"testing"

	"github.com/bytearena/arenas/arenas/lsm-compactor-easy/contract"
	"github.com/bytearena/arenas/arenas/lsm-compactor-easy/starter"
)

func runFactory(seed uint64, config PublicConfig, factory contract.Factory) (*simulator, Result) {
	config, policyCfg := normalizeConfig(config)
	sim := newSimulator(seed, config, factory, policyCfg)
	sim.run()
	return sim, sim.result()
}

func hasViolation(result Result, want string) bool {
	for _, violation := range result.Violations {
		if violation == want {
			return true
		}
	}
	return false
}

func smallFlushConfig() PublicConfig {
	return PublicConfig{
		Operations:         8,
		KeySpace:           16,
		ValueBytes:         32,
		ScanLength:         4,
		MemtableFlushBytes: 1,
		Level0TriggerRuns:  1,
		MaxLevel0Runs:      8,
		BudgetBytes:        1 << 20,
	}
}

func TestEvaluateIsDeterministicAndPassesWithStarter(t *testing.T) {
	config := DefaultPublicConfig()
	first := Evaluate(^uint64(0), config, starter.Factory)
	second := Evaluate(^uint64(0), config, starter.Factory)

	if !reflect.DeepEqual(first, second) {
		t.Fatalf("same seed produced different results:\n%+v\n%+v", first, second)
	}
	if first.Verdict != "pass" || len(first.Violations) != 0 {
		t.Fatalf("unexpected verdict: %+v", first)
	}
	if first.Metrics.CompactionStallTicks == 0 {
		t.Fatal("smoke workload did not perform any compaction")
	}
	if first.Metrics.BytesRewritten == 0 {
		t.Fatal("smoke workload did not rewrite any bytes")
	}
	if first.Metrics.PeakDiskBytes == 0 {
		t.Fatal("smoke workload did not observe disk usage")
	}
	if first.Metrics.ReadAmplificationBPS == 0 {
		t.Fatal("smoke workload did not observe read amplification")
	}
}

func TestResultEnvelopeContainsEveryPublicMetric(t *testing.T) {
	result := Evaluate(7, DefaultPublicConfig(), starter.Factory)
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}

	var envelope map[string]any
	if err := json.Unmarshal(encoded, &envelope); err != nil {
		t.Fatal(err)
	}
	metrics, ok := envelope["metrics"].(map[string]any)
	if !ok {
		t.Fatal("result metrics are not an object")
	}
	for _, metric := range []string{"read_amplification_bps", "bytes_rewritten", "compaction_stall_ticks", "peak_disk_bytes"} {
		if _, ok := metrics[metric]; !ok {
			t.Fatalf("result is missing metric %q", metric)
		}
	}
	if envelope["seed"] != "7" {
		t.Fatalf("seed not preserved as a decimal string: %v", envelope["seed"])
	}
}

type noopPolicy struct{}

func (noopPolicy) Pick(contract.State) (contract.Plan, bool) { return contract.Plan{}, false }

func TestBelowThresholdDoesNotForceCompaction(t *testing.T) {
	config, policyCfg := normalizeConfig(PublicConfig{
		ValueBytes:         32,
		MemtableFlushBytes: 1,
		Level0TriggerRuns:  4,
		MaxLevel0Runs:      8,
		BudgetBytes:        1 << 20,
	})
	sim := newSimulator(1, config, nil, policyCfg)
	sim.policy = noopPolicy{}

	sim.write(1)
	sim.write(2)

	if len(sim.violations) != 0 {
		t.Fatalf("two small Level-0 runs produced violations: %+v", sim.violations)
	}
	if len(sim.levels[0]) != 2 {
		t.Fatalf("Level-0 run count = %d, want 2", len(sim.levels[0]))
	}
	if sim.compactionStalls != 0 {
		t.Fatalf("compaction was forced below threshold: %d stalls", sim.compactionStalls)
	}
}

func TestValidatePlanRequiresOverlappingLevel1Runs(t *testing.T) {
	sim := validationSim(
		[]*run{rangeRun(10, 0, 10, 20)},
		[]*run{rangeRun(20, 1, 5, 15), rangeRun(21, 1, 16, 25), rangeRun(22, 1, 100, 120)},
	)

	missing := contract.Plan{InputRunIDs: []uint64{10, 20}, TargetLevel: 1}
	if got := sim.validatePlan(missing); got != "compaction plan is missing an overlapping level-1 run" {
		t.Fatalf("missing-overlap plan validation = %q", got)
	}

	valid := contract.Plan{InputRunIDs: []uint64{10, 20, 21}, TargetLevel: 1}
	if got := sim.validatePlan(valid); got != "" {
		t.Fatalf("valid overlap plan rejected: %q", got)
	}
}

func TestValidatePlanRejectsDistantLevel1Run(t *testing.T) {
	sim := validationSim(
		[]*run{rangeRun(10, 0, 10, 20)},
		[]*run{rangeRun(20, 1, 15, 25), rangeRun(30, 1, 100, 120)},
	)

	plan := contract.Plan{InputRunIDs: []uint64{10, 20, 30}, TargetLevel: 1}
	if got := sim.validatePlan(plan); got != "compaction plan includes a level-1 run that does not overlap the selected level-0 range" {
		t.Fatalf("distant Level-1 plan validation = %q", got)
	}
}

func TestValidatePlanRejectsDuplicateRunIDs(t *testing.T) {
	sim := validationSim(
		[]*run{rangeRun(10, 0, 10, 20)},
		[]*run{rangeRun(20, 1, 5, 15)},
	)

	plan := contract.Plan{InputRunIDs: []uint64{10, 10}, TargetLevel: 1}
	if got := sim.validatePlan(plan); got != "compaction plan contains duplicate run ids" {
		t.Fatalf("duplicate-run plan validation = %q", got)
	}
}

func TestValidatePlanRejectsWrongTargetLevel(t *testing.T) {
	sim := validationSim(
		[]*run{rangeRun(10, 0, 10, 20)},
		nil,
	)

	plan := contract.Plan{InputRunIDs: []uint64{10}, TargetLevel: 0}
	if got := sim.validatePlan(plan); got != "compaction target level must be level-1" {
		t.Fatalf("wrong-target plan validation = %q", got)
	}
}

func TestValidatePlanRejectsLevel1OnlyPlan(t *testing.T) {
	sim := validationSim(
		[]*run{rangeRun(10, 0, 10, 20)},
		[]*run{rangeRun(20, 1, 5, 25)},
	)

	plan := contract.Plan{InputRunIDs: []uint64{20}, TargetLevel: 1}
	if got := sim.validatePlan(plan); got != "compaction plan must include at least one level-0 run" {
		t.Fatalf("level-1-only plan validation = %q", got)
	}
}

func TestValidatePlanRejectsInvalidLevelRun(t *testing.T) {
	bad := rangeRun(10, 0, 10, 20)
	bad.level = 2
	sim := validationSim([]*run{bad}, nil)

	plan := contract.Plan{InputRunIDs: []uint64{10}, TargetLevel: 1}
	if got := sim.validatePlan(plan); got != "compaction plan references a run from an invalid level" {
		t.Fatalf("invalid-level plan validation = %q", got)
	}
}

type invalidPlanPolicy struct{}

func (invalidPlanPolicy) Pick(contract.State) (contract.Plan, bool) { return contract.Plan{}, true }

func TestInvalidPlanRejected(t *testing.T) {
	config, policyCfg := normalizeConfig(smallFlushConfig())
	sim := newSimulator(2, config, nil, policyCfg)
	sim.policy = invalidPlanPolicy{}
	sim.write(1)

	result := sim.result()
	if result.Verdict != "fail" {
		t.Fatalf("invalid plan was not failed: %+v", result)
	}
	if !hasViolation(result, "compaction plan is empty") {
		t.Fatalf("missing empty-plan violation: %+v", result.Violations)
	}
}

type nonExistentPolicy struct{}

func (nonExistentPolicy) Pick(contract.State) (contract.Plan, bool) {
	return contract.Plan{InputRunIDs: []uint64{999}, TargetLevel: 1}, true
}

func TestNonExistentRunIDRejected(t *testing.T) {
	config, policyCfg := normalizeConfig(smallFlushConfig())
	sim := newSimulator(3, config, nil, policyCfg)
	sim.policy = nonExistentPolicy{}
	sim.write(1)

	result := sim.result()
	if result.Verdict != "fail" {
		t.Fatalf("non-existent run id was not failed: %+v", result)
	}
	if !hasViolation(result, "compaction plan references a non-existent run") {
		t.Fatalf("missing non-existent-run violation: %+v", result.Violations)
	}
}

type stalePolicy struct {
	firstID uint64
	used    bool
}

func (p *stalePolicy) Pick(state contract.State) (contract.Plan, bool) {
	if len(state.Levels[0]) == 0 {
		return contract.Plan{}, false
	}
	if !p.used {
		p.used = true
		p.firstID = state.Levels[0][0].ID
		return contract.Plan{InputRunIDs: []uint64{p.firstID}, TargetLevel: 1}, true
	}
	return contract.Plan{InputRunIDs: []uint64{p.firstID}, TargetLevel: 1}, true
}

func TestStalePlanRejected(t *testing.T) {
	config, policyCfg := normalizeConfig(smallFlushConfig())
	sim := newSimulator(4, config, nil, policyCfg)
	sim.policy = &stalePolicy{}

	sim.write(1)
	sim.write(2)

	result := sim.result()
	if result.Verdict != "fail" {
		t.Fatalf("stale plan was not failed: %+v", result)
	}
	if !hasViolation(result, "compaction plan references a non-existent run") {
		t.Fatalf("missing stale-plan violation: %+v", result.Violations)
	}
}

type oversizedPolicy struct{}

func (oversizedPolicy) Pick(contract.State) (contract.Plan, bool) {
	ids := make([]uint64, 300)
	for index := range ids {
		ids[index] = uint64(index + 1)
	}
	return contract.Plan{InputRunIDs: ids, TargetLevel: 1}, true
}

func TestOversizedPlanRejected(t *testing.T) {
	config, policyCfg := normalizeConfig(smallFlushConfig())
	sim := newSimulator(5, config, nil, policyCfg)
	sim.policy = oversizedPolicy{}
	sim.write(1)

	result := sim.result()
	if result.Verdict != "fail" {
		t.Fatalf("oversized plan was not failed: %+v", result)
	}
	if !hasViolation(result, "compaction plan exceeds input run limit") {
		t.Fatalf("missing oversized-plan violation: %+v", result.Violations)
	}
}

type panickingPolicy struct{}

func (panickingPolicy) Pick(contract.State) (contract.Plan, bool) { panic("boom") }

func TestPolicyPanicRejected(t *testing.T) {
	config, policyCfg := normalizeConfig(smallFlushConfig())
	sim := newSimulator(6, config, nil, policyCfg)
	sim.policy = panickingPolicy{}
	sim.write(1)

	result := sim.result()
	if result.Verdict != "fail" {
		t.Fatalf("panicking policy was not failed: %+v", result)
	}
	if !hasViolation(result, "contestant policy panicked") {
		t.Fatalf("missing policy-panic violation: %+v", result.Violations)
	}
}

type countingPolicy struct {
	calls *int
}

func (p *countingPolicy) Pick(state contract.State) (contract.Plan, bool) {
	if p.calls != nil {
		*p.calls = *p.calls + 1
	}
	if len(state.Levels[0]) == 0 {
		return contract.Plan{}, false
	}
	return validPlanForState(state), true
}

func TestHostileAlwaysCompactPolicyTerminates(t *testing.T) {
	calls := 0
	factory := contract.Factory(func(contract.Config) contract.Policy {
		return &countingPolicy{calls: &calls}
	})

	sim, result := runFactory(7, smallFlushConfig(), factory)
	if result.Verdict != "pass" {
		t.Fatalf("always-compact policy failed: %+v", result)
	}
	if calls != int(sim.flushCount) {
		t.Fatalf("policy was called %d times for %d flushes; expected exactly one Pick per flush", calls, sim.flushCount)
	}
}

func TestNilPolicyFactoryRejected(t *testing.T) {
	result := Evaluate(8, DefaultPublicConfig(), nil)
	if result.Verdict != "fail" {
		t.Fatalf("nil policy factory was not failed: %+v", result)
	}
	if !hasViolation(result, "policy factory is nil") {
		t.Fatalf("missing nil-factory violation: %+v", result.Violations)
	}
}

func TestNilPolicyRejected(t *testing.T) {
	factory := contract.Factory(func(contract.Config) contract.Policy { return nil })
	_, result := runFactory(9, DefaultPublicConfig(), factory)
	if result.Verdict != "fail" {
		t.Fatalf("nil policy was not failed: %+v", result)
	}
	if !hasViolation(result, "policy factory returned a nil policy") {
		t.Fatalf("missing nil-policy violation: %+v", result.Violations)
	}
}

func TestGiantSingleFlushPasses(t *testing.T) {
	config := PublicConfig{
		Operations:         300,
		KeySpace:           32,
		ValueBytes:         64,
		ScanLength:         8,
		MemtableFlushBytes: 1 << 20,
		Level0TriggerRuns:  4,
		MaxLevel0Runs:      8,
		BudgetBytes:        1 << 20,
	}
	result := Evaluate(10, config, starter.Factory)
	if result.Verdict != "pass" {
		t.Fatalf("giant single flush failed: %+v", result.Violations)
	}
	if result.Metrics.CompactionStallTicks != 0 {
		t.Fatalf("giant single flush performed %d compactions, want 0", result.Metrics.CompactionStallTicks)
	}
	if result.Metrics.PeakDiskBytes == 0 {
		t.Fatal("giant single flush did not observe disk usage")
	}
}

func TestTombstoneOnlyCompactionPasses(t *testing.T) {
	config, policyCfg := normalizeConfig(PublicConfig{
		ValueBytes:         32,
		MemtableFlushBytes: 1,
		Level0TriggerRuns:  1,
		MaxLevel0Runs:      4,
		BudgetBytes:        1 << 20,
	})
	sim := newSimulator(11, config, nil, policyCfg)
	sim.policy = starter.NewPolicy(policyCfg)
	sim.delete(5)

	if len(sim.violations) != 0 {
		t.Fatalf("tombstone-only compaction failed: %+v", sim.violations)
	}
	if len(sim.levels[1]) != 1 || sim.levels[1][0].tombstones != 1 {
		t.Fatalf("tombstone-only compaction produced unexpected Level-1: %+v", sim.levels[1])
	}
}

func TestEmptyRunCompactionPasses(t *testing.T) {
	config, policyCfg := normalizeConfig(PublicConfig{
		ValueBytes:         32,
		MemtableFlushBytes: 1,
		Level0TriggerRuns:  1,
		MaxLevel0Runs:      4,
		BudgetBytes:        1 << 20,
	})
	sim := newSimulator(12, config, nil, policyCfg)
	sim.policy = starter.NewPolicy(policyCfg)
	sim.levels[0] = append(sim.levels[0], newRun(1, 0, 1, nil))
	sim.considerCompaction()

	if len(sim.violations) != 0 {
		t.Fatalf("empty-run compaction failed: %+v", sim.violations)
	}
	if len(sim.levels[1]) != 1 || sim.levels[1][0].hasKeys() {
		t.Fatalf("empty-run compaction produced unexpected Level-1: %+v", sim.levels[1])
	}
}

func TestMergeEntriesTieBreaking(t *testing.T) {
	t.Run("newest CreatedTick wins", func(t *testing.T) {
		older := newRun(1, 0, 10, []entry{{key: 5, version: 100, bytes: 111}})
		newer := newRun(2, 0, 20, []entry{{key: 5, version: 1, bytes: 222}})

		merged := mergeEntries([]*run{older, newer})
		if len(merged) != 1 {
			t.Fatalf("merged %d entries, want 1", len(merged))
		}
		if merged[0].version != 1 || merged[0].bytes != 222 {
			t.Fatalf("merged entry = %+v, want the entry from the newer CreatedTick run", merged[0])
		}
	})

	t.Run("higher Version wins on equal CreatedTick", func(t *testing.T) {
		low := newRun(1, 0, 10, []entry{{key: 5, version: 1, bytes: 111}})
		high := newRun(2, 0, 10, []entry{{key: 5, version: 9, bytes: 222}})

		merged := mergeEntries([]*run{low, high})
		if len(merged) != 1 {
			t.Fatalf("merged %d entries, want 1", len(merged))
		}
		if merged[0].version != 9 || merged[0].bytes != 222 {
			t.Fatalf("merged entry = %+v, want the higher-version entry", merged[0])
		}
	})

	t.Run("higher run id wins on equal CreatedTick and Version", func(t *testing.T) {
		lowID := newRun(1, 0, 10, []entry{{key: 5, version: 9, bytes: 111}})
		highID := newRun(2, 0, 10, []entry{{key: 5, version: 9, bytes: 222}})

		merged := mergeEntries([]*run{lowID, highID})
		if len(merged) != 1 {
			t.Fatalf("merged %d entries, want 1", len(merged))
		}
		if merged[0].bytes != 222 {
			t.Fatalf("merged entry = %+v, want the entry from the higher run id", merged[0])
		}
	})
}

func TestReadAccountingChargesSearchedRuns(t *testing.T) {
	memtableSim := &simulator{
		memtable: []entry{{key: 5, version: 1, bytes: 48}},
		levels: [2][]*run{
			{newRun(1, 0, 10, []entry{{key: 5, version: 1, bytes: 48}})},
			{newRun(2, 1, 20, []entry{{key: 5, version: 1, bytes: 48}})},
		},
	}
	if _, ok := memtableSim.lsmRead(5); !ok {
		t.Fatal("memtable hit was not found")
	}
	if memtableSim.logicalReadBytes != 0 {
		t.Fatalf("memtable hit charged %d run bytes, want 0", memtableSim.logicalReadBytes)
	}

	overlapA := newRun(1, 0, 10, []entry{{key: 5, version: 1, bytes: 48}, {key: 6, version: 1, bytes: 48}})
	overlapB := newRun(2, 0, 20, []entry{{key: 5, version: 2, bytes: 48}, {key: 9, version: 1, bytes: 48}})
	distant := newRun(3, 0, 30, []entry{{key: 100, version: 1, bytes: 48}})
	level0Sim := &simulator{levels: [2][]*run{{overlapA, overlapB, distant}, nil}}

	if _, ok := level0Sim.lsmRead(5); !ok {
		t.Fatal("level-0 hit was not found")
	}
	want := overlapA.bytes + overlapB.bytes
	if level0Sim.logicalReadBytes != want {
		t.Fatalf("level-0 hit charged %d bytes, want %d (only overlapping Level-0 runs)", level0Sim.logicalReadBytes, want)
	}
}

type compactNewestOnlyPolicy struct{}

func (compactNewestOnlyPolicy) Pick(state contract.State) (contract.Plan, bool) {
	if len(state.Levels[0]) < 2 {
		return contract.Plan{}, false
	}
	newestID := state.Levels[0][0].ID
	for _, run := range state.Levels[0] {
		if run.ID > newestID {
			newestID = run.ID
		}
	}
	return contract.Plan{InputRunIDs: []uint64{newestID}, TargetLevel: 1}, true
}

func staleReadSim() *simulator {
	config, policyCfg := normalizeConfig(PublicConfig{
		ValueBytes:         32,
		MemtableFlushBytes: 1,
		Level0TriggerRuns:  4,
		MaxLevel0Runs:      8,
		BudgetBytes:        1 << 20,
	})
	sim := newSimulator(42, config, nil, policyCfg)
	sim.policy = compactNewestOnlyPolicy{}
	sim.write(5)
	sim.write(5)
	return sim
}

func TestSubsetCompactionReadReturnsNewestEntry(t *testing.T) {
	const key = uint64(5)

	sim := staleReadSim()
	if len(sim.levels[0]) != 1 || len(sim.levels[1]) != 1 {
		t.Fatalf("unexpected levels after subset compaction: L0=%d L1=%d", len(sim.levels[0]), len(sim.levels[1]))
	}
	wantBytes := sim.levels[0][0].bytes + sim.levels[1][0].bytes

	actual, ok := sim.lsmRead(key)
	if !ok {
		t.Fatal("read did not find the key")
	}
	if actual.version != 2 {
		t.Fatalf("read returned version %d, want newest version 2", actual.version)
	}
	if sim.logicalReadBytes != wantBytes {
		t.Fatalf("read charged %d logical bytes, want %d (Level-0 hit plus newer Level-1 probe)", sim.logicalReadBytes, wantBytes)
	}

	fresh := staleReadSim()
	fresh.read(key)
	if len(fresh.violations) != 0 {
		t.Fatalf("read after subset compaction reported violations: %v", fresh.violations)
	}
	if fresh.logicalReadBytes != wantBytes {
		t.Fatalf("read path charged %d logical bytes, want %d", fresh.logicalReadBytes, wantBytes)
	}
}

func validationSim(l0, l1 []*run) *simulator {
	return &simulator{levels: [2][]*run{l0, l1}}
}

func rangeRun(id uint64, level uint32, minKey, maxKey uint64) *run {
	entries := []entry{{key: minKey, version: 1, bytes: 80}}
	if minKey != maxKey {
		entries = append(entries, entry{key: maxKey, version: 1, bytes: 80})
	}
	return newRun(id, level, id, entries)
}

func validPlanForState(state contract.State) contract.Plan {
	ids := make([]uint64, 0, len(state.Levels[0])+len(state.Levels[1]))
	for _, run := range state.Levels[0] {
		ids = append(ids, run.ID)
	}

	minKey, maxKey, hasRange := metaLevel0Range(state.Levels[0])
	if hasRange {
		for _, run := range state.Levels[1] {
			if run.OverlapsRange(minKey, maxKey) {
				ids = append(ids, run.ID)
			}
		}
	}

	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return contract.Plan{InputRunIDs: ids, TargetLevel: 1}
}

func metaLevel0Range(runs []contract.RunMeta) (uint64, uint64, bool) {
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
