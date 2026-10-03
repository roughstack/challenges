package harness

import (
	"encoding/json"
	"testing"

	"github.com/roughstack/challenges/challenges/stream-windows-easy/contract"
	"github.com/roughstack/challenges/challenges/stream-windows-easy/starter"
	"github.com/roughstack/challenges/challenges/stream-windows-easy/workload"
)

func ev(tick, key uint64, value int64) workload.Step {
	return workload.Step{Kind: workload.StepEvent, Tick: tick, Key: key, Value: value}
}

func wm(tick uint64) workload.Step {
	return workload.Step{Kind: workload.StepWatermark, Tick: tick}
}

func runSteps(seed uint64, cfg contract.Config, steps ...workload.Step) Result {
	return runStream(seed, cfg, starter.Factory, steps).result()
}

func runFactory(seed uint64, cfg contract.Config, factory contract.Factory, steps ...workload.Step) Result {
	return runStream(seed, cfg, factory, steps).result()
}

func hasViolation(result Result, want string) bool {
	for _, violation := range result.Violations {
		if violation == want {
			return true
		}
	}
	return false
}

func TestBasicAggregationPasses(t *testing.T) {
	result := runSteps(1, contract.Config{WindowSize: 10}, ev(0, 1, 2), ev(1, 1, 3), wm(10))
	if result.Verdict != "pass" || len(result.Violations) != 0 {
		t.Fatalf("basic aggregation failed: %+v", result.Violations)
	}
	if result.Metrics.EventsPerSecond == 0 {
		t.Fatal("events_per_second was zero")
	}
	if result.Metrics.OutputDelay != 0 {
		t.Fatalf("output_delay = %d, want 0", result.Metrics.OutputDelay)
	}
	if result.Metrics.PeakStateBytes == 0 {
		t.Fatal("peak_state_bytes did not observe live state")
	}
}

func TestBoundaryAssignmentPasses(t *testing.T) {
	result := runSteps(2, contract.Config{WindowSize: 10}, ev(9, 7, 1), ev(10, 7, 2), wm(10), wm(20))
	if result.Verdict != "pass" || len(result.Violations) != 0 {
		t.Fatalf("boundary assignment failed: %+v", result.Violations)
	}
}

func TestEmptyWindowProducesNoOutput(t *testing.T) {
	result := runSteps(3, contract.Config{WindowSize: 10}, wm(10))
	if result.Verdict != "pass" || len(result.Violations) != 0 {
		t.Fatalf("empty window failed: %+v", result.Violations)
	}
}

func TestEmitEmptyConfigPasses(t *testing.T) {
	result := runSteps(4, contract.Config{WindowSize: 10, EmitEmpty: true}, wm(10))
	if result.Verdict != "pass" || len(result.Violations) != 0 {
		t.Fatalf("emit-empty run failed: %+v", result.Violations)
	}
}

func TestInterleavedKeysPass(t *testing.T) {
	result := runSteps(5, contract.Config{WindowSize: 10}, ev(0, 1, 1), ev(0, 2, 10), ev(1, 1, 2), wm(10))
	if result.Verdict != "pass" || len(result.Violations) != 0 {
		t.Fatalf("interleaved keys failed: %+v", result.Violations)
	}
}

func TestFinalizeOnceWithRepeatedWatermarks(t *testing.T) {
	result := runSteps(6, contract.Config{WindowSize: 10}, ev(0, 1, 5), wm(10), wm(10), wm(20))
	if result.Verdict != "pass" || len(result.Violations) != 0 {
		t.Fatalf("finalize-once run failed: %+v", result.Violations)
	}
}

func TestCloseFinalizationPasses(t *testing.T) {
	result := runSteps(7, contract.Config{WindowSize: 10}, ev(0, 1, 5))
	if result.Verdict != "pass" || len(result.Violations) != 0 {
		t.Fatalf("Close finalization failed: %+v", result.Violations)
	}
}

func TestNegativeValuesPass(t *testing.T) {
	result := runSteps(8, contract.Config{WindowSize: 10}, ev(0, 1, -5), ev(1, 1, -3), wm(10))
	if result.Verdict != "pass" || len(result.Violations) != 0 {
		t.Fatalf("negative values failed: %+v", result.Violations)
	}
}

func TestZeroValuePasses(t *testing.T) {
	result := runSteps(9, contract.Config{WindowSize: 10}, ev(0, 1, 0), ev(1, 1, 0), wm(10))
	if result.Verdict != "pass" || len(result.Violations) != 0 {
		t.Fatalf("zero values failed: %+v", result.Violations)
	}
}

func TestTickZeroPasses(t *testing.T) {
	result := runSteps(10, contract.Config{WindowSize: 10}, ev(0, 1, 7), wm(10))
	if result.Verdict != "pass" || len(result.Violations) != 0 {
		t.Fatalf("tick zero failed: %+v", result.Violations)
	}
}

func TestMaximumLegalTickPasses(t *testing.T) {
	const windowSize uint64 = 1000
	max := contract.MaxLegalTick(windowSize)
	result := runSteps(11, contract.Config{WindowSize: windowSize}, ev(max, 2, 1), wm(^uint64(0)))
	if result.Verdict != "pass" || len(result.Violations) != 0 {
		t.Fatalf("maximum legal tick failed: %+v", result.Violations)
	}
	if result.Metrics.EventsPerSecond != 0 {
		t.Fatalf("events_per_second = %d at the maximum legal watermark, want 0", result.Metrics.EventsPerSecond)
	}
}

func TestManyKeysInOneWindowPass(t *testing.T) {
	steps := make([]workload.Step, 0, 101)
	for key := uint64(1); key <= 100; key++ {
		steps = append(steps, ev(0, key, int64(key)))
	}
	steps = append(steps, wm(10))
	result := runSteps(12, contract.Config{WindowSize: 10}, steps...)
	if result.Verdict != "pass" || len(result.Violations) != 0 {
		t.Fatalf("many keys failed: %+v", result.Violations)
	}
	if result.Metrics.PeakStateBytes == 0 {
		t.Fatal("many-key run did not report peak state")
	}
}

func TestLongEmptyGapPasses(t *testing.T) {
	result := runSteps(13, contract.Config{WindowSize: 10}, ev(0, 1, 1), wm(1_000_000))
	if result.Verdict != "pass" || len(result.Violations) != 0 {
		t.Fatalf("long empty gap failed: %+v", result.Violations)
	}
	if result.Metrics.OutputDelay == 0 {
		t.Fatal("long gap did not report output delay")
	}
}

func TestWatermarkRegressionRejected(t *testing.T) {
	result := runSteps(14, contract.Config{WindowSize: 10}, wm(20), wm(10))
	if result.Verdict != "fail" {
		t.Fatalf("watermark regression was not failed: %+v", result)
	}
	if !hasViolation(result, "watermark regressed") {
		t.Fatalf("missing watermark-regression violation: %+v", result.Violations)
	}
}

func TestEventAfterWatermarkRejected(t *testing.T) {
	result := runSteps(15, contract.Config{WindowSize: 10}, wm(10), ev(10, 1, 1))
	if result.Verdict != "fail" {
		t.Fatalf("event at/before watermark was not failed: %+v", result)
	}
	if !hasViolation(result, "event arrived at or before the watermark") {
		t.Fatalf("missing late-event violation: %+v", result.Violations)
	}
}

func TestEventTickRegressionRejected(t *testing.T) {
	result := runSteps(16, contract.Config{WindowSize: 10}, ev(5, 1, 1), ev(4, 1, 1))
	if result.Verdict != "fail" {
		t.Fatalf("out-of-order events were not failed: %+v", result)
	}
	if !hasViolation(result, "event tick regressed") {
		t.Fatalf("missing event-tick-regression violation: %+v", result.Violations)
	}
}

func TestIllegalTickRejected(t *testing.T) {
	const windowSize uint64 = 10
	illegal := contract.MaxLegalTick(windowSize) + 1
	result := runSteps(17, contract.Config{WindowSize: windowSize}, ev(illegal, 1, 1))
	if result.Verdict != "fail" {
		t.Fatalf("illegal tick was not failed: %+v", result)
	}
	if !hasViolation(result, "event tick exceeds maximum legal tick") {
		t.Fatalf("missing illegal-tick violation: %+v", result.Violations)
	}
}

// wrongSumOperator returns the correct shape but a wrong sum, so the harness
// canonical comparison must reject it.
type wrongSumOperator struct{}

func (wrongSumOperator) OnEvent(contract.Event) []contract.Output { return nil }
func (wrongSumOperator) OnWatermark(tick uint64) []contract.Output {
	if tick >= 10 {
		return []contract.Output{{WindowStart: 0, WindowEnd: 10, Key: 1, Count: 2, Sum: 999, Final: true}}
	}
	return nil
}
func (wrongSumOperator) Close() []contract.Output { return nil }
func (wrongSumOperator) StateBytes() uint64       { return 0 }

func TestWrongSumRejected(t *testing.T) {
	result := runFactory(18, contract.Config{WindowSize: 10}, contract.Factory(func(contract.Config) contract.Operator {
		return wrongSumOperator{}
	}), ev(0, 1, 2), ev(1, 1, 3), wm(10))
	if result.Verdict != "fail" {
		t.Fatalf("wrong sum was not failed: %+v", result)
	}
	if !hasViolation(result, "final outputs do not match expected records") {
		t.Fatalf("missing canonical-mismatch violation: %+v", result.Violations)
	}
}

// duplicateOperator emits the same final window twice at the same watermark.
type duplicateOperator struct{}

func (duplicateOperator) OnEvent(contract.Event) []contract.Output { return nil }
func (duplicateOperator) OnWatermark(tick uint64) []contract.Output {
	if tick >= 10 {
		out := contract.Output{WindowStart: 0, WindowEnd: 10, Key: 1, Count: 2, Sum: 5, Final: true}
		return []contract.Output{out, out}
	}
	return nil
}
func (duplicateOperator) Close() []contract.Output { return nil }
func (duplicateOperator) StateBytes() uint64       { return 0 }

func TestDuplicateFinalOutputRejected(t *testing.T) {
	result := runFactory(19, contract.Config{WindowSize: 10}, contract.Factory(func(contract.Config) contract.Operator {
		return duplicateOperator{}
	}), ev(0, 1, 2), ev(1, 1, 3), wm(10))
	if result.Verdict != "fail" {
		t.Fatalf("duplicate final output was not failed: %+v", result)
	}
	if !hasViolation(result, "duplicate final output") {
		t.Fatalf("missing duplicate-output violation: %+v", result.Violations)
	}
}

// prematureOperator emits a final output from OnEvent before its window end.
type prematureOperator struct{}

func (prematureOperator) OnEvent(contract.Event) []contract.Output {
	return []contract.Output{{WindowStart: 0, WindowEnd: 10, Key: 1, Count: 1, Sum: 2, Final: true}}
}
func (prematureOperator) OnWatermark(uint64) []contract.Output { return nil }
func (prematureOperator) Close() []contract.Output             { return nil }
func (prematureOperator) StateBytes() uint64                   { return 0 }

func TestPrematureFinalOutputRejected(t *testing.T) {
	result := runFactory(20, contract.Config{WindowSize: 10}, contract.Factory(func(contract.Config) contract.Operator {
		return prematureOperator{}
	}), ev(0, 1, 2), wm(10))
	if result.Verdict != "fail" {
		t.Fatalf("premature final output was not failed: %+v", result)
	}
	if !hasViolation(result, "final output emitted before its window end") {
		t.Fatalf("missing premature-output violation: %+v", result.Violations)
	}
}

// nonFinalOperator emits a record whose Final flag is false.
type nonFinalOperator struct{}

func (nonFinalOperator) OnEvent(contract.Event) []contract.Output { return nil }
func (nonFinalOperator) OnWatermark(tick uint64) []contract.Output {
	if tick >= 10 {
		return []contract.Output{{WindowStart: 0, WindowEnd: 10, Key: 1, Count: 2, Sum: 5}}
	}
	return nil
}
func (nonFinalOperator) Close() []contract.Output { return nil }
func (nonFinalOperator) StateBytes() uint64       { return 0 }

func TestNonFinalOutputRejected(t *testing.T) {
	result := runFactory(21, contract.Config{WindowSize: 10}, contract.Factory(func(contract.Config) contract.Operator {
		return nonFinalOperator{}
	}), ev(0, 1, 2), ev(1, 1, 3), wm(10))
	if result.Verdict != "fail" {
		t.Fatalf("non-final output was not failed: %+v", result)
	}
	if !hasViolation(result, "non-final output emitted") {
		t.Fatalf("missing non-final-output violation: %+v", result.Violations)
	}
}

// panickingOperator panics inside OnEvent.
type panickingOperator struct{}

func (panickingOperator) OnEvent(contract.Event) []contract.Output { panic("boom") }
func (panickingOperator) OnWatermark(uint64) []contract.Output     { return nil }
func (panickingOperator) Close() []contract.Output                 { return nil }
func (panickingOperator) StateBytes() uint64                       { return 0 }

func TestPanickingOperatorRejected(t *testing.T) {
	result := runFactory(22, contract.Config{WindowSize: 10}, contract.Factory(func(contract.Config) contract.Operator {
		return panickingOperator{}
	}), ev(0, 1, 2))
	if result.Verdict != "fail" {
		t.Fatalf("panicking operator was not failed: %+v", result)
	}
	if !hasViolation(result, "contestant operator panicked") {
		t.Fatalf("missing panic violation: %+v", result.Violations)
	}
}

func TestDeterministicLogicalMetrics(t *testing.T) {
	config := DefaultPublicConfig()
	first := Evaluate(^uint64(0), config, starter.Factory)
	second := Evaluate(^uint64(0), config, starter.Factory)
	if first.Verdict != "pass" || second.Verdict != "pass" {
		t.Fatalf("deterministic smoke run failed: %+v / %+v", first.Violations, second.Violations)
	}
	if first.Metrics.EventsPerSecond != second.Metrics.EventsPerSecond ||
		first.Metrics.PeakStateBytes != second.Metrics.PeakStateBytes ||
		first.Metrics.OutputDelay != second.Metrics.OutputDelay ||
		first.Metrics.AllocBytes != second.Metrics.AllocBytes {
		t.Fatalf("logical metrics diverged for the same seed:\n%+v\n%+v", first.Metrics, second.Metrics)
	}
}

func TestResultEnvelopeContainsEveryPublicMetric(t *testing.T) {
	result := Evaluate(23, DefaultPublicConfig(), starter.Factory)
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
	for _, metric := range []string{"events_per_second", "peak_state_bytes", "output_delay", "alloc_bytes"} {
		if _, ok := metrics[metric]; !ok {
			t.Fatalf("result is missing metric %q", metric)
		}
	}
	if envelope["seed"] != "23" {
		t.Fatalf("seed not preserved as a decimal string: %v", envelope["seed"])
	}
}
