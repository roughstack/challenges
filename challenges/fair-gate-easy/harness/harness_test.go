package harness

import (
	"encoding/json"
	"testing"

	"github.com/roughstack/challenges/challenges/fair-gate-easy/contract"
	"github.com/roughstack/challenges/challenges/fair-gate-easy/starter"
)

func req(tick uint64, cost uint32) contract.Request {
	return contract.Request{Tenant: 1, Cost: cost, NowTick: tick}
}

func runStarter(seed uint64, cfg contract.Config, requests ...contract.Request) Result {
	return RunRequests(seed, cfg, requests, starter.NewLimiter)
}

func runFactory(seed uint64, cfg contract.Config, factory contract.Factory, requests ...contract.Request) Result {
	return RunRequests(seed, cfg, requests, factory)
}

func hasViolation(result Result, want string) bool {
	for _, violation := range result.Violations {
		if violation == want {
			return true
		}
	}
	return false
}

func TestInitialBurstPasses(t *testing.T) {
	requests := []contract.Request{
		req(0, 1), req(0, 1), req(0, 1), req(0, 1), req(0, 1), req(0, 1),
	}
	result := runStarter(1, contract.Config{RateNum: 1, RateDen: 1, Burst: 5}, requests...)
	if result.Verdict != "pass" || len(result.Violations) != 0 {
		t.Fatalf("initial burst failed: %+v", result.Violations)
	}
	if result.Metrics.DecisionsPerSecond == 0 {
		t.Fatal("decisions_per_second was zero")
	}
}

func TestRefillRestoresExactlyThreeTokensPasses(t *testing.T) {
	result := runStarter(2, contract.Config{RateNum: 1, RateDen: 1, Burst: 5},
		req(0, 5), req(3, 4), req(3, 3), req(3, 1))
	if result.Verdict != "pass" || len(result.Violations) != 0 {
		t.Fatalf("refill run failed: %+v", result.Violations)
	}
}

func TestWeightedCostPasses(t *testing.T) {
	result := runStarter(3, contract.Config{RateNum: 0, RateDen: 1, Burst: 5}, req(0, 3), req(0, 3))
	if result.Verdict != "pass" || len(result.Violations) != 0 {
		t.Fatalf("weighted cost failed: %+v", result.Violations)
	}
}

func TestRetryBoundaryPasses(t *testing.T) {
	result := runStarter(4, contract.Config{RateNum: 2, RateDen: 1, Burst: 10}, req(10, 10), req(10, 3))
	if result.Verdict != "pass" || len(result.Violations) != 0 {
		t.Fatalf("retry boundary failed: %+v", result.Violations)
	}
}

func TestLargeTickJumpPasses(t *testing.T) {
	const nearMax = ^uint64(0) - 1
	result := runStarter(5, contract.Config{RateNum: 2, RateDen: 1, Burst: 5},
		req(0, 5), req(nearMax, 5), req(nearMax, 1))
	if result.Verdict != "pass" || len(result.Violations) != 0 {
		t.Fatalf("large tick jump failed: %+v", result.Violations)
	}
}

func TestZeroRateRejectsPositiveCostPasses(t *testing.T) {
	result := runStarter(6, contract.Config{RateNum: 0, RateDen: 1, Burst: 5}, req(0, 5), req(0, 1))
	if result.Verdict != "pass" || len(result.Violations) != 0 {
		t.Fatalf("zero rate run failed: %+v", result.Violations)
	}
}

func TestZeroBurstRejectsPositiveCostPasses(t *testing.T) {
	result := runStarter(7, contract.Config{RateNum: 1, RateDen: 1, Burst: 0}, req(0, 1), req(0, 0))
	if result.Verdict != "pass" || len(result.Violations) != 0 {
		t.Fatalf("zero burst run failed: %+v", result.Violations)
	}
}

func TestCostAboveBurstRejectedDeterministically(t *testing.T) {
	result := runStarter(8, contract.Config{RateNum: 1, RateDen: 1, Burst: 5}, req(0, 6))
	if result.Verdict != "pass" || len(result.Violations) != 0 {
		t.Fatalf("cost above burst failed: %+v", result.Violations)
	}
}

func TestSameTickRequestsDoNotRefillPasses(t *testing.T) {
	result := runStarter(9, contract.Config{RateNum: 10, RateDen: 1, Burst: 10}, req(0, 10), req(0, 1))
	if result.Verdict != "pass" || len(result.Violations) != 0 {
		t.Fatalf("same-tick run failed: %+v", result.Violations)
	}
}

func TestNonMonotonicTimestampRejected(t *testing.T) {
	result := runStarter(10, contract.Config{RateNum: 1, RateDen: 1, Burst: 5}, req(5, 1), req(4, 1))
	if result.Verdict != "fail" {
		t.Fatalf("non-monotonic timestamp was not failed: %+v", result)
	}
	if !hasViolation(result, "request timestamp regressed") {
		t.Fatalf("missing timestamp-regression violation: %+v", result.Violations)
	}
}

func TestConfigOverflowRejected(t *testing.T) {
	result := runStarter(17, contract.Config{RateNum: 1, RateDen: ^uint64(0), Burst: 2}, req(0, 1))
	if result.Verdict != "fail" {
		t.Fatalf("overflowing config was not failed: %+v", result)
	}
	if !hasViolation(result, "config capacity overflow") {
		t.Fatalf("missing config-overflow violation: %+v", result.Violations)
	}
}

func TestEmptyRunPasses(t *testing.T) {
	result := runStarter(18, contract.Config{RateNum: 1, RateDen: 1, Burst: 5})
	if result.Verdict != "pass" || len(result.Violations) != 0 {
		t.Fatalf("empty run failed: %+v", result.Violations)
	}
	if result.Metrics.DecisionsPerSecond != 0 || result.Metrics.ArithmeticWork != 0 {
		t.Fatalf("empty run reported non-zero work: %+v", result.Metrics)
	}
}

func TestNilFactoryRejected(t *testing.T) {
	result := RunRequests(11, contract.Config{RateNum: 1, RateDen: 1, Burst: 5}, []contract.Request{req(0, 1)}, nil)
	if result.Verdict != "fail" || !hasViolation(result, "limiter factory returned nil") {
		t.Fatalf("nil factory was not failed closed: %+v", result)
	}
}

func TestNilLimiterRejected(t *testing.T) {
	factory := contract.Factory(func(contract.Config) contract.Limiter { return nil })
	result := runFactory(12, contract.Config{RateNum: 1, RateDen: 1, Burst: 5}, factory, req(0, 1))
	if result.Verdict != "fail" || !hasViolation(result, "limiter factory returned nil") {
		t.Fatalf("nil limiter was not failed closed: %+v", result)
	}
}

type panickingLimiter struct{}

func (panickingLimiter) Decide(contract.Request) contract.Decision { panic("boom") }

func TestPanickingLimiterRejected(t *testing.T) {
	factory := contract.Factory(func(contract.Config) contract.Limiter { return panickingLimiter{} })
	result := runFactory(13, contract.Config{RateNum: 1, RateDen: 1, Burst: 5}, factory, req(0, 1))
	if result.Verdict != "fail" || !hasViolation(result, "limiter panicked") {
		t.Fatalf("panicking limiter was not failed closed: %+v", result)
	}
}

// overchargeLimiter subtracts one extra token on every admission, so it blocks
// a later request that the oracle admits.
type overchargeLimiter struct {
	remaining uint64
}

func (l *overchargeLimiter) Decide(request contract.Request) contract.Decision {
	if l.remaining >= uint64(request.Cost) {
		l.remaining -= uint64(request.Cost) + 1
		return contract.Decision{Allowed: true}
	}
	return contract.Decision{Allowed: false, RetryAt: contract.RetryNever}
}

func TestOverchargingLimiterRejected(t *testing.T) {
	factory := contract.Factory(func(contract.Config) contract.Limiter {
		return &overchargeLimiter{remaining: 5}
	})
	result := runFactory(14, contract.Config{RateNum: 1, RateDen: 1, Burst: 5}, factory, req(0, 3), req(0, 2))
	if result.Verdict != "fail" || !hasViolation(result, "limiter rejected a request the oracle admitted") {
		t.Fatalf("overcharging limiter was not failed closed: %+v", result)
	}
}

// leakyLimiter admits every request regardless of the bucket, so it leaks
// tokens between calls.
type leakyLimiter struct{}

func (leakyLimiter) Decide(contract.Request) contract.Decision {
	return contract.Decision{Allowed: true}
}

func TestLeakyLimiterRejected(t *testing.T) {
	factory := contract.Factory(func(contract.Config) contract.Limiter { return leakyLimiter{} })
	result := runFactory(15, contract.Config{RateNum: 1, RateDen: 1, Burst: 5}, factory, req(0, 5), req(0, 1))
	if result.Verdict != "fail" || !hasViolation(result, "limiter admitted a request the oracle rejected") {
		t.Fatalf("leaky limiter was not failed closed: %+v", result)
	}
}

// wrongRetryLimiter admits the first request and then reports a retry tick that
// is later than the earliest legal tick.
type wrongRetryLimiter struct {
	used bool
}

func (l *wrongRetryLimiter) Decide(request contract.Request) contract.Decision {
	if !l.used {
		l.used = true
		return contract.Decision{Allowed: true}
	}
	return contract.Decision{Allowed: false, RetryAt: request.NowTick + 99}
}

func TestWrongRetryTickRejected(t *testing.T) {
	factory := contract.Factory(func(contract.Config) contract.Limiter { return &wrongRetryLimiter{} })
	result := runFactory(16, contract.Config{RateNum: 1, RateDen: 1, Burst: 5}, factory, req(0, 5), req(0, 1))
	if result.Verdict != "fail" || !hasViolation(result, "retry tick does not match expected retry tick") {
		t.Fatalf("wrong retry tick was not failed closed: %+v", result)
	}
}

func TestDeterministicLogicalMetrics(t *testing.T) {
	config := DefaultPublicConfig()
	first := Evaluate(^uint64(0), config, starter.NewLimiter)
	second := Evaluate(^uint64(0), config, starter.NewLimiter)
	if first.Verdict != "pass" || second.Verdict != "pass" {
		t.Fatalf("deterministic smoke run failed: %+v / %+v", first.Violations, second.Violations)
	}
	if first.Metrics != second.Metrics {
		t.Fatalf("logical metrics diverged for the same seed:\n%+v\n%+v", first.Metrics, second.Metrics)
	}
	if first.Metrics.ArithmeticWork == 0 || first.Metrics.AllocBytes == 0 {
		t.Fatalf("expected non-zero work and allocation counters: %+v", first.Metrics)
	}
}

func TestResultEnvelopeContainsEveryPublicMetric(t *testing.T) {
	result := Evaluate(23, DefaultPublicConfig(), starter.NewLimiter)
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
	for _, metric := range []string{"decisions_per_second", "arithmetic_work", "alloc_bytes"} {
		if _, ok := metrics[metric]; !ok {
			t.Fatalf("result is missing metric %q", metric)
		}
	}
	if envelope["seed"] != "23" {
		t.Fatalf("seed not preserved as a decimal string: %v", envelope["seed"])
	}
}
