// Package harness owns public execution, the deterministic monotonic virtual
// clock, traffic validation, correctness gates, accounting, and result
// construction for the fair-gate-easy arena. The contestant surface is
// contract.Limiter only; the harness owns everything else.
package harness

import (
	"runtime"
	"runtime/debug"
	"strconv"

	"github.com/roughstack/challenges/challenges/fair-gate-easy/contract"
	"github.com/roughstack/challenges/challenges/fair-gate-easy/workload"
)

const (
	// ArenaID is the stable public identifier for this variant.
	ArenaID = "fair-gate-easy"
	// ArenaVersion is the immutable public version for this variant.
	ArenaVersion = "1.0.0"
	// WorkloadID identifies the deterministic public smoke workload.
	WorkloadID = "public-smoke-v1"
	// FixtureWorkloadID identifies explicit request fixtures used by tests.
	FixtureWorkloadID = "public-fixture-v1"

	// ticksPerLogicalSecond converts the injected virtual tick into a logical
	// second for the decisions_per_second throughput metric.
	ticksPerLogicalSecond uint64 = 1_000_000

	// maxViolations bounds how many violations are recorded before a run stops
	// early and fails closed.
	maxViolations = 8
)

// Metrics are deterministic logical measurements from one workload run.
type Metrics struct {
	DecisionsPerSecond uint64 `json:"decisions_per_second"`
	ArithmeticWork     uint64 `json:"arithmetic_work"`
	AllocBytes         uint64 `json:"alloc_bytes"`
}

// RunInfo records protocol-required run metadata without wall-clock noise.
type RunInfo struct {
	WorkloadID      string `json:"workload_id"`
	DurationNS      uint64 `json:"duration_ns"`
	PeakMemoryBytes uint64 `json:"peak_memory_bytes"`
}

// Result is the versioned one-object stdout protocol.
type Result struct {
	ProtocolVersion int      `json:"protocol_version"`
	ArenaID         string   `json:"arena_id"`
	ArenaVersion    string   `json:"arena_version"`
	Seed            string   `json:"seed"`
	Verdict         string   `json:"verdict"`
	Score           int      `json:"score"`
	Metrics         Metrics  `json:"metrics"`
	Violations      []string `json:"violations"`
	Run             RunInfo  `json:"run"`
}

// PublicConfig is intentionally small and distinct from private ranked inputs.
type PublicConfig struct {
	Requests   int
	RateNum    uint64
	RateDen    uint64
	Burst      uint64
	BurstEvery int
	MaxGap     uint64
	CostUpper  uint64
}

// DefaultPublicConfig returns the fast public smoke workload.
func DefaultPublicConfig() PublicConfig {
	return PublicConfig{
		Requests:   4000,
		RateNum:    2,
		RateDen:    1,
		Burst:      10,
		BurstEvery: 4,
		MaxGap:     8,
		CostUpper:  3,
	}
}

// Evaluate runs the deterministic public smoke workload against a contestant
// limiter and returns the versioned result envelope.
func Evaluate(seed uint64, config PublicConfig, factory contract.Factory) Result {
	requests := workload.Generate(workload.Config{
		Seed:       seed,
		Requests:   config.Requests,
		Burst:      config.Burst,
		RateNum:    config.RateNum,
		RateDen:    config.RateDen,
		BurstEvery: config.BurstEvery,
		MaxGap:     config.MaxGap,
		CostUpper:  config.CostUpper,
	})
	contractRequests := make([]contract.Request, len(requests))
	for index, request := range requests {
		contractRequests[index] = contract.Request{
			Tenant:  1,
			Cost:    uint32(request.Cost),
			NowTick: request.Tick,
		}
	}

	result := RunRequests(seed, contract.Config{
		RateNum: config.RateNum,
		RateDen: config.RateDen,
		Burst:   config.Burst,
	}, contractRequests, factory)
	result.Run.WorkloadID = WorkloadID
	return result
}

// RunRequests executes an explicit deterministic request sequence against a
// contestant limiter. It is exported so public tests can assert boundary and
// fail-closed behavior on named fixtures without mirroring a ranked workload
// distribution.
func RunRequests(seed uint64, cfg contract.Config, requests []contract.Request, factory contract.Factory) Result {
	result := Result{
		ProtocolVersion: 1,
		ArenaID:         ArenaID,
		ArenaVersion:    ArenaVersion,
		Seed:            strconv.FormatUint(seed, 10),
		Verdict:         "pass",
		Score:           0,
		Violations:      []string{},
		Run: RunInfo{
			WorkloadID: FixtureWorkloadID,
		},
	}

	cfg = cfg.Normalized()
	capacityUnits, ok := cfg.CapacityUnits()
	if !ok {
		result.Violations = []string{"config capacity overflow"}
		result.Verdict = "fail"
		return result
	}

	if violations := validateRequests(requests); len(violations) > 0 {
		result.Violations = violations
		result.Verdict = "fail"
		return result
	}

	previousGC := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(previousGC)

	// The factory is contestant code, so its allocation volume is charged to
	// the deterministic memory metric alongside every Decide call.
	var factoryBefore, factoryAfter runtime.MemStats
	runtime.ReadMemStats(&factoryBefore)
	limiter := newLimiter(factory, cfg)
	runtime.ReadMemStats(&factoryAfter)
	allocBytes := factoryAfter.TotalAlloc - factoryBefore.TotalAlloc
	if limiter == nil {
		result.Violations = []string{"limiter factory returned nil"}
		result.Verdict = "fail"
		return result
	}

	runner := &runner{
		seed:          seed,
		cfg:           cfg,
		den:           cfg.RateDenEffective(),
		capacityUnits: capacityUnits,
		limiter:       limiter,
		oracleUnits:   capacityUnits,
		allocBytes:    allocBytes,
	}

	for _, request := range requests {
		if !runner.call(request) {
			break
		}
		if len(runner.violations) >= maxViolations {
			break
		}
	}

	result.Metrics = Metrics{
		DecisionsPerSecond: decisionsPerSecond(runner.decisions, runner.logicalSpan),
		ArithmeticWork:     runner.decisions,
		AllocBytes:         runner.allocBytes,
	}
	result.Run.PeakMemoryBytes = runner.allocBytes

	if len(runner.violations) > 0 {
		result.Violations = runner.violations
		result.Verdict = "fail"
	}
	return result
}

func validateRequests(requests []contract.Request) []string {
	var violations []string
	for index := 1; index < len(requests); index++ {
		if requests[index].NowTick < requests[index-1].NowTick {
			violations = append(violations, "request timestamp regressed")
			if len(violations) >= maxViolations {
				break
			}
		}
	}
	return violations
}

// newLimiter invokes the contestant factory and converts a panic into a nil
// limiter.
func newLimiter(factory contract.Factory, cfg contract.Config) (limiter contract.Limiter) {
	if factory == nil {
		return nil
	}
	defer func() {
		if recover() != nil {
			limiter = nil
		}
	}()
	return factory(cfg)
}

type runner struct {
	seed          uint64
	cfg           contract.Config
	den           uint64
	capacityUnits uint64
	limiter       contract.Limiter

	oracleUnits    uint64
	oracleLastTick uint64

	decisions   uint64
	logicalSpan uint64
	allocBytes  uint64
	violations  []string
}

// call delivers one request to the contestant limiter inside a panic boundary,
// charges the deterministic allocation volume for the call, and compares the
// returned decision against the trusted oracle.
func (r *runner) call(request contract.Request) bool {
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)

	var got contract.Decision
	if !r.callDecide(request, &got) {
		runtime.ReadMemStats(&after)
		r.allocBytes += after.TotalAlloc - before.TotalAlloc
		return false
	}
	runtime.ReadMemStats(&after)
	r.allocBytes += after.TotalAlloc - before.TotalAlloc

	expected := r.oracleDecide(request)
	r.decisions++
	r.logicalSpan = satAdd(request.NowTick, 1)
	r.checkDecision(got, expected)
	return true
}

func (r *runner) callDecide(request contract.Request, got *contract.Decision) (ok bool) {
	defer func() {
		if recover() != nil {
			r.violations = append(r.violations, "limiter panicked")
			*got = contract.Decision{}
			ok = false
		}
	}()
	*got = r.limiter.Decide(request)
	return true
}

// oracleDecide is the trusted deterministic token-bucket model. It owns the
// authoritative answer for every request and advances independently of the
// contestant implementation.
func (r *runner) oracleDecide(request contract.Request) contract.Decision {
	r.oracleUnits = refill(r.oracleUnits, r.oracleLastTick, request.NowTick, r.cfg.RateNum, r.capacityUnits)
	r.oracleLastTick = request.NowTick

	costUnits, ok := mulUint64(uint64(request.Cost), r.den)
	if !ok || costUnits > r.capacityUnits {
		return contract.Decision{Allowed: false, RetryAt: contract.RetryNever}
	}

	if r.oracleUnits >= costUnits {
		r.oracleUnits -= costUnits
		return contract.Decision{Allowed: true}
	}

	return contract.Decision{Allowed: false, RetryAt: retryAt(r.oracleUnits, costUnits, request.NowTick, r.cfg.RateNum)}
}

func (r *runner) checkDecision(got, expected contract.Decision) {
	if got.Allowed != expected.Allowed {
		if got.Allowed {
			r.violations = append(r.violations, "limiter admitted a request the oracle rejected")
		} else {
			r.violations = append(r.violations, "limiter rejected a request the oracle admitted")
		}
		return
	}

	if got.Allowed {
		if got.RetryAt != 0 {
			r.violations = append(r.violations, "allowed decision returned a non-zero retry tick")
		}
		return
	}

	if got.RetryAt != expected.RetryAt {
		r.violations = append(r.violations, "retry tick does not match expected retry tick")
	}
}

// refill adds tokens earned between lastTick and now and clamps the result to
// capacity. A tick interval whose product would overflow is saturated to a
// full bucket instead of wrapping.
func refill(units, lastTick, now, rateNum, capacity uint64) uint64 {
	if now <= lastTick || rateNum == 0 {
		return units
	}
	elapsed := now - lastTick
	if elapsed > ^uint64(0)/rateNum {
		return capacity
	}
	added := elapsed * rateNum
	if added > capacity-units {
		return capacity
	}
	return units + added
}

// retryAt returns the earliest tick strictly greater than now at which the
// bucket holds at least costUnits, or RetryNever when the tick cannot be
// represented in uint64.
func retryAt(units, costUnits, now, rateNum uint64) uint64 {
	if rateNum == 0 {
		return contract.RetryNever
	}
	deficit := costUnits - units
	delta := deficit / rateNum
	if deficit%rateNum != 0 {
		delta++
	}
	if delta == 0 {
		delta = 1
	}
	if now > ^uint64(0)-delta {
		return contract.RetryNever
	}
	return now + delta
}

func decisionsPerSecond(decisions, logicalSpan uint64) uint64 {
	if decisions == 0 || logicalSpan == 0 {
		return 0
	}
	scaled, ok := mulUint64(decisions, ticksPerLogicalSecond)
	if !ok {
		scaled = ^uint64(0)
	}
	return scaled / logicalSpan
}

func mulUint64(a, b uint64) (uint64, bool) {
	if a != 0 && b > ^uint64(0)/a {
		return 0, false
	}
	return a * b, true
}

func satAdd(a, b uint64) uint64 {
	if a > ^uint64(0)-b {
		return ^uint64(0)
	}
	return a + b
}
