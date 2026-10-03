// Package starter contains a deliberately simple, correct single-tenant token
// bucket for the fair-gate-easy arena. It keeps one fixed-point token counter,
// refills lazily from a monotonic accounting tick, clamps to capacity, and
// returns the earliest legal retry tick for rejected requests.
//
// It is intentionally non-optimal: every decision recomputes the rational
// rate denominator and performs checked multiplications even when no refill is
// needed. That keeps the implementation small and obviously correct without
// adding precomputed tables or branch-free fast paths.
package starter

import "github.com/bytearena/arenas/arenas/fair-gate-easy/contract"

// NewLimiter returns a correct, deliberately simple token bucket. The bucket
// starts full when the configured capacity is positive and starts empty when
// Burst is zero.
func NewLimiter(cfg contract.Config) contract.Limiter {
	cfg = cfg.Normalized()
	capacityUnits, ok := cfg.CapacityUnits()
	l := &limiter{
		cfg:           cfg,
		den:           cfg.RateDenEffective(),
		capacityUnits: capacityUnits,
		valid:         ok,
	}
	if ok {
		l.units = capacityUnits
	}
	return l
}

type limiter struct {
	cfg           contract.Config
	den           uint64
	capacityUnits uint64
	units         uint64
	lastTick      uint64
	valid         bool
}

func (l *limiter) Decide(req contract.Request) contract.Decision {
	if !l.valid {
		// The harness rejects an overflowing config before constructing the
		// limiter; being defensive here keeps a direct call fail closed.
		return contract.Decision{Allowed: false, RetryAt: contract.RetryNever}
	}

	if req.NowTick < l.lastTick {
		// The harness rejects a regressing clock before calling Decide; a
		// direct call is answered conservatively without advancing state.
		return contract.Decision{Allowed: false, RetryAt: contract.RetryNever}
	}

	l.units = refill(l.units, l.lastTick, req.NowTick, l.cfg.RateNum, l.capacityUnits)
	l.lastTick = req.NowTick

	costUnits, ok := mulUint64(uint64(req.Cost), l.den)
	if !ok || costUnits > l.capacityUnits {
		return contract.Decision{Allowed: false, RetryAt: contract.RetryNever}
	}

	if l.units >= costUnits {
		l.units -= costUnits
		return contract.Decision{Allowed: true}
	}

	return contract.Decision{Allowed: false, RetryAt: retryAt(l.units, costUnits, req.NowTick, l.cfg.RateNum)}
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

func mulUint64(a, b uint64) (uint64, bool) {
	if a != 0 && b > ^uint64(0)/a {
		return 0, false
	}
	return a * b, true
}
