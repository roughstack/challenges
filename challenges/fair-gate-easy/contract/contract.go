// Package contract defines the narrow contestant-owned surface for the
// fair-gate-easy arena. Everything else — the monotonic virtual clock, traffic
// generation, correctness gates, accounting, and result construction — is
// owned by the trusted harness.
package contract

// DefaultRateDen is the refill period in ticks used when Config leaves
// RateDen unset or zero.
const DefaultRateDen uint64 = 1

// RetryNever is the documented RetryAt value returned for a rejected request
// that can never be satisfied under the configured rate. A rejected request
// whose earliest legal retry tick exists always receives a RetryAt strictly
// greater than its NowTick, so zero is unambiguous as a "never" sentinel.
const RetryNever uint64 = 0

// Config is the immutable per-run rate-limit configuration handed to the
// limiter.
//
// The refill rate is the rational RateNum/RateDen tokens per tick. RateDen
// zero falls back to DefaultRateDen. RateNum zero means the bucket never
// refills after its initial fill. Burst is the maximum token capacity of the
// bucket; a zero burst means the bucket starts empty and no positive-cost
// request can ever be admitted.
type Config struct {
	RateNum uint64
	RateDen uint64
	Burst   uint64
}

// Normalized returns a copy of c with the zero RateDen default applied.
func (c Config) Normalized() Config {
	if c.RateDen == 0 {
		c.RateDen = DefaultRateDen
	}
	return c
}

// RateDenEffective returns the configured refill period with the zero value
// resolved to DefaultRateDen.
func (c Config) RateDenEffective() uint64 {
	if c.RateDen == 0 {
		return DefaultRateDen
	}
	return c.RateDen
}

// CapacityUnits returns the bucket capacity expressed in fixed-point token
// units (one whole token equals RateDenEffective units). It reports false when
// the product would overflow uint64.
func (c Config) CapacityUnits() (uint64, bool) {
	den := c.RateDenEffective()
	if c.Burst != 0 && den > ^uint64(0)/c.Burst {
		return 0, false
	}
	return c.Burst * den, true
}

// Request is one rate-limit decision delivered to the limiter.
//
// Tenant is reserved for higher tiers and is always ignored by the easy,
// single-tenant bucket. Cost is the number of whole tokens the request
// consumes. NowTick is the harness-owned monotonic virtual clock tick at which
// the request arrives.
type Request struct {
	Tenant  uint64
	Cost    uint32
	NowTick uint64
}

// Decision is the limiter's deterministic answer for one request.
//
// When Allowed is true, RetryAt must be zero. When Allowed is false, RetryAt
// is the earliest tick strictly greater than NowTick at which an identical
// retry would be admitted, assuming no intervening requests, or RetryNever
// when no finite tick satisfies the request.
type Decision struct {
	Allowed bool
	RetryAt uint64
}

// Limiter is the complete contestant-owned surface for this arena. The
// harness owns the clock, traffic, correctness gates, accounting, and result
// construction; the contestant only answers Decide calls.
type Limiter interface {
	Decide(req Request) Decision
}

// Factory constructs a fresh limiter for one isolated workload run.
type Factory func(cfg Config) Limiter
