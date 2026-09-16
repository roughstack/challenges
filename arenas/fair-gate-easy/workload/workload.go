// Package workload generates the deterministic public request stream for the
// fair-gate-easy arena. The harness owns this stream and injects it into the
// contestant limiter one decision at a time; the workload package never sees
// contestant state.
package workload

// Request is one deterministic rate-limit request. Cost is in whole tokens and
// is always in [0, Burst] after normalization.
type Request struct {
	Tick uint64
	Cost uint64
}

// Config controls deterministic public generation without exposing ranked
// workload distributions.
type Config struct {
	Seed     uint64
	Requests int

	// Burst is the configured token capacity. It bounds every generated cost so
	// the public smoke workload never depends on cost-above-burst handling.
	Burst uint64

	// RateNum and RateDen are carried so future tiers can shape the stream to
	// the configured rate; the easy generator does not use them for tick
	// placement.
	RateNum uint64
	RateDen uint64

	// BurstEvery is the number of requests that share one virtual tick before
	// the clock advances. Same-tick bursts exercise burst semantics.
	BurstEvery int

	// MaxGap bounds the idle tick gap between bursts. A gap of at least one
	// tick is always inserted between bursts.
	MaxGap uint64

	// CostUpper bounds generated costs. When unset it defaults to three, and it
	// is always capped to Burst.
	CostUpper uint64
}

// Generate returns a deterministic, monotonically increasing request stream.
// Costs stay within the configured burst so a correct limiter never sees an
// unsatisfiable request in the public smoke workload.
func Generate(config Config) []Request {
	if config.Requests <= 0 {
		return nil
	}
	if config.BurstEvery <= 0 {
		config.BurstEvery = 4
	}
	if config.MaxGap == 0 {
		config.MaxGap = 8
	}
	if config.RateDen == 0 {
		config.RateDen = 1
	}

	costUpper := config.CostUpper
	if costUpper == 0 {
		costUpper = 3
	}
	if costUpper > config.Burst {
		costUpper = config.Burst
	}
	// The public contract carries Cost as uint32. Keep generated costs
	// representable so the harness never truncates them.
	const maxUint32Cost = uint64(^uint32(0))
	if costUpper > maxUint32Cost {
		costUpper = maxUint32Cost
	}

	random := splitMix64{state: config.Seed}
	requests := make([]Request, 0, config.Requests)
	tick := uint64(0)

	for index := 0; index < config.Requests; index++ {
		if index > 0 && index%config.BurstEvery == 0 {
			tick = satAdd(tick, 1+random.next()%config.MaxGap)
		}

		cost := uint64(0)
		if costUpper > 0 {
			cost = 1 + random.next()%costUpper
		}

		requests = append(requests, Request{Tick: tick, Cost: cost})
	}

	return requests
}

func satAdd(a, b uint64) uint64 {
	if a > ^uint64(0)-b {
		return ^uint64(0)
	}
	return a + b
}

type splitMix64 struct {
	state uint64
}

func (r *splitMix64) next() uint64 {
	r.state += 0x9e3779b97f4a7c15
	value := r.state
	value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9
	value = (value ^ (value >> 27)) * 0x94d049bb133111eb
	return value ^ (value >> 31)
}
