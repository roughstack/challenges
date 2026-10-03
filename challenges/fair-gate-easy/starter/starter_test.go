package starter

import (
	"testing"

	"github.com/bytearena/arenas/arenas/fair-gate-easy/contract"
)

func decide(limiter contract.Limiter, tick uint64, cost uint32) contract.Decision {
	return limiter.Decide(contract.Request{Tenant: 1, Cost: cost, NowTick: tick})
}

func TestInitialBurst(t *testing.T) {
	limiter := NewLimiter(contract.Config{RateNum: 1, RateDen: 1, Burst: 5})
	for index := 0; index < 5; index++ {
		if got := decide(limiter, 0, 1); !got.Allowed || got.RetryAt != 0 {
			t.Fatalf("request %d of initial burst = %+v, want allowed", index, got)
		}
	}
	got := decide(limiter, 0, 1)
	if got.Allowed || got.RetryAt != 1 {
		t.Fatalf("sixth same-tick request = %+v, want rejected with RetryAt 1", got)
	}
}

func TestRefillRestoresExactlyThreeTokens(t *testing.T) {
	limiter := NewLimiter(contract.Config{RateNum: 1, RateDen: 1, Burst: 5})
	if got := decide(limiter, 0, 5); !got.Allowed {
		t.Fatalf("draining request = %+v, want allowed", got)
	}

	// Three idle ticks restore exactly three tokens, so a cost-4 request is
	// still one token short.
	got := decide(limiter, 3, 4)
	if got.Allowed || got.RetryAt != 4 {
		t.Fatalf("cost 4 after 3 idle ticks = %+v, want rejected with RetryAt 4", got)
	}

	if got := decide(limiter, 3, 3); !got.Allowed {
		t.Fatalf("cost 3 after 3 idle ticks = %+v, want allowed", got)
	}
	if got := decide(limiter, 3, 1); got.Allowed {
		t.Fatalf("cost 1 after restored tokens were spent = %+v, want rejected", got)
	}
}

func TestWeightedCost(t *testing.T) {
	limiter := NewLimiter(contract.Config{RateNum: 0, RateDen: 1, Burst: 5})
	if got := decide(limiter, 0, 3); !got.Allowed {
		t.Fatalf("cost 3 from full bucket = %+v, want allowed", got)
	}
	got := decide(limiter, 0, 3)
	if got.Allowed || got.RetryAt != contract.RetryNever {
		t.Fatalf("second cost 3 with zero rate = %+v, want rejected with RetryNever", got)
	}
}

func TestRetryBoundary(t *testing.T) {
	limiter := NewLimiter(contract.Config{RateNum: 2, RateDen: 1, Burst: 10})
	if got := decide(limiter, 10, 10); !got.Allowed {
		t.Fatalf("draining request = %+v, want allowed", got)
	}
	got := decide(limiter, 10, 3)
	if got.Allowed || got.RetryAt != 12 {
		t.Fatalf("cost 3 from empty bucket at tick 10 = %+v, want rejected with RetryAt 12", got)
	}
}

func TestLargeTickJumpClampsToFull(t *testing.T) {
	limiter := NewLimiter(contract.Config{RateNum: 2, RateDen: 1, Burst: 5})
	if got := decide(limiter, 0, 5); !got.Allowed {
		t.Fatalf("draining request = %+v, want allowed", got)
	}

	const nearMax = ^uint64(0) - 1
	if got := decide(limiter, nearMax, 5); !got.Allowed {
		t.Fatalf("near-maximum tick jump = %+v, want allowed full bucket", got)
	}
	got := decide(limiter, nearMax, 1)
	if got.Allowed || got.RetryAt != ^uint64(0) {
		t.Fatalf("same-tick request after full spend = %+v, want rejected with RetryAt MaxUint64", got)
	}
}

func TestZeroRateRejectsPositiveCost(t *testing.T) {
	limiter := NewLimiter(contract.Config{RateNum: 0, RateDen: 1, Burst: 5})
	if got := decide(limiter, 0, 5); !got.Allowed {
		t.Fatalf("initial fill was not available: %+v", got)
	}
	got := decide(limiter, 0, 1)
	if got.Allowed || got.RetryAt != contract.RetryNever {
		t.Fatalf("zero-rate request = %+v, want rejected with RetryNever", got)
	}
}

func TestZeroBurstRejectsPositiveCost(t *testing.T) {
	limiter := NewLimiter(contract.Config{RateNum: 1, RateDen: 1, Burst: 0})
	if got := decide(limiter, 0, 1); got.Allowed || got.RetryAt != contract.RetryNever {
		t.Fatalf("zero-burst positive-cost request = %+v, want rejected with RetryNever", got)
	}
	if got := decide(limiter, 0, 0); !got.Allowed {
		t.Fatalf("zero-cost request = %+v, want allowed", got)
	}
}

func TestCostAboveBurstRejected(t *testing.T) {
	limiter := NewLimiter(contract.Config{RateNum: 1, RateDen: 1, Burst: 5})
	got := decide(limiter, 0, 6)
	if got.Allowed || got.RetryAt != contract.RetryNever {
		t.Fatalf("cost above burst = %+v, want rejected with RetryNever", got)
	}
}

func TestSameTickRequestsDoNotRefill(t *testing.T) {
	limiter := NewLimiter(contract.Config{RateNum: 10, RateDen: 1, Burst: 10})
	if got := decide(limiter, 0, 10); !got.Allowed {
		t.Fatalf("draining request = %+v, want allowed", got)
	}
	got := decide(limiter, 0, 1)
	if got.Allowed || got.RetryAt != 1 {
		t.Fatalf("same-tick request = %+v, want rejected with RetryAt 1", got)
	}
}

func TestFractionalRateUsesRationalAccounting(t *testing.T) {
	// One token per two ticks: the bucket earns half a token per tick.
	limiter := NewLimiter(contract.Config{RateNum: 1, RateDen: 2, Burst: 5})
	if got := decide(limiter, 0, 5); !got.Allowed {
		t.Fatalf("draining request = %+v, want allowed", got)
	}
	if got := decide(limiter, 1, 1); got.Allowed || got.RetryAt != 2 {
		t.Fatalf("half-token refill at tick 1 = %+v, want rejected with RetryAt 2", got)
	}
	if got := decide(limiter, 2, 1); !got.Allowed {
		t.Fatalf("full token at tick 2 = %+v, want allowed", got)
	}
	if got := decide(limiter, 3, 1); got.Allowed || got.RetryAt != 4 {
		t.Fatalf("half-token refill at tick 3 = %+v, want rejected with RetryAt 4", got)
	}
}

func TestNonMonotonicTickFailsClosed(t *testing.T) {
	limiter := NewLimiter(contract.Config{RateNum: 1, RateDen: 1, Burst: 5})
	if got := decide(limiter, 5, 1); !got.Allowed {
		t.Fatalf("first request = %+v, want allowed", got)
	}
	got := decide(limiter, 4, 1)
	if got.Allowed || got.RetryAt != contract.RetryNever {
		t.Fatalf("non-monotonic tick = %+v, want rejected with RetryNever", got)
	}
}

func TestOverflowingConfigFailsClosed(t *testing.T) {
	limiter := NewLimiter(contract.Config{RateNum: 1, RateDen: ^uint64(0), Burst: 2})
	got := decide(limiter, 0, 0)
	if got.Allowed || got.RetryAt != contract.RetryNever {
		t.Fatalf("overflowing config decision = %+v, want rejected with RetryNever", got)
	}
}
