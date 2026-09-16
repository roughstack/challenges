// Package workload generates the deterministic public job stream for the
// deadline-scheduler-easy arena.
package workload

// Job is one deterministic arrival. IDs are assigned 1..Jobs in ascending order
// so equal arrivals always resolve deterministically by ID.
type Job struct {
	ID            uint64
	Arrival       uint64
	EstimatedWork uint64
}

// Config controls deterministic public job generation without exposing ranked
// workload distributions.
type Config struct {
	Seed uint64
	Jobs int

	// MaxWork is the runtime of a "long" job. Short jobs use a runtime in
	// [1, 8], capped to MaxWork when MaxWork is smaller than 8.
	MaxWork uint64

	// MaxGap bounds the idle gap between arrival bursts. A gap of at least one
	// tick is always inserted between bursts.
	MaxGap uint64

	// BurstEvery controls the burst size: every BurstEvery-th job opens a new
	// arrival tick and the next BurstEvery-1 jobs arrive together with it.
	BurstEvery int

	// LongEvery makes every LongEvery-th job (and the first job) use MaxWork
	// instead of a short runtime.
	LongEvery int
}

// Generate creates a deterministic public stream: a long job arrives first,
// short jobs arrive in bursts with idle gaps between bursts, and additional
// long jobs are interleaved periodically.
func Generate(config Config) []Job {
	if config.Jobs <= 0 {
		return nil
	}
	if config.MaxWork == 0 {
		config.MaxWork = 64
	}
	if config.MaxGap == 0 {
		config.MaxGap = 8
	}
	if config.BurstEvery <= 0 {
		config.BurstEvery = 4
	}
	if config.LongEvery <= 0 {
		config.LongEvery = 53
	}

	random := splitMix64{state: config.Seed}
	jobs := make([]Job, config.Jobs)
	arrival := uint64(0)

	for index := 0; index < config.Jobs; index++ {
		if index > 0 && index%config.BurstEvery == 0 {
			arrival += 1 + random.next()%config.MaxGap
		}

		work := shortWork(&random, config.MaxWork)
		if index == 0 || index%config.LongEvery == 0 {
			work = config.MaxWork
		}

		jobs[index] = Job{
			ID:            uint64(index + 1),
			Arrival:       arrival,
			EstimatedWork: work,
		}
	}

	return jobs
}

func shortWork(random *splitMix64, maxWork uint64) uint64 {
	limit := uint64(8)
	if maxWork < limit {
		limit = maxWork
	}
	return 1 + random.next()%limit
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
