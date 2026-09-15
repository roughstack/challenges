// Package workload generates the deterministic public job stream for the
// durable-queue-easy arena.
package workload

// Action is the disposition of a job's first lease.
type Action uint8

const (
	// ActionAck leases the job once and acknowledges it.
	ActionAck Action = iota
	// ActionNack leases the job once, nacks it, then leases and acks it.
	ActionNack
	// ActionExpire leases the job once, lets its visibility expire, then
	// leases and acks it.
	ActionExpire
)

// Job is one deterministic enqueue. IDs are assigned 1..Jobs in order.
type Job struct {
	ID      uint64
	Payload []byte
}

// Config controls deterministic public job generation without exposing ranked
// workload distributions.
type Config struct {
	Seed        uint64
	Jobs        int
	MaxPayload  int
	NackEvery   int // >0: every N-th job's first lease is nacked once
	ExpireEvery int // >0: every N-th job's first lease is left to expire once
}

// Generate produces the job list and the matching first-lease action for each
// job. Payload sizes and bytes are fully determined by the seed.
func Generate(config Config) ([]Job, []Action) {
	if config.Jobs <= 0 {
		return nil, nil
	}
	if config.MaxPayload <= 0 {
		config.MaxPayload = 256
	}

	random := splitMix64{state: config.Seed}
	jobs := make([]Job, config.Jobs)
	actions := make([]Action, config.Jobs)
	for index := 0; index < config.Jobs; index++ {
		id := uint64(index + 1)
		size := 1 + int(random.next()%uint64(config.MaxPayload))
		jobs[index] = Job{ID: id, Payload: Payload(config.Seed, id, size)}
		switch {
		case config.NackEvery > 0 && (index+1)%config.NackEvery == 0:
			actions[index] = ActionNack
		case config.ExpireEvery > 0 && (index+1)%config.ExpireEvery == 0:
			actions[index] = ActionExpire
		default:
			actions[index] = ActionAck
		}
	}
	return jobs, actions
}

// Payload returns the deterministic payload bytes for (seed, id, size).
func Payload(seed, id uint64, size int) []byte {
	payload := make([]byte, size)
	state := seed ^ rotateLeft(id, 17) ^ rotateLeft(uint64(size), 41)
	for index := range payload {
		state ^= state << 13
		state ^= state >> 7
		state ^= state << 17
		payload[index] = byte(state)
	}
	return payload
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

func rotateLeft(value uint64, shift uint) uint64 {
	return value<<shift | value>>(64-shift)
}
