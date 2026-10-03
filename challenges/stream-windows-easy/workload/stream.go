// Package workload generates the deterministic public event stream and
// watermark schedule for the stream-windows-easy arena. The harness owns this
// schedule; the operator receives it one step at a time and never buffers the
// full stream.
package workload

// StepKind distinguishes an event step from a watermark step.
type StepKind uint8

const (
	// StepEvent delivers one in-order event.
	StepEvent StepKind = iota
	// StepWatermark advances event time to Tick.
	StepWatermark
)

// Step is one scheduled occurrence in the deterministic public workload.
type Step struct {
	Kind  StepKind
	Tick  uint64
	Key   uint64
	Value int64
}

// Config controls deterministic public generation without exposing ranked
// workload distributions.
type Config struct {
	Seed       uint64
	WindowSize uint64
	Windows    int
	Keys       int
	// EventsPerWindowUpper is the largest event count generated inside one
	// non-empty window. It is clamped to WindowSize.
	EventsPerWindowUpper int
	// EmptyEvery, when positive, makes every N-th window empty.
	EmptyEvery int
}

// Normalized applies the public defaults for unset fields and returns the
// effective window size and event bound.
func (c Config) Normalized() (Config, uint64, int) {
	if c.WindowSize == 0 {
		c.WindowSize = 1000
	}
	if c.Windows <= 0 {
		c.Windows = 1
	}
	if c.Keys <= 0 {
		c.Keys = 16
	}
	if c.EventsPerWindowUpper <= 0 {
		c.EventsPerWindowUpper = 8
	}
	upper := c.EventsPerWindowUpper
	// Events are generated strictly between window boundaries so a watermark at
	// the previous boundary is never followed by an event at or before it.
	if c.WindowSize <= 1 {
		upper = 0
	} else if uint64(upper) >= c.WindowSize {
		upper = int(c.WindowSize - 1)
	}
	return c, c.WindowSize, upper
}

// Generate returns a deterministic schedule of events and watermarks. Events
// are in non-decreasing event-time order, watermarks are non-decreasing, and a
// watermark is emitted at every window boundary except the final boundary,
// which the harness resolves with Close.
func Generate(cfg Config) []Step {
	cfg, windowSize, upper := cfg.Normalized()
	random := splitMix64{state: cfg.Seed}

	steps := make([]Step, 0)
	for window := 0; window < cfg.Windows; window++ {
		start := uint64(window) * windowSize
		end := start + windowSize

		if cfg.EmptyEvery > 0 && (window+1)%cfg.EmptyEvery == 0 {
			steps = append(steps, Step{Kind: StepWatermark, Tick: end})
			continue
		}

		count := 0
		if upper > 0 {
			count = 1 + int(random.next()%uint64(upper))
		}
		for offset := 0; offset < count; offset++ {
			key := 1 + random.next()%uint64(cfg.Keys)
			value := eventValue(random.next())
			steps = append(steps, Step{
				Kind:  StepEvent,
				Tick:  start + 1 + uint64(offset),
				Key:   key,
				Value: value,
			})
		}

		if window != cfg.Windows-1 {
			steps = append(steps, Step{Kind: StepWatermark, Tick: end})
		}
	}
	return steps
}

// eventValue maps one PRNG output to a small int64 that exercises negative,
// zero, and positive sums deterministically.
func eventValue(state uint64) int64 {
	switch state % 4 {
	case 0:
		return 0
	case 1:
		return -int64(1 + state%1000)
	default:
		return int64(1 + state%1000)
	}
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
