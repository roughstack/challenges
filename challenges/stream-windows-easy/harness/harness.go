// Package harness owns public execution, the deterministic event loop, the
// injected virtual clock, output canonicalization, accounting, and result
// construction for the stream-windows-easy arena. The contestant surface is the
// contract.Operator only.
package harness

import (
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"

	"github.com/roughstack/challenges/challenges/stream-windows-easy/contract"
	"github.com/roughstack/challenges/challenges/stream-windows-easy/workload"
)

const (
	// ArenaID is the stable public identifier for this variant.
	ArenaID = "stream-windows-easy"
	// ArenaVersion is the immutable public version for this variant.
	ArenaVersion = "1.0.0"
	// WorkloadID identifies the deterministic public smoke workload.
	WorkloadID = "public-smoke-v1"
	// ticksPerLogicalSecond converts the injected virtual tick into a logical
	// second for the events_per_second rate metric.
	ticksPerLogicalSecond uint64 = 1_000_000
)

// Metrics are deterministic logical measurements from one workload run.
type Metrics struct {
	EventsPerSecond uint64 `json:"events_per_second"`
	PeakStateBytes  uint64 `json:"peak_state_bytes"`
	OutputDelay     uint64 `json:"output_delay"`
	AllocBytes      uint64 `json:"alloc_bytes"`
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
	WindowSize           uint64
	Windows              int
	Keys                 int
	EventsPerWindowUpper int
	EmptyEvery           int
	EmitEmpty            bool
}

// DefaultPublicConfig returns the fast public smoke workload.
func DefaultPublicConfig() PublicConfig {
	return PublicConfig{
		WindowSize:           1000,
		Windows:              100,
		Keys:                 16,
		EventsPerWindowUpper: 8,
		EmptyEvery:           7,
		EmitEmpty:            false,
	}
}

// Evaluate runs the deterministic public workload against a contestant
// operator and returns the versioned result envelope.
func Evaluate(seed uint64, config PublicConfig, factory contract.Factory) (result Result) {
	result = Result{
		ProtocolVersion: 1,
		ArenaID:         ArenaID,
		ArenaVersion:    ArenaVersion,
		Seed:            strconv.FormatUint(seed, 10),
		Verdict:         "pass",
		Score:           0,
		Violations:      []string{},
		Run: RunInfo{
			WorkloadID: WorkloadID,
		},
	}

	previousGC := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(previousGC)

	cfg := contract.Config{
		WindowSize: config.WindowSize,
		EmitEmpty:  config.EmitEmpty,
	}
	windowSize := cfg.WindowSizeEffective()
	if windowSize == 0 {
		result.Violations = append(result.Violations, "invalid public config: window size must be positive")
		result.Verdict = "fail"
		return result
	}

	steps := workload.Generate(workload.Config{
		Seed:                 seed,
		WindowSize:           config.WindowSize,
		Windows:              config.Windows,
		Keys:                 config.Keys,
		EventsPerWindowUpper: config.EventsPerWindowUpper,
		EmptyEvery:           config.EmptyEvery,
	})

	runner := runStream(seed, cfg, factory, steps)
	return runner.result()
}

type windowKey struct {
	start uint64
	key   uint64
}

type aggModel struct {
	count uint64
	sum   int64
}

type actualOutput struct {
	output   contract.Output
	emitTick uint64
}

type runner struct {
	seed    uint64
	cfg     contract.Config
	op      contract.Operator
	steps   []workload.Step
	created bool

	// model mirrors the operator's expected active windows.
	model     map[windowKey]*aggModel
	nextStart uint64 // EmitEmpty scan cursor

	actual   []actualOutput
	expected []contract.Output

	now             uint64
	lastEventTick   uint64
	hasEvent        bool
	lastWatermark   uint64
	hasWatermark    bool
	eventsProcessed uint64
	peakState       uint64
	outputDelay     uint64
	allocBytes      uint64
	closeTick       uint64
	violations      []string
}

// runStream executes an explicit deterministic schedule against a freshly
// constructed operator. Public tests use it to craft boundary and adversarial
// schedules; Evaluate builds the schedule from the public workload generator.
func runStream(seed uint64, cfg contract.Config, factory contract.Factory, steps []workload.Step) *runner {
	r := &runner{
		seed:       seed,
		cfg:        cfg,
		steps:      append([]workload.Step(nil), steps...),
		model:      make(map[windowKey]*aggModel),
		violations: []string{},
	}

	if factory == nil {
		r.violations = append(r.violations, "operator factory is nil")
		return r
	}

	if !r.call(func() {
		r.op = factory(cfg)
	}) {
		return r
	}
	if r.op == nil {
		r.violations = append(r.violations, "operator factory returned a nil operator")
		return r
	}
	r.created = true
	r.sampleState()

	for _, step := range r.steps {
		if !r.applyStep(step) {
			break
		}
		if len(r.violations) >= 8 {
			break
		}
	}

	// Close is terminal and finalizes every remaining active window. The
	// virtual clock advances far enough to cover the end of every remaining
	// active window so Close outputs are never treated as premature.
	if r.created {
		r.sampleState()
		windowSize := r.cfg.WindowSizeEffective()
		r.closeTick = satAdd(r.now, 1)
		for key := range r.model {
			if end := key.start + windowSize; end > r.closeTick {
				r.closeTick = end
			}
		}
		var outputs []contract.Output
		if r.call(func() {
			outputs = r.op.Close()
		}) {
			r.recordOutputs(outputs, r.closeTick)
		}
		r.finalizeAllModel()
		r.sampleState()
	}

	r.compareOutputs()
	return r
}

func (r *runner) result() Result {
	result := Result{
		ProtocolVersion: 1,
		ArenaID:         ArenaID,
		ArenaVersion:    ArenaVersion,
		Seed:            strconv.FormatUint(r.seed, 10),
		Verdict:         "pass",
		Score:           0,
		Violations:      []string{},
		Run: RunInfo{
			WorkloadID: WorkloadID,
		},
	}

	logicalTicks := r.closeTick
	if logicalTicks == 0 {
		logicalTicks = 1
	}
	result.Metrics = Metrics{
		EventsPerSecond: r.eventsProcessed * ticksPerLogicalSecond / logicalTicks,
		PeakStateBytes:  r.peakState,
		OutputDelay:     r.outputDelay,
		AllocBytes:      r.allocBytes,
	}
	result.Run.PeakMemoryBytes = r.peakState

	if len(r.violations) > 0 {
		result.Violations = append(result.Violations, r.violations...)
		result.Verdict = "fail"
	}
	return result
}

func (r *runner) applyStep(step workload.Step) bool {
	windowSize := r.cfg.WindowSizeEffective()
	switch step.Kind {
	case workload.StepEvent:
		if r.hasEvent && step.Tick < r.lastEventTick {
			r.violations = append(r.violations, "event tick regressed")
			return false
		}
		if r.hasWatermark && step.Tick <= r.lastWatermark {
			r.violations = append(r.violations, "event arrived at or before the watermark")
			return false
		}
		if step.Tick > contract.MaxLegalTick(windowSize) {
			r.violations = append(r.violations, "event tick exceeds maximum legal tick")
			return false
		}

		r.lastEventTick = step.Tick
		r.hasEvent = true
		r.now = step.Tick

		var outputs []contract.Output
		if !r.call(func() {
			outputs = r.op.OnEvent(contract.Event{
				ID:        r.eventsProcessed,
				EventTick: step.Tick,
				Key:       step.Key,
				Value:     step.Value,
			})
		}) {
			return false
		}
		r.recordOutputs(outputs, step.Tick)
		r.addModel(step)
		r.eventsProcessed++
		r.sampleState()
		return true

	case workload.StepWatermark:
		if r.hasWatermark && step.Tick < r.lastWatermark {
			r.violations = append(r.violations, "watermark regressed")
			return false
		}
		r.lastWatermark = step.Tick
		r.hasWatermark = true
		r.now = step.Tick

		var outputs []contract.Output
		if !r.call(func() {
			outputs = r.op.OnWatermark(step.Tick)
		}) {
			return false
		}
		r.recordOutputs(outputs, step.Tick)
		r.finalizeModel(step.Tick)
		r.sampleState()
		return true

	default:
		r.violations = append(r.violations, "unknown workload step kind")
		return false
	}
}

func (r *runner) addModel(step workload.Step) {
	windowSize := r.cfg.WindowSizeEffective()
	start := (step.Tick / windowSize) * windowSize
	key := windowKey{start: start, key: step.Key}
	agg := r.model[key]
	if agg == nil {
		agg = &aggModel{}
		r.model[key] = agg
	}
	agg.count++
	agg.sum += step.Value
}

func (r *runner) finalizeModel(tick uint64) {
	windowSize := r.cfg.WindowSizeEffective()
	if tick < windowSize {
		return
	}
	limit := tick - windowSize

	if r.cfg.EmitEmpty {
		for r.nextStart <= limit {
			start := r.nextStart
			end := start + windowSize
			emitted := false
			for key, agg := range r.model {
				if key.start != start {
					continue
				}
				r.expected = append(r.expected, contract.Output{
					WindowStart: start,
					WindowEnd:   end,
					Key:         key.key,
					Count:       agg.count,
					Sum:         agg.sum,
					Final:       true,
				})
				delete(r.model, key)
				emitted = true
			}
			if !emitted {
				r.expected = append(r.expected, contract.Output{
					WindowStart: start,
					WindowEnd:   end,
					Final:       true,
				})
			}
			r.nextStart = end
		}
		return
	}

	for key, agg := range r.model {
		if key.start > limit {
			continue
		}
		r.expected = append(r.expected, contract.Output{
			WindowStart: key.start,
			WindowEnd:   key.start + windowSize,
			Key:         key.key,
			Count:       agg.count,
			Sum:         agg.sum,
			Final:       true,
		})
		delete(r.model, key)
	}
}

// finalizeAllModel mirrors the operator's Close: every remaining active window
// is emitted exactly once, and empty future windows are never emitted even
// when EmitEmpty is set.
func (r *runner) finalizeAllModel() {
	windowSize := r.cfg.WindowSizeEffective()
	for key, agg := range r.model {
		r.expected = append(r.expected, contract.Output{
			WindowStart: key.start,
			WindowEnd:   key.start + windowSize,
			Key:         key.key,
			Count:       agg.count,
			Sum:         agg.sum,
			Final:       true,
		})
		delete(r.model, key)
	}
}

func (r *runner) recordOutputs(outputs []contract.Output, emitTick uint64) {
	windowSize := r.cfg.WindowSizeEffective()
	for _, output := range outputs {
		if !output.Final {
			r.violations = append(r.violations, "non-final output emitted")
			continue
		}
		if output.WindowEnd != output.WindowStart+windowSize || output.WindowStart%windowSize != 0 {
			r.violations = append(r.violations, "malformed window output")
			continue
		}
		if output.WindowEnd > emitTick {
			r.violations = append(r.violations, "final output emitted before its window end")
			continue
		}
		r.actual = append(r.actual, actualOutput{output: output, emitTick: emitTick})
		r.outputDelay += emitTick - output.WindowEnd
	}
}

func (r *runner) compareOutputs() {
	actual := make([]contract.Output, len(r.actual))
	for index := range r.actual {
		actual[index] = r.actual[index].output
	}
	sortOutputs(actual)
	for index := 1; index < len(actual); index++ {
		if sameRecord(actual[index], actual[index-1]) {
			r.violations = append(r.violations, "duplicate final output")
		}
	}

	expected := append([]contract.Output(nil), r.expected...)
	sortOutputs(expected)

	if len(actual) != len(expected) {
		r.violations = append(r.violations, "final outputs do not match expected records")
		return
	}
	for index := range actual {
		if actual[index].WindowStart != expected[index].WindowStart ||
			actual[index].WindowEnd != expected[index].WindowEnd ||
			actual[index].Key != expected[index].Key ||
			actual[index].Count != expected[index].Count ||
			actual[index].Sum != expected[index].Sum {
			r.violations = append(r.violations, "final outputs do not match expected records")
			return
		}
	}
}

func (r *runner) sampleState() {
	if !r.created {
		return
	}
	var state uint64
	if !r.call(func() {
		state = r.op.StateBytes()
	}) {
		return
	}
	if state > r.peakState {
		r.peakState = state
	}
}

// call runs one contestant-owned call inside a panic boundary and charges the
// operator for the heap bytes allocated by the call. GC is disabled for the
// duration of Evaluate so the cumulative allocation counter stays
// deterministic.
func (r *runner) call(fn func()) (ok bool) {
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	defer func() {
		if recover() != nil {
			r.violations = append(r.violations, "contestant operator panicked")
			ok = false
		}
		runtime.ReadMemStats(&after)
		r.allocBytes += after.TotalAlloc - before.TotalAlloc
	}()
	fn()
	return true
}

// satAdd returns a+b, clamped to MaxUint64 when the sum would overflow. It
// keeps the virtual clock monotonic at the maximum legal watermark instead of
// wrapping to zero and corrupting the events_per_second denominator.
func satAdd(a, b uint64) uint64 {
	if ^uint64(0)-a < b {
		return ^uint64(0)
	}
	return a + b
}

func sortOutputs(outputs []contract.Output) {
	sort.SliceStable(outputs, func(i, j int) bool {
		if outputs[i].WindowStart != outputs[j].WindowStart {
			return outputs[i].WindowStart < outputs[j].WindowStart
		}
		return outputs[i].Key < outputs[j].Key
	})
}

func sameRecord(a, b contract.Output) bool {
	return a.WindowStart == b.WindowStart &&
		a.WindowEnd == b.WindowEnd &&
		a.Key == b.Key &&
		a.Count == b.Count &&
		a.Sum == b.Sum
}
