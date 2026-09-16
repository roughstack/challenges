// Package contract defines the narrow contestant-owned surface for the
// stream-windows-easy arena. Everything else — the deterministic event stream,
// watermark schedule, output canonicalization, accounting, and result
// construction — is owned by the trusted harness.
package contract

// DefaultWindowSize is the window length in event-time ticks used when the
// config leaves WindowSize unset or zero.
const DefaultWindowSize uint64 = 1000

// Config is the immutable per-run configuration handed to the operator.
type Config struct {
	// WindowSize is the fixed, non-overlapping window length in event-time
	// ticks. A window covers [start, start+WindowSize). A zero value falls back
	// to DefaultWindowSize.
	WindowSize uint64
	// EmitEmpty requests a final output for every empty window whose end is
	// crossed by a watermark. When false, empty windows produce no output.
	EmitEmpty bool
}

// WindowSizeEffective returns the configured window length with the zero value
// resolved to the default.
func (c Config) WindowSizeEffective() uint64 {
	if c.WindowSize == 0 {
		return DefaultWindowSize
	}
	return c.WindowSize
}

// MaxLegalTick returns the largest event tick whose containing window end still
// fits in a uint64 for the given window size. A window is
// [tick/tickSize*tickSize, tick/tickSize*tickSize+tickSize), so the last legal
// window end is the largest multiple of tickSize that is <= MaxUint64.
func MaxLegalTick(windowSize uint64) uint64 {
	if windowSize == 0 {
		windowSize = DefaultWindowSize
	}
	return ^uint64(0) - (^uint64(0) % windowSize) - 1
}

// Event is one in-order service event delivered to the operator.
type Event struct {
	ID        uint64
	EventTick uint64
	Key       uint64
	Value     int64
}

// Output is one final windowed aggregate emitted by the operator. WindowStart
// and WindowEnd are aligned to the configured window size, Count is the number
// of events in the window for Key, and Sum is their exact int64 sum.
type Output struct {
	WindowStart uint64
	WindowEnd   uint64
	Key         uint64
	Count       uint64
	Sum         int64
	Final       bool
}

// Operator is the complete contestant-owned surface for this arena.
//
//   - OnEvent delivers one event and returns any final outputs it produces.
//   - OnWatermark advances event time to tick. Watermarks are monotonic and
//     promise that no on-time event at or before tick will arrive later. The
//     operator must finalize every active window whose end is <= tick exactly
//     once.
//   - Close is terminal: it finalizes every remaining active window exactly
//     once.
//   - StateBytes reports the current live operator state size in bytes.
//
// The harness compares final outputs as canonical records, not by callback
// batch boundaries.
type Operator interface {
	OnEvent(Event) []Output
	OnWatermark(tick uint64) []Output
	Close() []Output
	StateBytes() uint64
}

// Factory constructs a fresh operator for one isolated workload run.
type Factory func(cfg Config) Operator
