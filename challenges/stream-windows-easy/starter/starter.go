// Package starter contains a deliberately simple, correct streaming window
// operator. It keeps one map entry per active (window start, key) aggregate,
// finalizes windows exactly once on watermark progress, and recomputes its
// state-byte report by walking live entries. It is intentionally conservative:
// no flat key encoding, no window ring, no indexed eviction.
package starter

import (
	"sort"

	"github.com/roughstack/challenges/challenges/stream-windows-easy/contract"
)

// Factory constructs the public operator. It is referenced by the harness and
// smoke command only; the contestant replaces the body behind this constructor.
var Factory = contract.Factory(NewOperator)

// Accounting constants describe the deterministic state-byte model used by the
// starter's StateBytes report. They intentionally overestimate Go's own map and
// struct overhead so live-state growth is visible and monotonic.
const (
	windowOverheadBytes uint64 = 64
	aggregateBytes      uint64 = 32
)

type aggregate struct {
	count uint64
	sum   int64
}

type operator struct {
	cfg contract.Config

	// windows maps an active window start to its per-key aggregates. An entry
	// exists only while the window has at least one event and has not been
	// finalized.
	windows map[uint64]map[uint64]*aggregate

	// nextStart is used only when EmitEmpty is set. It is the smallest window
	// start that has not yet been scanned for empty-window emission.
	nextStart uint64

	lastWatermark uint64
	hasWatermark  bool
}

// NewOperator returns a correct, deliberately simple windowed aggregator.
func NewOperator(cfg contract.Config) contract.Operator {
	return &operator{
		cfg:     cfg,
		windows: make(map[uint64]map[uint64]*aggregate),
	}
}

func (o *operator) OnEvent(event contract.Event) []contract.Output {
	// The harness guarantees in-order events after the watermark promise. Being
	// defensive here is cheap and keeps the operator safe if driven directly.
	if o.hasWatermark && event.EventTick <= o.lastWatermark {
		return nil
	}

	start := windowStart(event.EventTick, o.cfg.WindowSizeEffective())
	keys := o.windows[start]
	if keys == nil {
		keys = make(map[uint64]*aggregate)
		o.windows[start] = keys
	}
	agg := keys[event.Key]
	if agg == nil {
		agg = &aggregate{}
		keys[event.Key] = agg
	}
	agg.count++
	agg.sum += event.Value
	return nil
}

func (o *operator) OnWatermark(tick uint64) []contract.Output {
	// The harness rejects a regressing watermark as a violation. Ignore a
	// regression defensively rather than corrupting internal progress.
	if o.hasWatermark && tick < o.lastWatermark {
		return nil
	}
	o.lastWatermark = tick
	o.hasWatermark = true
	return o.finalizeUpTo(tick)
}

func (o *operator) Close() []contract.Output {
	// Close finalizes only active windows. Empty future windows are not active
	// and are never emitted here, even when EmitEmpty is set.
	out := make([]contract.Output, 0, len(o.windows))
	windowSize := o.cfg.WindowSizeEffective()
	for start, keys := range o.windows {
		for key, agg := range keys {
			out = append(out, contract.Output{
				WindowStart: start,
				WindowEnd:   start + windowSize,
				Key:         key,
				Count:       agg.count,
				Sum:         agg.sum,
				Final:       true,
			})
		}
	}
	o.windows = make(map[uint64]map[uint64]*aggregate)
	sortOutputs(out)
	return out
}

func (o *operator) StateBytes() uint64 {
	bytes := uint64(0)
	for _, keys := range o.windows {
		bytes += windowOverheadBytes
		for range keys {
			bytes += aggregateBytes
		}
	}
	return bytes
}

// finalizeUpTo emits every window whose end is <= tick and removes it from live
// state. When EmitEmpty is set it also emits empty windows crossed by tick.
func (o *operator) finalizeUpTo(tick uint64) []contract.Output {
	windowSize := o.cfg.WindowSizeEffective()
	if tick < windowSize {
		return nil
	}
	limit := tick - windowSize

	var out []contract.Output
	if o.cfg.EmitEmpty {
		for o.nextStart <= limit {
			start := o.nextStart
			end := start + windowSize
			keys := o.windows[start]
			if len(keys) == 0 {
				out = append(out, contract.Output{
					WindowStart: start,
					WindowEnd:   end,
					Final:       true,
				})
			} else {
				for key, agg := range keys {
					out = append(out, contract.Output{
						WindowStart: start,
						WindowEnd:   end,
						Key:         key,
						Count:       agg.count,
						Sum:         agg.sum,
						Final:       true,
					})
				}
				delete(o.windows, start)
			}
			o.nextStart = end
		}
	} else {
		for start, keys := range o.windows {
			if start > limit {
				continue
			}
			for key, agg := range keys {
				out = append(out, contract.Output{
					WindowStart: start,
					WindowEnd:   start + windowSize,
					Key:         key,
					Count:       agg.count,
					Sum:         agg.sum,
					Final:       true,
				})
			}
			delete(o.windows, start)
		}
	}

	sortOutputs(out)
	return out
}

func windowStart(tick, windowSize uint64) uint64 {
	return (tick / windowSize) * windowSize
}

func sortOutputs(outputs []contract.Output) {
	sort.SliceStable(outputs, func(i, j int) bool {
		if outputs[i].WindowStart != outputs[j].WindowStart {
			return outputs[i].WindowStart < outputs[j].WindowStart
		}
		return outputs[i].Key < outputs[j].Key
	})
}
