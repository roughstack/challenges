// Package starter is the learner-facing scaffold for event-time windows.
package starter

import "github.com/roughstack/challenges/challenges/stream-windows-easy/contract"

type operator struct {
	cfg contract.Config
	// TODO: maintain per-key, per-window aggregates and watermark state.
}

func NewOperator(cfg contract.Config) contract.Operator {
	return &operator{cfg: cfg}
}

func (o *operator) OnEvent(event contract.Event) []contract.Output {
	panic("TODO: assign this event to its half-open window")
}

func (o *operator) OnWatermark(tick uint64) []contract.Output {
	panic("TODO: finalize exactly the windows behind the watermark")
}

func (o *operator) Close() []contract.Output {
	panic("TODO: finalize every remaining window once")
}

func (o *operator) StateBytes() uint64 {
	panic("TODO: report live aggregate state")
}
