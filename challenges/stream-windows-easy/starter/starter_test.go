package starter

import (
	"testing"

	"github.com/roughstack/challenges/challenges/stream-windows-easy/contract"
)

func testConfig(windowSize uint64, emitEmpty bool) contract.Config {
	return contract.Config{WindowSize: windowSize, EmitEmpty: emitEmpty}
}

func TestBasicAggregationEmitsOnce(t *testing.T) {
	op := NewOperator(testConfig(10, false))
	op.OnEvent(contract.Event{EventTick: 0, Key: 1, Value: 2})
	op.OnEvent(contract.Event{EventTick: 1, Key: 1, Value: 3})

	outputs := op.OnWatermark(10)
	if len(outputs) != 1 {
		t.Fatalf("outputs = %d, want 1: %+v", len(outputs), outputs)
	}
	got := outputs[0]
	if got.WindowStart != 0 || got.WindowEnd != 10 || got.Key != 1 || got.Count != 2 || got.Sum != 5 || !got.Final {
		t.Fatalf("unexpected output: %+v", got)
	}
	if op.StateBytes() != 0 {
		t.Fatalf("finalized window was not evicted: state bytes %d", op.StateBytes())
	}
	if again := op.OnWatermark(10); len(again) != 0 {
		t.Fatalf("repeated watermark emitted again: %+v", again)
	}
}

func TestBoundaryEventsLandInAdjacentWindows(t *testing.T) {
	op := NewOperator(testConfig(10, false))
	op.OnEvent(contract.Event{EventTick: 9, Key: 7, Value: 1})  // window end-1
	op.OnEvent(contract.Event{EventTick: 10, Key: 7, Value: 2}) // next window start

	first := op.OnWatermark(10)
	if len(first) != 1 || first[0].WindowStart != 0 || first[0].Sum != 1 {
		t.Fatalf("first window output wrong: %+v", first)
	}

	second := op.OnWatermark(20)
	if len(second) != 1 || second[0].WindowStart != 10 || second[0].Sum != 2 {
		t.Fatalf("second window output wrong: %+v", second)
	}
}

func TestEmptyWindowProducesNoOutputUnlessRequested(t *testing.T) {
	silent := NewOperator(testConfig(10, false))
	if outputs := silent.OnWatermark(10); len(outputs) != 0 {
		t.Fatalf("empty window produced output: %+v", outputs)
	}

	explicit := NewOperator(testConfig(10, true))
	outputs := explicit.OnWatermark(10)
	if len(outputs) != 1 || outputs[0].Key != 0 || outputs[0].Count != 0 || outputs[0].Sum != 0 {
		t.Fatalf("requested empty output wrong: %+v", outputs)
	}
}

func TestInterleavedKeysAggregateIndependently(t *testing.T) {
	op := NewOperator(testConfig(10, false))
	op.OnEvent(contract.Event{EventTick: 0, Key: 1, Value: 1})
	op.OnEvent(contract.Event{EventTick: 0, Key: 2, Value: 10})
	op.OnEvent(contract.Event{EventTick: 1, Key: 1, Value: 2})

	outputs := op.OnWatermark(10)
	if len(outputs) != 2 {
		t.Fatalf("outputs = %d, want 2: %+v", len(outputs), outputs)
	}
	if outputs[0].Key != 1 || outputs[0].Count != 2 || outputs[0].Sum != 3 {
		t.Fatalf("key 1 aggregate wrong: %+v", outputs[0])
	}
	if outputs[1].Key != 2 || outputs[1].Count != 1 || outputs[1].Sum != 10 {
		t.Fatalf("key 2 aggregate wrong: %+v", outputs[1])
	}
}

func TestCloseFinalizesRemainingWindows(t *testing.T) {
	op := NewOperator(testConfig(10, false))
	op.OnEvent(contract.Event{EventTick: 0, Key: 1, Value: 5})

	outputs := op.Close()
	if len(outputs) != 1 || outputs[0].Sum != 5 || outputs[0].Count != 1 {
		t.Fatalf("Close output wrong: %+v", outputs)
	}
	if again := op.Close(); len(again) != 0 {
		t.Fatalf("second Close emitted again: %+v", again)
	}
	if op.StateBytes() != 0 {
		t.Fatalf("Close did not evict state: %d", op.StateBytes())
	}
}

func TestStateBytesReflectsLiveWindows(t *testing.T) {
	op := NewOperator(testConfig(10, false))
	if op.StateBytes() != 0 {
		t.Fatalf("empty operator reported %d bytes", op.StateBytes())
	}

	op.OnEvent(contract.Event{EventTick: 0, Key: 1, Value: 1})
	op.OnEvent(contract.Event{EventTick: 0, Key: 2, Value: 1})
	if live := op.StateBytes(); live == 0 {
		t.Fatal("active windows reported zero state bytes")
	}

	op.OnWatermark(10)
	if op.StateBytes() != 0 {
		t.Fatalf("finalized windows still occupy state: %d", op.StateBytes())
	}
}

func TestNegativeAndZeroValues(t *testing.T) {
	op := NewOperator(testConfig(10, false))
	op.OnEvent(contract.Event{EventTick: 0, Key: 1, Value: -5})
	op.OnEvent(contract.Event{EventTick: 1, Key: 1, Value: 0})

	outputs := op.OnWatermark(10)
	if len(outputs) != 1 || outputs[0].Count != 2 || outputs[0].Sum != -5 {
		t.Fatalf("negative/zero aggregate wrong: %+v", outputs)
	}
}

func TestTickZeroAndMaximumLegalTick(t *testing.T) {
	op := NewOperator(testConfig(1000, false))
	op.OnEvent(contract.Event{EventTick: 0, Key: 1, Value: 1})

	max := contract.MaxLegalTick(1000)
	op.OnEvent(contract.Event{EventTick: max, Key: 2, Value: 2})

	outputs := op.OnWatermark(^uint64(0))
	if len(outputs) != 2 {
		t.Fatalf("outputs = %d, want 2: %+v", len(outputs), outputs)
	}
	if outputs[0].WindowEnd > ^uint64(0) {
		t.Fatalf("maximum legal window end overflowed: %+v", outputs[0])
	}
}
