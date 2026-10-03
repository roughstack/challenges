package workload

import (
	"reflect"
	"testing"
)

func TestGenerateIsDeterministic(t *testing.T) {
	cfg := Config{Seed: 99, WindowSize: 100, Windows: 12, Keys: 8, EventsPerWindowUpper: 6, EmptyEvery: 5}
	first := Generate(cfg)
	second := Generate(cfg)
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("same seed produced different schedules:\n%+v\n%+v", first, second)
	}

	other := Generate(Config{Seed: 100, WindowSize: 100, Windows: 12, Keys: 8, EventsPerWindowUpper: 6, EmptyEvery: 5})
	if reflect.DeepEqual(first, other) {
		t.Fatal("different seeds produced identical schedules")
	}
}

func TestGenerateKeepsEventsInOrderAndWatermarksMonotonic(t *testing.T) {
	steps := Generate(Config{Seed: 7, WindowSize: 10, Windows: 40, Keys: 4, EventsPerWindowUpper: 4, EmptyEvery: 6})
	var lastEvent, lastWatermark uint64
	seenEvent := false
	seenWatermark := false
	for _, step := range steps {
		switch step.Kind {
		case StepEvent:
			if seenEvent && step.Tick < lastEvent {
				t.Fatalf("event tick regressed: %d after %d", step.Tick, lastEvent)
			}
			lastEvent = step.Tick
			seenEvent = true
		case StepWatermark:
			if seenWatermark && step.Tick < lastWatermark {
				t.Fatalf("watermark regressed: %d after %d", step.Tick, lastWatermark)
			}
			lastWatermark = step.Tick
			seenWatermark = true
		}
	}
	if !seenEvent || !seenWatermark {
		t.Fatalf("schedule missing events or watermarks: %d steps", len(steps))
	}
}

func TestGenerateProducesRequestedEmptyWindows(t *testing.T) {
	const emptyEvery = 3
	steps := Generate(Config{Seed: 11, WindowSize: 10, Windows: 6, Keys: 2, EventsPerWindowUpper: 2, EmptyEvery: emptyEvery})

	emptyBoundaries := map[uint64]bool{}
	for w := 1; w <= 6; w++ {
		if w%emptyEvery == 0 {
			emptyBoundaries[uint64(w)*10] = true
		}
	}

	// An empty window has no events between its start and the watermark at its
	// end.
	for boundary := range emptyBoundaries {
		start := boundary - 10
		for _, step := range steps {
			if step.Kind == StepEvent && step.Tick >= start && step.Tick < boundary {
				t.Fatalf("empty window [%d,%d) contained event: %+v", start, boundary, step)
			}
		}
	}
}
