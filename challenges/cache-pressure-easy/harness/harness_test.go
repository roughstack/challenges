package harness

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/bytearena/arenas/arenas/cache-pressure-easy/starter"
)

func TestEvaluateIsDeterministicAndWithinCapacity(t *testing.T) {
	config := DefaultPublicConfig()
	first := Evaluate(^uint64(0), config, starter.NewCache)
	second := Evaluate(^uint64(0), config, starter.NewCache)

	if !reflect.DeepEqual(first, second) {
		t.Fatalf("same seed produced different results:\n%+v\n%+v", first, second)
	}
	if first.Verdict != "pass" || len(first.Violations) != 0 {
		t.Fatalf("unexpected verdict: %+v", first)
	}
	if first.Metrics.BackingReads == 0 {
		t.Fatal("workload did not exercise backing reads")
	}
	if first.Metrics.PeakAccountedBytes > config.CapacityBytes {
		t.Fatal("peak accounted bytes exceeded capacity")
	}
}

func TestResultEnvelopeContainsEveryPublicMetric(t *testing.T) {
	result := Evaluate(7, DefaultPublicConfig(), starter.NewCache)
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}

	var envelope map[string]any
	if err := json.Unmarshal(encoded, &envelope); err != nil {
		t.Fatal(err)
	}
	metrics, ok := envelope["metrics"].(map[string]any)
	if !ok {
		t.Fatal("result metrics are not an object")
	}
	for _, metric := range []string{"backing_reads", "metadata_work", "peak_accounted_bytes"} {
		if _, ok := metrics[metric]; !ok {
			t.Fatalf("result is missing metric %q", metric)
		}
	}
}
