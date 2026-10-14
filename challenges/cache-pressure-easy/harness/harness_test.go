package harness

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/roughstack/challenges/challenges/cache-pressure-easy/contract"
	"github.com/roughstack/challenges/challenges/cache-pressure-easy/starter"
)

type neverAdmitCache struct{}

func (neverAdmitCache) Get(uint64, contract.RequestMeta) ([]byte, bool) { return nil, false }
func (neverAdmitCache) Put(uint64, []byte, contract.RequestMeta)        {}
func (neverAdmitCache) Delete(uint64)                                   {}
func (neverAdmitCache) UsedBytes() uint64                               { return 0 }

type ignoreDeleteCache struct{ contract.Cache }

func (ignoreDeleteCache) Delete(uint64) {}

func TestEvaluateRejectsCachesThatDoNotImplementTheContract(t *testing.T) {
	tests := []struct {
		name    string
		factory contract.Factory
		want    string
	}{
		{"never admits", func(uint64) contract.Cache { return neverAdmitCache{} }, "admit"},
		{"ignores delete", func(capacity uint64) contract.Cache {
			return ignoreDeleteCache{starter.NewCache(capacity)}
		}, "Delete"},
		{"nil cache", func(uint64) contract.Cache { return nil }, "nil"},
		{"nil factory", nil, "nil"},
		{"panicking factory", func(uint64) contract.Cache { panic("broken") }, "panicked"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := Evaluate(17, DefaultPublicConfig(), test.factory)
			if result.Verdict != "fail" || len(result.Violations) == 0 || !strings.Contains(result.Violations[0], test.want) {
				t.Fatalf("expected %q failure, got %+v", test.want, result)
			}
		})
	}
}

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
