package harness

import (
	"context"
	"encoding/json"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/roughstack/challenges/challenges/durable-queue-easy/contract"
	"github.com/roughstack/challenges/challenges/durable-queue-easy/starter"
)

func TestEvaluateIsDeterministicAndConverges(t *testing.T) {
	config := DefaultPublicConfig()
	first := Evaluate(^uint64(0), config, starter.OpenQueue)
	second := Evaluate(^uint64(0), config, starter.OpenQueue)

	if !reflect.DeepEqual(first, second) {
		t.Fatalf("same seed produced different results:\n%+v\n%+v", first, second)
	}
	if first.Verdict != "pass" || len(first.Violations) != 0 {
		t.Fatalf("unexpected verdict: %+v", first)
	}
	if first.Metrics.CompletedJobs != uint64(config.Jobs) {
		t.Fatalf("completed jobs = %d, want %d", first.Metrics.CompletedJobs, config.Jobs)
	}
	if first.Metrics.SyncWork == 0 {
		t.Fatal("workload did not charge synchronization work")
	}
	if first.Metrics.PeakBytes > config.CapacityBytes {
		t.Fatal("peak bytes exceeded capacity")
	}
	if first.Metrics.LogicalQueueWait == 0 {
		t.Fatal("queue wait was not measured")
	}
}

func TestResultEnvelopeContainsEveryPublicMetric(t *testing.T) {
	result := Evaluate(7, DefaultPublicConfig(), starter.OpenQueue)
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
	for _, metric := range []string{"completed_jobs", "logical_queue_wait", "sync_work", "peak_bytes"} {
		if _, ok := metrics[metric]; !ok {
			t.Fatalf("result is missing metric %q", metric)
		}
	}
	for _, field := range []string{"protocol_version", "arena_id", "arena_version", "seed", "verdict", "score", "run"} {
		if _, ok := envelope[field]; !ok {
			t.Fatalf("result is missing field %q", field)
		}
	}
}

// A queue that ignores the capacity bound must fail the correctness gate.
type unboundedQueue struct{}

func (unboundedQueue) Enqueue(context.Context, contract.Job) error { return nil }
func (unboundedQueue) Lease(context.Context, uint64, uint64) (contract.Lease, error) {
	return contract.Lease{Job: contract.Job{ID: 1}, Token: 1, Deadline: 0, Attempt: 1}, nil
}
func (unboundedQueue) Ack(context.Context, uint64) error  { return nil }
func (unboundedQueue) Nack(context.Context, uint64) error { return nil }
func (unboundedQueue) Close() error                       { return nil }

func TestEvaluateGatesCorrectnessFailures(t *testing.T) {
	factory := func(string, uint64) (contract.Queue, error) { return unboundedQueue{}, nil }
	result := Evaluate(9, DefaultPublicConfig(), factory)
	if result.Verdict != "fail" {
		t.Fatalf("verdict = %q, want fail; violations: %v", result.Verdict, result.Violations)
	}
	if result.Score != 0 {
		t.Fatalf("failed run reported a non-zero score: %d", result.Score)
	}
}

// A nil queue is a malformed contestant and must fail closed without a panic.
func TestEvaluateRejectsNilQueue(t *testing.T) {
	factory := func(string, uint64) (contract.Queue, error) { return nil, nil }
	result := Evaluate(1, DefaultPublicConfig(), factory)
	if result.Verdict != "fail" {
		t.Fatalf("verdict = %q, want fail; violations: %v", result.Verdict, result.Violations)
	}
	if !containsViolation(result.Violations, "OpenQueue returned a nil queue") {
		t.Fatalf("violations = %v, want the nil-queue violation", result.Violations)
	}
}

type panickingQueue struct{}

func (panickingQueue) Enqueue(context.Context, contract.Job) error { panic("boom") }
func (panickingQueue) Lease(context.Context, uint64, uint64) (contract.Lease, error) {
	return contract.Lease{}, nil
}
func (panickingQueue) Ack(context.Context, uint64) error  { return nil }
func (panickingQueue) Nack(context.Context, uint64) error { return nil }
func (panickingQueue) Close() error                       { return nil }

func TestEvaluateIsolatesContestantPanic(t *testing.T) {
	factory := func(string, uint64) (contract.Queue, error) { return panickingQueue{}, nil }
	result := Evaluate(1, DefaultPublicConfig(), factory)
	if result.Verdict != "fail" {
		t.Fatalf("verdict = %q, want fail; violations: %v", result.Verdict, result.Violations)
	}
	if !containsViolation(result.Violations, "contestant queue panicked") {
		t.Fatalf("violations = %v, want the contestant-panic violation", result.Violations)
	}
	if result.ProtocolVersion != 1 || result.ArenaID != ArenaID {
		t.Fatalf("result envelope was not returned intact: %+v", result)
	}
}

type blockingQueue struct {
	mu        sync.Mutex
	calls     []string
	block     chan struct{}
	started   chan struct{}
	startOnce sync.Once
}

func (q *blockingQueue) record(call string) {
	q.mu.Lock()
	q.calls = append(q.calls, call)
	q.mu.Unlock()
}

func (q *blockingQueue) Enqueue(context.Context, contract.Job) error {
	q.record("Enqueue")
	q.startOnce.Do(func() { close(q.started) })
	<-q.block
	return nil
}
func (q *blockingQueue) Lease(context.Context, uint64, uint64) (contract.Lease, error) {
	q.record("Lease")
	return contract.Lease{}, nil
}
func (q *blockingQueue) Ack(context.Context, uint64) error {
	q.record("Ack")
	return nil
}
func (q *blockingQueue) Nack(context.Context, uint64) error {
	q.record("Nack")
	return nil
}
func (q *blockingQueue) Close() error {
	q.record("Close")
	return nil
}

func (q *blockingQueue) recordedCalls() []string {
	q.mu.Lock()
	defer q.mu.Unlock()
	return append([]string(nil), q.calls...)
}

func TestEvaluateWatchdogCutsOffBlockingQueue(t *testing.T) {
	original := watchdogBudget
	watchdogBudget = 100 * time.Millisecond
	defer func() { watchdogBudget = original }()

	q := &blockingQueue{block: make(chan struct{}), started: make(chan struct{})}
	factory := func(string, uint64) (contract.Queue, error) { return q, nil }

	done := make(chan Result, 1)
	go func() { done <- Evaluate(1, DefaultPublicConfig(), factory) }()
	select {
	case <-q.started:
	case <-time.After(5 * time.Second):
		t.Fatal("blocking Enqueue was never entered")
	}
	result := <-done

	if result.Verdict != "fail" {
		t.Fatalf("verdict = %q, want fail; violations: %v", result.Verdict, result.Violations)
	}
	if !containsViolation(result.Violations, "contestant queue call did not return") {
		t.Fatalf("violations = %v, want the watchdog violation", result.Violations)
	}

	calls := q.recordedCalls()
	if len(calls) != 1 || calls[0] != "Enqueue" {
		t.Fatalf("queue calls after the blocking Enqueue = %v, want exactly one Enqueue", calls)
	}
}

func containsViolation(violations []string, want string) bool {
	for _, violation := range violations {
		if violation == want {
			return true
		}
	}
	return false
}
