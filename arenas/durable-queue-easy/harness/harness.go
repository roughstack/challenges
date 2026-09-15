// Package harness owns public execution, virtual time, accounting, correctness
// gates, and result construction for the durable-queue-easy arena.
package harness

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"time"

	"github.com/bytearena/arenas/arenas/durable-queue-easy/contract"
	"github.com/bytearena/arenas/arenas/durable-queue-easy/workload"
)

const (
	// ArenaID is the stable public identifier for this variant.
	ArenaID = "durable-queue-easy"
	// ArenaVersion is the immutable public version for this variant.
	ArenaVersion = "1.0.0"
	// WorkloadID identifies the deterministic public smoke workload.
	WorkloadID = "public-smoke-v1"
)

// watchdogBudget is the judge-side wall-clock limit for a single contestant
// queue method call. It exists only to fail closed on a non-cooperative queue;
// a compliant deterministic queue never comes close to this budget, so results
// remain seed-deterministic.
var watchdogBudget = 10 * time.Second

// runAbort unwinds Evaluate after a contestant panic or watchdog timeout while
// still allowing the deferred recovery to return the result envelope.
type runAbort struct{}

// Metrics are deterministic logical measurements from one workload run.
type Metrics struct {
	CompletedJobs    uint64 `json:"completed_jobs"`
	LogicalQueueWait uint64 `json:"logical_queue_wait"`
	SyncWork         uint64 `json:"sync_work"`
	PeakBytes        uint64 `json:"peak_bytes"`
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
	CapacityBytes uint64
	Jobs          int
	Consumers     int
	Visibility    uint64
	MaxPayload    int
	NackEvery     int
	ExpireEvery   int
}

// DefaultPublicConfig returns the fast public smoke workload.
func DefaultPublicConfig() PublicConfig {
	return PublicConfig{
		CapacityBytes: 64 * 1024,
		Jobs:          1200,
		Consumers:     8,
		Visibility:    40,
		MaxPayload:    256,
		NackEvery:     13,
		ExpireEvery:   31,
	}
}

type leasedModel struct {
	id       uint64
	deadline uint64
}

type model struct {
	seed         uint64
	capacity     uint64
	visibility   uint64
	consumers    int
	jobs         []workload.Job
	scripts      [][]workload.Action
	scriptIdx    []int
	nextEnqueue  int
	resident     uint64
	peak         uint64
	completed    uint64
	available    []uint64 // expected lease order of job IDs
	leased       map[uint64]leasedModel
	nextAttempt  []uint32
	enqueuedTick []uint64
	waits        []uint64
	seenTokens   map[uint64]bool
}

// callOutcome carries the result of one guarded contestant call from the
// helper goroutine back to the Evaluate goroutine.
type callOutcome[T any] struct {
	value    T
	err      error
	panicked bool
}

// callGuard wraps every contestant queue call in a wall-clock watchdog and a
// panic boundary. It implements contract.Queue so the mirror-model methods can
// keep using the same call sites without changing sync-work accounting.
type callGuard struct {
	q      contract.Queue
	result *Result
}

func callQueue[T any](g *callGuard, fn func() (T, error)) (T, error) {
	done := make(chan callOutcome[T], 1)
	go func() {
		defer func() {
			if recover() != nil {
				done <- callOutcome[T]{panicked: true}
			}
		}()
		value, err := fn()
		done <- callOutcome[T]{value: value, err: err}
	}()

	timer := time.NewTimer(watchdogBudget)
	defer timer.Stop()
	select {
	case out := <-done:
		if out.panicked {
			g.result.Violations = append(g.result.Violations, "contestant queue panicked")
			g.result.Verdict = "fail"
			panic(runAbort{})
		}
		return out.value, out.err
	case <-timer.C:
		g.result.Violations = append(g.result.Violations, "contestant queue call did not return")
		g.result.Verdict = "fail"
		panic(runAbort{})
	}
}

func (g *callGuard) Enqueue(ctx context.Context, job contract.Job) error {
	_, err := callQueue(g, func() (struct{}, error) {
		return struct{}{}, g.q.Enqueue(ctx, job)
	})
	return err
}

func (g *callGuard) Lease(ctx context.Context, nowTick uint64, visibility uint64) (contract.Lease, error) {
	return callQueue(g, func() (contract.Lease, error) {
		return g.q.Lease(ctx, nowTick, visibility)
	})
}

func (g *callGuard) Ack(ctx context.Context, token uint64) error {
	_, err := callQueue(g, func() (struct{}, error) {
		return struct{}{}, g.q.Ack(ctx, token)
	})
	return err
}

func (g *callGuard) Nack(ctx context.Context, token uint64) error {
	_, err := callQueue(g, func() (struct{}, error) {
		return struct{}{}, g.q.Nack(ctx, token)
	})
	return err
}

func (g *callGuard) Close() error {
	_, err := callQueue(g, func() (struct{}, error) {
		return struct{}{}, g.q.Close()
	})
	return err
}

// Evaluate runs the deterministic public workload against a contestant queue.
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

	var m *model
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		if _, ok := r.(runAbort); !ok {
			panic(r)
		}
		if m != nil {
			result.Metrics.CompletedJobs = m.completed
			result.Metrics.LogicalQueueWait = percentile(m.waits, 0.99)
			result.Metrics.PeakBytes = m.peak
			result.Run.PeakMemoryBytes = m.peak
		}
		if len(result.Violations) > 0 {
			result.Verdict = "fail"
		}
	}()

	if err := normalize(&config); err != nil {
		result.Violations = append(result.Violations, "invalid public config: "+err.Error())
		result.Verdict = "fail"
		return result
	}

	jobs, firstActions := workload.Generate(workload.Config{
		Seed:        seed,
		Jobs:        config.Jobs,
		MaxPayload:  config.MaxPayload,
		NackEvery:   config.NackEvery,
		ExpireEvery: config.ExpireEvery,
	})

	q, err := factory("", config.CapacityBytes)
	if err != nil {
		result.Violations = append(result.Violations, "OpenQueue failed: "+err.Error())
		result.Verdict = "fail"
		return result
	}
	if q == nil {
		result.Violations = append(result.Violations, "OpenQueue returned a nil queue")
		result.Verdict = "fail"
		return result
	}

	g := &callGuard{q: q, result: &result}

	m = &model{
		seed:         seed,
		capacity:     config.CapacityBytes,
		visibility:   config.Visibility,
		consumers:    config.Consumers,
		jobs:         jobs,
		scripts:      make([][]workload.Action, len(jobs)+1),
		scriptIdx:    make([]int, len(jobs)+1),
		leased:       make(map[uint64]leasedModel),
		nextAttempt:  make([]uint32, len(jobs)+1),
		enqueuedTick: make([]uint64, len(jobs)+1),
		seenTokens:   make(map[uint64]bool),
	}
	for index, action := range firstActions {
		id := uint64(index + 1)
		switch action {
		case workload.ActionNack:
			m.scripts[id] = []workload.Action{workload.ActionNack, workload.ActionAck}
		case workload.ActionExpire:
			m.scripts[id] = []workload.Action{workload.ActionExpire, workload.ActionAck}
		default:
			m.scripts[id] = []workload.Action{workload.ActionAck}
		}
	}

	maxTicks := uint64(len(jobs))*(8+config.Visibility) + 10000
	tick := uint64(0)
	for m.completed < uint64(len(jobs)) && tick <= maxTicks {
		m.producerStep(tick, g, &result)
		expired := m.expire(tick)
		m.consumerStep(tick, g, &result)

		// The first consumer Lease above triggered the queue's lazy expiry, so
		// every token collected here must now be rejected as stale.
		for _, token := range expired {
			result.Metrics.SyncWork++
			if err := g.Ack(context.Background(), token); !errors.Is(err, contract.ErrStaleToken) {
				result.Violations = append(result.Violations, "stale token after visibility expiry was not rejected")
			}
		}

		if len(result.Violations) >= 8 {
			break
		}
		tick++
	}

	if m.completed < uint64(len(jobs)) {
		result.Violations = append(result.Violations, "workload did not converge within the logical tick budget")
	}

	// Close is idempotent and every later operation reports the closed error.
	result.Metrics.SyncWork++
	if err := g.Close(); err != nil {
		result.Violations = append(result.Violations, "first Close failed: "+err.Error())
	}
	result.Metrics.SyncWork++
	if err := g.Close(); err != nil {
		result.Violations = append(result.Violations, "second Close was not idempotent")
	}
	result.Metrics.SyncWork++
	if err := g.Enqueue(context.Background(), contract.Job{ID: 0, Payload: []byte{1}}); !errors.Is(err, contract.ErrClosed) {
		result.Violations = append(result.Violations, "Enqueue after Close did not return ErrClosed")
	}
	result.Metrics.SyncWork++
	if _, err := g.Lease(context.Background(), tick, 1); !errors.Is(err, contract.ErrClosed) {
		result.Violations = append(result.Violations, "Lease after Close did not return ErrClosed")
	}
	result.Metrics.SyncWork++
	if err := g.Ack(context.Background(), 1); !errors.Is(err, contract.ErrClosed) {
		result.Violations = append(result.Violations, "Ack after Close did not return ErrClosed")
	}

	result.Metrics.CompletedJobs = m.completed
	result.Metrics.LogicalQueueWait = percentile(m.waits, 0.99)
	result.Metrics.PeakBytes = m.peak
	result.Run.PeakMemoryBytes = m.peak
	if len(result.Violations) > 0 {
		result.Verdict = "fail"
	}
	return result
}

func (m *model) producerStep(tick uint64, q contract.Queue, result *Result) {
	probed := false
	for m.nextEnqueue < len(m.jobs) {
		job := m.jobs[m.nextEnqueue]
		size := uint64(len(job.Payload))

		if satAdd(m.resident, size) > m.capacity {
			// Backpressure: with no room the enqueue must honor cancellation
			// rather than admit the job past the capacity bound. Probe once per
			// tick and wait for consumers to free space.
			if !probed {
				probed = true
				result.Metrics.SyncWork++
				if err := q.Enqueue(canceledContext(), contract.Job{ID: job.ID, Payload: job.Payload}); !errors.Is(err, context.Canceled) {
					result.Violations = append(result.Violations, "enqueue under backpressure did not return context cancellation")
				}
			}
			return
		}

		result.Metrics.SyncWork++
		if err := q.Enqueue(context.Background(), contract.Job{ID: job.ID, Payload: job.Payload}); err != nil {
			result.Violations = append(result.Violations, fmt.Sprintf("enqueue of job %d failed: %v", job.ID, err))
			return
		}
		m.resident = satAdd(m.resident, size)
		if m.resident > m.peak {
			m.peak = m.resident
		}
		m.available = append(m.available, job.ID)
		m.enqueuedTick[job.ID] = tick
		m.nextEnqueue++
		// Mutate the caller's copy after enqueue to prove the queue copied it.
		job.Payload[0] ^= 0xff
	}
}

func (m *model) consumerStep(tick uint64, q contract.Queue, result *Result) {
	for c := 0; c < m.consumers; c++ {
		if len(m.available) == 0 {
			// Consumers waiting on a visibility expiry are simulated with a
			// deterministic canceled-context probe on the first slot only.
			if c == 0 && len(m.leased) > 0 {
				result.Metrics.SyncWork++
				if _, err := q.Lease(canceledContext(), tick, m.visibility); !errors.Is(err, context.Canceled) {
					result.Violations = append(result.Violations, "lease with canceled context did not return cancellation")
				}
			}
			continue
		}

		wantID := m.available[0]
		result.Metrics.SyncWork++
		lease, err := q.Lease(context.Background(), tick, m.visibility)
		if err != nil {
			result.Violations = append(result.Violations, fmt.Sprintf("lease failed: %v", err))
			continue
		}
		id := lease.Job.ID
		if id < 1 || id > uint64(len(m.jobs)) {
			result.Violations = append(result.Violations, fmt.Sprintf("leased unknown job id %d", id))
			continue
		}
		if id != wantID {
			result.Violations = append(result.Violations, fmt.Sprintf("FIFO violation: leased %d, want %d", id, wantID))
		}
		m.available = m.available[1:]

		wantPayload := workload.Payload(m.seed, id, len(lease.Job.Payload))
		if !bytes.Equal(lease.Job.Payload, wantPayload) {
			result.Violations = append(result.Violations, fmt.Sprintf("leased payload for job %d does not match enqueued bytes", id))
		}
		if lease.Deadline != satAdd(tick, m.visibility) {
			result.Violations = append(result.Violations, fmt.Sprintf("lease deadline mismatch for job %d", id))
		}

		m.nextAttempt[id]++
		if lease.Attempt != m.nextAttempt[id] {
			result.Violations = append(result.Violations, fmt.Sprintf("attempt mismatch for job %d: got %d want %d", id, lease.Attempt, m.nextAttempt[id]))
		}
		if m.seenTokens[lease.Token] {
			result.Violations = append(result.Violations, "duplicate lease token observed")
		}
		m.seenTokens[lease.Token] = true

		if m.scriptIdx[id] >= len(m.scripts[id]) {
			result.Violations = append(result.Violations, fmt.Sprintf("job %d delivered more times than scheduled", id))
			continue
		}
		action := m.scripts[id][m.scriptIdx[id]]
		m.scriptIdx[id]++
		switch action {
		case workload.ActionAck:
			result.Metrics.SyncWork++
			if err := q.Ack(context.Background(), lease.Token); err != nil {
				result.Violations = append(result.Violations, fmt.Sprintf("ack of job %d failed: %v", id, err))
				continue
			}
			result.Metrics.SyncWork++
			if err := q.Ack(context.Background(), lease.Token); !errors.Is(err, contract.ErrStaleToken) {
				result.Violations = append(result.Violations, "duplicate ack was not rejected")
			}
			m.resident = satSub(m.resident, uint64(len(lease.Job.Payload)))
			m.completed++
			m.waits = append(m.waits, tick-m.enqueuedTick[id])
		case workload.ActionNack:
			result.Metrics.SyncWork++
			if err := q.Nack(context.Background(), lease.Token); err != nil {
				result.Violations = append(result.Violations, fmt.Sprintf("nack of job %d failed: %v", id, err))
				continue
			}
			result.Metrics.SyncWork++
			if err := q.Nack(context.Background(), lease.Token); !errors.Is(err, contract.ErrStaleToken) {
				result.Violations = append(result.Violations, "duplicate nack was not rejected")
			}
			m.available = append(m.available, id)
		case workload.ActionExpire:
			m.leased[lease.Token] = leasedModel{id: id, deadline: lease.Deadline}
		}
	}
}

// expire mirrors the queue's visibility expiry: leases whose deadline has been
// reached return to the back of the available order in ascending deadline order
// (ties broken by enqueue order, which equals job ID here). It returns the
// invalidated tokens.
func (m *model) expire(tick uint64) []uint64 {
	if len(m.leased) == 0 {
		return nil
	}
	expired := make([]uint64, 0, len(m.leased))
	for token, lm := range m.leased {
		if lm.deadline <= tick {
			expired = append(expired, token)
		}
	}
	if len(expired) == 0 {
		return nil
	}
	sort.Slice(expired, func(i, j int) bool {
		a, b := m.leased[expired[i]], m.leased[expired[j]]
		if a.deadline != b.deadline {
			return a.deadline < b.deadline
		}
		return a.id < b.id
	})
	for _, token := range expired {
		m.available = append(m.available, m.leased[token].id)
		delete(m.leased, token)
	}
	return expired
}

func normalize(config *PublicConfig) error {
	if config.CapacityBytes == 0 {
		return errors.New("capacity must be positive")
	}
	if config.Jobs <= 0 {
		config.Jobs = DefaultPublicConfig().Jobs
	}
	if config.Consumers <= 0 {
		config.Consumers = DefaultPublicConfig().Consumers
	}
	if config.Visibility == 0 {
		config.Visibility = DefaultPublicConfig().Visibility
	}
	if config.MaxPayload <= 0 {
		config.MaxPayload = DefaultPublicConfig().MaxPayload
	}
	if uint64(config.MaxPayload) > config.CapacityBytes {
		return errors.New("max payload exceeds capacity")
	}
	return nil
}

func canceledContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

func percentile(values []uint64, p float64) uint64 {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]uint64(nil), values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	index := int(math.Ceil(float64(len(sorted))*p)) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return sorted[index]
}

func satAdd(a, b uint64) uint64 {
	if a > ^uint64(0)-b {
		return ^uint64(0)
	}
	return a + b
}

func satSub(a, b uint64) uint64 {
	if b >= a {
		return 0
	}
	return a - b
}
