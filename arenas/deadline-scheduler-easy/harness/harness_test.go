package harness

import (
	"encoding/json"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/bytearena/arenas/arenas/deadline-scheduler-easy/contract"
	"github.com/bytearena/arenas/arenas/deadline-scheduler-easy/starter"
)

func TestEvaluateIsDeterministicAndConverges(t *testing.T) {
	config := DefaultPublicConfig()
	first := Evaluate(^uint64(0), config, starter.NewScheduler)
	second := Evaluate(^uint64(0), config, starter.NewScheduler)

	if !reflect.DeepEqual(first, second) {
		t.Fatalf("same seed produced different results:\n%+v\n%+v", first, second)
	}
	if first.Verdict != "pass" || len(first.Violations) != 0 {
		t.Fatalf("unexpected verdict: %+v", first)
	}
	if first.Metrics.MeanFlowTicks == 0 {
		t.Fatal("workload did not measure mean flow time")
	}
	if first.Metrics.SchedulerWork == 0 {
		t.Fatal("workload did not charge scheduler work")
	}
	if first.Metrics.PeakStateBytes == 0 {
		t.Fatal("workload did not observe scheduler state")
	}
}

func TestResultEnvelopeContainsEveryPublicMetric(t *testing.T) {
	result := Evaluate(7, DefaultPublicConfig(), starter.NewScheduler)
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
	for _, metric := range []string{"mean_flow_ticks", "p95_flow_ticks", "starvation_penalty", "scheduler_work", "peak_state_bytes"} {
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

// recordingScheduler observes start and completion ticks by wrapping another
// scheduler. It uses only the public Scheduler surface, so behavior tests assert
// externally visible execution rather than harness internals.
type recordingScheduler struct {
	inner       contract.Scheduler
	starts      map[uint64]uint64
	completions map[uint64]uint64
	callTicks   []uint64
}

func (r *recordingScheduler) OnArrival(job contract.Job, now uint64) []contract.Action {
	r.callTicks = append(r.callTicks, now)
	return r.inner.OnArrival(job, now)
}

func (r *recordingScheduler) OnProgress(progress contract.Progress, now uint64) []contract.Action {
	r.callTicks = append(r.callTicks, now)
	if progress.JobID != 0 {
		if _, seen := r.starts[progress.JobID]; !seen {
			r.starts[progress.JobID] = now
		}
	}
	return r.inner.OnProgress(progress, now)
}

func (r *recordingScheduler) OnComplete(completion contract.Completion, now uint64) []contract.Action {
	r.callTicks = append(r.callTicks, now)
	r.completions[completion.JobID] = now
	return r.inner.OnComplete(completion, now)
}

func (r *recordingScheduler) StateBytes() uint64 { return r.inner.StateBytes() }

func TestShortBeforeLong(t *testing.T) {
	jobs := []contract.Job{
		{ID: 1, Arrival: 0, EstimatedWork: 100},
		{ID: 2, Arrival: 0, EstimatedWork: 1},
		{ID: 3, Arrival: 0, EstimatedWork: 1},
		{ID: 4, Arrival: 0, EstimatedWork: 1},
	}
	obs := runWithObserver(t, jobs, newSJF)

	if obs.starts[1] <= obs.starts[2] || obs.starts[1] <= obs.starts[3] || obs.starts[1] <= obs.starts[4] {
		t.Fatalf("long job started before a short job: starts=%v", obs.starts)
	}
	for _, shortID := range []uint64{2, 3, 4} {
		if _, ok := obs.completions[shortID]; !ok {
			t.Fatalf("short job %d did not complete: %v", shortID, obs.completions)
		}
	}
}

func TestArrivalBoundary(t *testing.T) {
	jobs := []contract.Job{
		{ID: 1, Arrival: 0, EstimatedWork: 5},
		{ID: 2, Arrival: 1, EstimatedWork: 1},
	}
	obs := runWithObserver(t, jobs, func(contract.Cluster) contract.Scheduler {
		return starter.NewScheduler(contract.Cluster{Workers: 1})
	})

	if obs.starts[1] != 0 || obs.completions[1] != 5 {
		t.Fatalf("running job was preempted: starts=%v completions=%v", obs.starts, obs.completions)
	}
	if obs.starts[2] != 5 {
		t.Fatalf("arriving job started before the running job completed: starts=%v", obs.starts)
	}
}

func TestTiesResolveByJobID(t *testing.T) {
	jobs := []contract.Job{
		{ID: 1, Arrival: 0, EstimatedWork: 1},
		{ID: 2, Arrival: 0, EstimatedWork: 1},
		{ID: 3, Arrival: 0, EstimatedWork: 1},
	}
	obs := runWithObserver(t, jobs, func(contract.Cluster) contract.Scheduler {
		return starter.NewScheduler(contract.Cluster{Workers: 1})
	})

	if obs.starts[1] > obs.starts[2] || obs.starts[2] > obs.starts[3] {
		t.Fatalf("equal work and arrival did not resolve by ascending ID: starts=%v", obs.starts)
	}
}

func TestAgingStartsLongJobBeforeBound(t *testing.T) {
	jobs := []contract.Job{
		{ID: 1, Arrival: 0, EstimatedWork: 100},
		{ID: 2, Arrival: 0, EstimatedWork: 1},
	}
	for id := uint64(3); id <= 120; id++ {
		jobs = append(jobs, contract.Job{ID: id, Arrival: id - 2, EstimatedWork: 1})
	}

	obs := runWithObserver(t, jobs, newAging)

	start := obs.starts[1]
	if start > contract.MaxWaitTicks {
		t.Fatalf("long job started at tick %d, want at or before %d", start, contract.MaxWaitTicks)
	}
	if _, ok := obs.completions[1]; !ok {
		t.Fatal("long job never completed")
	}
}

func TestIdleGapProducesNoContestantCalls(t *testing.T) {
	jobs := []contract.Job{
		{ID: 1, Arrival: 0, EstimatedWork: 1},
		{ID: 2, Arrival: 1000, EstimatedWork: 1},
	}
	obs := runWithObserver(t, jobs, func(contract.Cluster) contract.Scheduler {
		return starter.NewScheduler(contract.Cluster{Workers: 1})
	})

	for _, tick := range obs.callTicks {
		if tick > 1 && tick < 1000 {
			t.Fatalf("scheduler was called at idle tick %d", tick)
		}
	}
	if obs.starts[2] != 1000 {
		t.Fatalf("second job start = %d, want 1000", obs.starts[2])
	}
}

func runWithObserver(t *testing.T, jobs []contract.Job, policy func(contract.Cluster) contract.Scheduler) *recordingScheduler {
	t.Helper()
	var obs *recordingScheduler
	factory := func(cluster contract.Cluster) contract.Scheduler {
		obs = &recordingScheduler{
			inner:       policy(cluster),
			starts:      make(map[uint64]uint64),
			completions: make(map[uint64]uint64),
		}
		return obs
	}
	result := RunJobs(9, jobs, factory)
	if result.Verdict != "pass" {
		t.Fatalf("verdict = %q, want pass; violations: %v", result.Verdict, result.Violations)
	}
	return obs
}

// sjfScheduler is a test-only policy: shortest estimated work first, ties by
// ascending job ID. It dispatches only from the idle dispatch opportunity so it
// can compare every job that arrived in the same tick.
type sjfScheduler struct {
	waiting []contract.Job
	running uint64
}

func newSJF(contract.Cluster) contract.Scheduler {
	return &sjfScheduler{}
}

func (s *sjfScheduler) OnArrival(job contract.Job, _ uint64) []contract.Action {
	s.waiting = append(s.waiting, job)
	return nil
}

func (s *sjfScheduler) OnProgress(progress contract.Progress, now uint64) []contract.Action {
	if progress.JobID == 0 {
		return s.dispatch()
	}
	s.running = progress.JobID
	return nil
}

func (s *sjfScheduler) OnComplete(_ contract.Completion, _ uint64) []contract.Action {
	s.running = 0
	return nil
}

func (s *sjfScheduler) StateBytes() uint64 {
	return contract.PerJobStateBytes * uint64(len(s.waiting))
}

func (s *sjfScheduler) dispatch() []contract.Action {
	if s.running != 0 || len(s.waiting) == 0 {
		return nil
	}
	sort.Slice(s.waiting, func(i, j int) bool {
		if s.waiting[i].EstimatedWork != s.waiting[j].EstimatedWork {
			return s.waiting[i].EstimatedWork < s.waiting[j].EstimatedWork
		}
		return s.waiting[i].ID < s.waiting[j].ID
	})
	job := s.waiting[0]
	s.waiting = s.waiting[1:]
	s.running = job.ID
	return []contract.Action{{Kind: contract.ActionRun, JobID: job.ID}}
}

// agingScheduler is a test-only policy: shortest job first, except that any job
// that has waited at least contract.MaxWaitTicks is selected first (earliest
// arrival, ties by ID).
type agingScheduler struct {
	waiting  []contract.Job
	arrivals map[uint64]uint64
	running  uint64
}

func newAging(contract.Cluster) contract.Scheduler {
	return &agingScheduler{arrivals: make(map[uint64]uint64)}
}

func (s *agingScheduler) OnArrival(job contract.Job, now uint64) []contract.Action {
	s.arrivals[job.ID] = now
	s.waiting = append(s.waiting, job)
	return nil
}

func (s *agingScheduler) OnProgress(progress contract.Progress, now uint64) []contract.Action {
	if progress.JobID == 0 {
		return s.dispatch(now)
	}
	s.running = progress.JobID
	return nil
}

func (s *agingScheduler) OnComplete(_ contract.Completion, _ uint64) []contract.Action {
	s.running = 0
	return nil
}

func (s *agingScheduler) StateBytes() uint64 {
	return contract.PerJobStateBytes * uint64(len(s.waiting))
}

func (s *agingScheduler) dispatch(now uint64) []contract.Action {
	if s.running != 0 || len(s.waiting) == 0 {
		return nil
	}
	sort.Slice(s.waiting, func(i, j int) bool {
		a, b := s.waiting[i], s.waiting[j]
		aOverdue := now-s.arrivals[a.ID] >= contract.MaxWaitTicks
		bOverdue := now-s.arrivals[b.ID] >= contract.MaxWaitTicks
		if aOverdue != bOverdue {
			return aOverdue
		}
		if aOverdue {
			if a.Arrival != b.Arrival {
				return a.Arrival < b.Arrival
			}
			return a.ID < b.ID
		}
		if a.EstimatedWork != b.EstimatedWork {
			return a.EstimatedWork < b.EstimatedWork
		}
		if a.Arrival != b.Arrival {
			return a.Arrival < b.Arrival
		}
		return a.ID < b.ID
	})
	job := s.waiting[0]
	s.waiting = s.waiting[1:]
	s.running = job.ID
	return []contract.Action{{Kind: contract.ActionRun, JobID: job.ID}}
}

func TestEvaluateGatesFailures(t *testing.T) {
	tests := []struct {
		name    string
		jobs    []contract.Job
		factory contract.Factory
		want    string
	}{
		{
			name: "zero work job",
			jobs: []contract.Job{{ID: 1, Arrival: 0, EstimatedWork: 0}},
			factory: func(contract.Cluster) contract.Scheduler {
				return starter.NewScheduler(contract.Cluster{Workers: 1})
			},
			want: "zero estimated work",
		},
		{
			name: "duplicate IDs",
			jobs: []contract.Job{
				{ID: 1, Arrival: 0, EstimatedWork: 1},
				{ID: 1, Arrival: 1, EstimatedWork: 1},
			},
			factory: func(contract.Cluster) contract.Scheduler {
				return starter.NewScheduler(contract.Cluster{Workers: 1})
			},
			want: "duplicate job ID 1",
		},
		{
			name: "run before arrival",
			jobs: []contract.Job{
				{ID: 1, Arrival: 0, EstimatedWork: 1},
				{ID: 2, Arrival: 10, EstimatedWork: 1},
			},
			factory: func(contract.Cluster) contract.Scheduler { return eagerScheduler{} },
			want:    "before its arrival",
		},
		{
			name: "estimate addition overflow",
			jobs: []contract.Job{{ID: 1, Arrival: 10, EstimatedWork: ^uint64(0)}},
			factory: func(contract.Cluster) contract.Scheduler {
				return overflowScheduler{}
			},
			want: "runtime addition overflow",
		},
		{
			name: "preempt while busy",
			jobs: []contract.Job{
				{ID: 1, Arrival: 0, EstimatedWork: 5},
				{ID: 2, Arrival: 1, EstimatedWork: 1},
			},
			factory: func(contract.Cluster) contract.Scheduler { return &preemptingScheduler{} },
			want:    "while job 1 is running",
		},
		{
			name: "scheduler panics",
			jobs: []contract.Job{{ID: 1, Arrival: 0, EstimatedWork: 1}},
			factory: func(contract.Cluster) contract.Scheduler {
				return panickingScheduler{}
			},
			want: "scheduler panicked",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := RunJobs(1, test.jobs, test.factory)
			if result.Verdict != "fail" {
				t.Fatalf("verdict = %q, want fail; violations: %v", result.Verdict, result.Violations)
			}
			if result.Score != 0 {
				t.Fatalf("failed run reported a non-zero score: %d", result.Score)
			}
			if !containsViolation(result.Violations, test.want) {
				t.Fatalf("violations = %v, want one containing %q", result.Violations, test.want)
			}
		})
	}
}

func TestValidateJobsCapsViolations(t *testing.T) {
	jobs := make([]contract.Job, 0, maxViolations*2)
	for id := uint64(1); id <= uint64(maxViolations*2); id++ {
		jobs = append(jobs, contract.Job{ID: id, Arrival: 0, EstimatedWork: 0})
	}
	violations := validateJobs(jobs)
	if len(violations) != maxViolations {
		t.Fatalf("validateJobs returned %d violations, want %d", len(violations), maxViolations)
	}
}

func TestEvaluateRejectsNilScheduler(t *testing.T) {
	factory := func(contract.Cluster) contract.Scheduler { return nil }
	result := RunJobs(1, []contract.Job{{ID: 1, Arrival: 0, EstimatedWork: 1}}, factory)
	if result.Verdict != "fail" {
		t.Fatalf("verdict = %q, want fail; violations: %v", result.Verdict, result.Violations)
	}
	if !containsViolation(result.Violations, "NewScheduler returned a nil scheduler") {
		t.Fatalf("violations = %v, want the nil-scheduler violation", result.Violations)
	}
}

type eagerScheduler struct{}

func (eagerScheduler) OnArrival(job contract.Job, _ uint64) []contract.Action {
	if job.ID == 1 {
		return []contract.Action{{Kind: contract.ActionRun, JobID: 2}}
	}
	return nil
}
func (eagerScheduler) OnProgress(contract.Progress, uint64) []contract.Action { return nil }
func (eagerScheduler) OnComplete(contract.Completion, uint64) []contract.Action {
	return nil
}
func (eagerScheduler) StateBytes() uint64 { return 0 }

type overflowScheduler struct{}

func (overflowScheduler) OnArrival(job contract.Job, _ uint64) []contract.Action {
	return []contract.Action{{Kind: contract.ActionRun, JobID: job.ID}}
}
func (overflowScheduler) OnProgress(contract.Progress, uint64) []contract.Action { return nil }
func (overflowScheduler) OnComplete(contract.Completion, uint64) []contract.Action {
	return nil
}
func (overflowScheduler) StateBytes() uint64 { return 0 }

type preemptingScheduler struct {
	running uint64
}

func (p *preemptingScheduler) OnArrival(job contract.Job, _ uint64) []contract.Action {
	switch job.ID {
	case 1:
		p.running = 1
		return []contract.Action{{Kind: contract.ActionRun, JobID: 1}}
	case 2:
		return []contract.Action{{Kind: contract.ActionRun, JobID: 2}}
	default:
		return nil
	}
}
func (p *preemptingScheduler) OnProgress(progress contract.Progress, _ uint64) []contract.Action {
	if progress.JobID != 0 {
		p.running = progress.JobID
	}
	return nil
}
func (p *preemptingScheduler) OnComplete(_ contract.Completion, _ uint64) []contract.Action {
	p.running = 0
	return nil
}
func (p *preemptingScheduler) StateBytes() uint64 { return 0 }

type panickingScheduler struct{}

func (panickingScheduler) OnArrival(contract.Job, uint64) []contract.Action { panic("boom") }
func (panickingScheduler) OnProgress(contract.Progress, uint64) []contract.Action {
	return nil
}
func (panickingScheduler) OnComplete(contract.Completion, uint64) []contract.Action { return nil }
func (panickingScheduler) StateBytes() uint64                                       { return 0 }

// startThenPanicScheduler starts a job and then panics mid-flight, leaving the
// started job never completed. It is a hostile fake that proves finishMetrics
// does not invent completion metrics for a job that only started.
type startThenPanicScheduler struct{}

func (startThenPanicScheduler) OnArrival(job contract.Job, _ uint64) []contract.Action {
	return []contract.Action{{Kind: contract.ActionRun, JobID: job.ID}}
}
func (startThenPanicScheduler) OnProgress(contract.Progress, uint64) []contract.Action {
	panic("mid-flight")
}
func (startThenPanicScheduler) OnComplete(contract.Completion, uint64) []contract.Action {
	return nil
}
func (startThenPanicScheduler) StateBytes() uint64 { return 0 }

func TestFailedRunReportsNoCompletionMetricsForStartedButNeverCompletedJob(t *testing.T) {
	jobs := []contract.Job{
		{ID: 1, Arrival: 0, EstimatedWork: 2},
		{ID: 2, Arrival: 5, EstimatedWork: 1},
	}
	result := RunJobs(1, jobs, func(contract.Cluster) contract.Scheduler {
		return startThenPanicScheduler{}
	})

	if result.Verdict != "fail" {
		t.Fatalf("verdict = %q, want fail; violations: %v", result.Verdict, result.Violations)
	}
	if !containsViolation(result.Violations, "scheduler panicked") {
		t.Fatalf("violations = %v, want one containing %q", result.Violations, "scheduler panicked")
	}
	if result.Metrics.MeanFlowTicks != 0 {
		t.Fatalf("mean_flow_ticks = %d, want 0 for a never-completed job", result.Metrics.MeanFlowTicks)
	}
	if result.Metrics.P95FlowTicks != 0 {
		t.Fatalf("p95_flow_ticks = %d, want 0 for a never-completed job", result.Metrics.P95FlowTicks)
	}
	if result.Metrics.StarvationPenalty != 0 {
		t.Fatalf("starvation_penalty = %d, want 0 for a never-completed job", result.Metrics.StarvationPenalty)
	}
}

func containsViolation(violations []string, want string) bool {
	for _, violation := range violations {
		if strings.Contains(violation, want) {
			return true
		}
	}
	return false
}
