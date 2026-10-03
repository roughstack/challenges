// Package harness owns public execution, the deterministic virtual clock, action
// validation, accounting, and result construction for the
// deadline-scheduler-easy arena. The contestant surface is contract.Scheduler
// only; the harness never preempts a running job and never invents progress or
// completion.
package harness

import (
	"fmt"
	"math"
	"sort"
	"strconv"

	"github.com/roughstack/challenges/challenges/deadline-scheduler-easy/contract"
	"github.com/roughstack/challenges/challenges/deadline-scheduler-easy/workload"
)

const (
	// ArenaID is the stable public identifier for this variant.
	ArenaID = "deadline-scheduler-easy"
	// ArenaVersion is the immutable public version for this variant.
	ArenaVersion = "1.0.0"
	// WorkloadID identifies the deterministic public smoke workload.
	WorkloadID = "public-smoke-v1"
	// FixtureWorkloadID identifies explicit job fixtures used by public tests.
	FixtureWorkloadID = "public-fixture-v1"

	// maxWorkBudget bounds the number of simulated running/event ticks. It is
	// generous for public workloads but keeps a pathological contestant from
	// driving the virtual machine forever.
	maxWorkBudget uint64 = 10_000_000

	// maxViolations bounds how many violations are recorded before the run
	// stops early and fails closed.
	maxViolations = 16
)

// Metrics are deterministic logical measurements from one workload run.
type Metrics struct {
	MeanFlowTicks     uint64 `json:"mean_flow_ticks"`
	P95FlowTicks      uint64 `json:"p95_flow_ticks"`
	StarvationPenalty uint64 `json:"starvation_penalty"`
	SchedulerWork     uint64 `json:"scheduler_work"`
	PeakStateBytes    uint64 `json:"peak_state_bytes"`
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
	Jobs       int
	MaxWork    uint64
	MaxGap     uint64
	BurstEvery int
	LongEvery  int
}

// DefaultPublicConfig returns the fast public smoke workload.
func DefaultPublicConfig() PublicConfig {
	return PublicConfig{
		Jobs:       1200,
		MaxWork:    64,
		MaxGap:     8,
		BurstEvery: 4,
		LongEvery:  53,
	}
}

// Evaluate runs the deterministic public smoke workload against a contestant
// scheduler.
func Evaluate(seed uint64, config PublicConfig, factory contract.Factory) Result {
	jobs := workload.Generate(workload.Config{
		Seed:       seed,
		Jobs:       config.Jobs,
		MaxWork:    config.MaxWork,
		MaxGap:     config.MaxGap,
		BurstEvery: config.BurstEvery,
		LongEvery:  config.LongEvery,
	})
	contractJobs := make([]contract.Job, len(jobs))
	for index, job := range jobs {
		contractJobs[index] = contract.Job{
			ID:            job.ID,
			Arrival:       job.Arrival,
			EstimatedWork: job.EstimatedWork,
		}
	}
	result := RunJobs(seed, contractJobs, factory)
	result.Run.WorkloadID = WorkloadID
	return result
}

// RunJobs executes an explicit deterministic job set. It is exported so public
// tests can assert scheduling behavior on named fixtures without mirroring a
// ranked workload distribution.
func RunJobs(seed uint64, jobs []contract.Job, factory contract.Factory) Result {
	result := Result{
		ProtocolVersion: 1,
		ArenaID:         ArenaID,
		ArenaVersion:    ArenaVersion,
		Seed:            strconv.FormatUint(seed, 10),
		Verdict:         "pass",
		Score:           0,
		Violations:      []string{},
		Run: RunInfo{
			WorkloadID: FixtureWorkloadID,
		},
	}

	if violations := validateJobs(jobs); len(violations) > 0 {
		result.Violations = violations
		result.Verdict = "fail"
		return result
	}

	scheduler := newScheduler(factory)
	if scheduler == nil {
		result.Violations = []string{"NewScheduler returned a nil scheduler"}
		result.Verdict = "fail"
		return result
	}

	h := newHarness(seed, jobs, scheduler)
	h.run()
	h.finishMetrics()

	result.Metrics = h.metrics
	result.Run.PeakMemoryBytes = h.metrics.PeakStateBytes
	if len(h.violations) > 0 {
		result.Violations = h.violations
		result.Verdict = "fail"
	}
	return result
}

func validateJobs(jobs []contract.Job) []string {
	var violations []string
	seen := make(map[uint64]bool, len(jobs))
	for _, job := range jobs {
		if len(violations) >= maxViolations {
			break
		}
		if job.ID == 0 {
			violations = append(violations, "job has zero ID")
			continue
		}
		if seen[job.ID] {
			violations = append(violations, fmt.Sprintf("duplicate job ID %d", job.ID))
			continue
		}
		seen[job.ID] = true
		if job.EstimatedWork == 0 {
			violations = append(violations, fmt.Sprintf("job %d has zero estimated work", job.ID))
		}
	}
	return violations
}

type runningJob struct {
	id         uint64
	remaining  uint64
	finishTick uint64
}

type harness struct {
	seed      uint64
	jobs      []contract.Job
	scheduler contract.Scheduler

	tick        uint64
	nextArrival int
	running     *runningJob
	arrived     map[uint64]bool
	started     map[uint64]uint64 // job ID -> start tick
	completed   map[uint64]bool
	jobByID     map[uint64]contract.Job

	metrics    Metrics
	violations []string
	panicked   bool
}

func newHarness(seed uint64, jobs []contract.Job, scheduler contract.Scheduler) *harness {
	sorted := append([]contract.Job(nil), jobs...)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].Arrival != sorted[j].Arrival {
			return sorted[i].Arrival < sorted[j].Arrival
		}
		return sorted[i].ID < sorted[j].ID
	})

	h := &harness{
		seed:      seed,
		jobs:      sorted,
		scheduler: scheduler,
		arrived:   make(map[uint64]bool, len(jobs)),
		started:   make(map[uint64]uint64, len(jobs)),
		completed: make(map[uint64]bool, len(jobs)),
		jobByID:   make(map[uint64]contract.Job, len(jobs)),
	}
	for _, job := range sorted {
		h.jobByID[job.ID] = job
	}
	return h
}

// newScheduler invokes the contestant factory and converts a panic into a nil
// scheduler.
func newScheduler(factory contract.Factory) (scheduler contract.Scheduler) {
	defer func() {
		if recover() != nil {
			scheduler = nil
		}
	}()
	scheduler = factory(contract.Cluster{Workers: 1})
	return scheduler
}

func (h *harness) run() {
	steps := uint64(0)
	budget := h.workBudget()

	for {
		if h.panicked {
			break
		}
		if steps > budget {
			h.violate("logical work budget exceeded")
			break
		}
		if h.running == nil && h.nextArrival == len(h.jobs) {
			if len(h.completed) == len(h.jobs) {
				break
			}
			h.violate("scheduler stalled with waiting jobs")
			break
		}

		if h.running == nil {
			nextArrival := h.jobs[h.nextArrival].Arrival
			if nextArrival > h.tick {
				// Long idle intervals advance the clock without any contestant
				// call, so there is no busy loop and no invented work.
				h.tick = nextArrival
			}
		}

		h.processTick(h.tick)
		h.sampleState()
		if h.tick != ^uint64(0) {
			h.tick++
		}
		steps++
		if len(h.violations) >= maxViolations {
			break
		}
	}

	if !h.panicked && len(h.completed) != len(h.jobs) && len(h.violations) == 0 {
		h.violate("workload did not converge")
	}
}

// workBudget returns the deterministic logical tick budget for this job set.
// A saturated sum is reported as a violation before execution begins.
func (h *harness) workBudget() uint64 {
	total := uint64(0)
	for _, job := range h.jobs {
		if job.EstimatedWork > ^uint64(0)-total {
			h.violate("estimated work addition overflow")
			return maxWorkBudget
		}
		total += job.EstimatedWork
	}
	budget := satAdd(satAdd(total, uint64(len(h.jobs))), uint64(len(h.jobs)))
	if budget > maxWorkBudget {
		budget = maxWorkBudget
	}
	return budget
}

func (h *harness) processTick(now uint64) {
	var actions []contract.Action

	// Completion events happen before same-tick arrivals, so a job arriving at
	// the completion tick can be dispatched by OnComplete or OnArrival.
	if h.running != nil && h.running.finishTick == now {
		if !h.callComplete(now, &actions) {
			return
		}
		h.completed[h.running.id] = true
		h.running = nil
	}

	for h.nextArrival < len(h.jobs) && h.jobs[h.nextArrival].Arrival == now {
		job := h.jobs[h.nextArrival]
		h.arrived[job.ID] = true
		if !h.callArrival(job, now, &actions) {
			return
		}
		h.nextArrival++
	}

	h.applyActions(actions, now)

	if h.running == nil {
		// Idle dispatch opportunity after all same-tick arrivals and
		// completions are known. The scheduler can compare the full batch
		// before choosing a job.
		var idleActions []contract.Action
		if !h.callIdle(now, &idleActions) {
			return
		}
		h.applyActions(idleActions, now)
	}

	if h.running != nil {
		var progressActions []contract.Action
		if !h.callProgress(h.running.id, h.running.remaining, now, &progressActions) {
			return
		}
		// The worker is busy for the entire progress event, so any run action
		// returned here is a preemption or double-placement request.
		h.applyActions(progressActions, now)
		if h.running.remaining > 0 {
			h.running.remaining--
		}
	}
}

// applyActions validates every returned action and starts at most one job. The
// first valid run action while the worker is idle starts a job; later actions
// are validated against the now-busy worker, so preemption and double-placement
// requests fail closed.
func (h *harness) applyActions(actions []contract.Action, now uint64) {
	started := false
	for _, action := range actions {
		h.metrics.SchedulerWork++
		runningID := uint64(0)
		if h.running != nil {
			runningID = h.running.id
		}
		if !h.validateAction(action, now, runningID) {
			continue
		}
		if !started && h.running == nil {
			if h.startJob(action.JobID, now) {
				started = true
			}
		}
	}
}

func (h *harness) validateAction(action contract.Action, now uint64, runningID uint64) bool {
	if action.Kind != contract.ActionRun {
		h.violate("scheduler returned an unknown action kind")
		return false
	}
	job, ok := h.jobByID[action.JobID]
	if !ok {
		h.violate(fmt.Sprintf("action references unknown job %d", action.JobID))
		return false
	}
	if !h.arrived[action.JobID] {
		h.violate(fmt.Sprintf("action runs job %d before its arrival", action.JobID))
		return false
	}
	if h.completed[action.JobID] {
		h.violate(fmt.Sprintf("action runs completed job %d", action.JobID))
		return false
	}
	if runningID != 0 {
		if action.JobID == runningID {
			h.violate(fmt.Sprintf("action double-places running job %d", action.JobID))
		} else {
			h.violate(fmt.Sprintf("action runs job %d while job %d is running", action.JobID, runningID))
		}
		return false
	}
	if job.EstimatedWork > ^uint64(0)-now {
		h.violate(fmt.Sprintf("job %d runtime addition overflow at tick %d", action.JobID, now))
		return false
	}
	return true
}

func (h *harness) startJob(id, now uint64) bool {
	job := h.jobByID[id]
	h.running = &runningJob{
		id:         id,
		remaining:  job.EstimatedWork,
		finishTick: satAdd(now, job.EstimatedWork),
	}
	h.started[id] = now
	return true
}

func (h *harness) callArrival(job contract.Job, now uint64, actions *[]contract.Action) bool {
	var out []contract.Action
	if !h.call(func() { out = h.scheduler.OnArrival(job, now) }) {
		return false
	}
	h.metrics.SchedulerWork++
	*actions = append(*actions, out...)
	return true
}

func (h *harness) callProgress(id, remaining, now uint64, actions *[]contract.Action) bool {
	var out []contract.Action
	if !h.call(func() {
		out = h.scheduler.OnProgress(contract.Progress{JobID: id, RemainingWork: remaining}, now)
	}) {
		return false
	}
	h.metrics.SchedulerWork++
	*actions = append(*actions, out...)
	return true
}

func (h *harness) callComplete(now uint64, actions *[]contract.Action) bool {
	var out []contract.Action
	if !h.call(func() {
		out = h.scheduler.OnComplete(contract.Completion{JobID: h.running.id}, now)
	}) {
		return false
	}
	h.metrics.SchedulerWork++
	*actions = append(*actions, out...)
	return true
}

// callIdle delivers the idle dispatch opportunity. A JobID of zero is the
// documented sentinel: the worker is idle and the scheduler may choose among
// every job that has arrived up to nowTick.
func (h *harness) callIdle(now uint64, actions *[]contract.Action) bool {
	var out []contract.Action
	if !h.call(func() {
		out = h.scheduler.OnProgress(contract.Progress{}, now)
	}) {
		return false
	}
	h.metrics.SchedulerWork++
	*actions = append(*actions, out...)
	return true
}

// sampleState records the scheduler's deterministic logical state size.
func (h *harness) sampleState() {
	var state uint64
	if !h.call(func() { state = h.scheduler.StateBytes() }) {
		return
	}
	if state > h.metrics.PeakStateBytes {
		h.metrics.PeakStateBytes = state
	}
}

// call runs one contestant scheduler method and converts a panic into a
// deterministic violation instead of crashing the harness process.
func (h *harness) call(fn func()) (ok bool) {
	defer func() {
		if recover() != nil {
			h.panicked = true
			h.violate("scheduler panicked")
			ok = false
		}
	}()
	fn()
	return true
}

func (h *harness) violate(message string) {
	if len(h.violations) < maxViolations {
		h.violations = append(h.violations, message)
	}
}

func (h *harness) finishMetrics() {
	flows := make([]uint64, 0, len(h.jobs))
	var totalFlow uint64
	for _, job := range h.jobs {
		if !h.completed[job.ID] {
			continue
		}
		start, ok := h.started[job.ID]
		if !ok {
			continue
		}
		flow := satSub(satAdd(start, job.EstimatedWork), job.Arrival)
		flows = append(flows, flow)
		totalFlow = satAdd(totalFlow, flow)

		wait := satSub(start, job.Arrival)
		if wait > contract.MaxWaitTicks {
			h.metrics.StarvationPenalty = satAdd(h.metrics.StarvationPenalty, wait-contract.MaxWaitTicks)
		}
	}
	if len(flows) > 0 {
		h.metrics.MeanFlowTicks = totalFlow / uint64(len(flows))
		h.metrics.P95FlowTicks = percentile(flows, 0.95)
	}
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
