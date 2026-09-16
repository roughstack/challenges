// Package contract defines the narrow contestant-owned surface for the
// deadline-scheduler-easy arena. The deterministic virtual machine, event
// loop, action validation, accounting, and result construction are all owned
// by the trusted harness.
package contract

// MaxWaitTicks is the published fairness bound for this arena. Every tick a job
// waits beyond this bound is added to the scored starvation_penalty metric.
// Schedules that delay a job past this bound remain correct but are penalized;
// no gate enforces a particular selection order.
const MaxWaitTicks uint64 = 96

// PerJobStateBytes is the fixed logical memory charge for one job tracked by a
// scheduler's StateBytes report. It keeps the memory metric deterministic and
// independent of host allocator behavior.
const PerJobStateBytes uint64 = 64

// Job is a unit of work. The harness owns arrival and completion events and
// executes the schedule on a deterministic virtual machine.
//
// Easy uses only ID, Arrival, and EstimatedWork. Runtime estimates are exact,
// so a job started at tick s completes at tick s + EstimatedWork. The remaining
// fields are reserved for higher tiers and are always zero in public workloads.
type Job struct {
	ID            uint64
	Tenant        uint64
	Arrival       uint64
	EstimatedWork uint64
	Deadline      uint64
	Priority      uint8
	CPU           uint32
	Memory        uint32
	Preemptible   bool
}

// Cluster describes the fixed execution environment. Easy always has exactly
// one worker and no preemption.
type Cluster struct {
	Workers int
}

// ActionKind identifies a scheduler-requested action.
type ActionKind uint8

const (
	// ActionRun requests that the harness start a waiting job on the single
	// worker. It is the only action in the easy tier.
	ActionRun ActionKind = iota + 1
)

// Action is one validated scheduler request. The harness rejects actions that
// would run a job before arrival, double-place a job, preempt the running job,
// run a completed job, or overflow virtual-clock arithmetic.
type Action struct {
	Kind  ActionKind
	JobID uint64
}

// Progress is a deterministic execution event delivered to the scheduler.
//
// When JobID is non-zero the worker is busy and RemainingWork is the exact
// remaining runtime for that job. When JobID is zero the worker is idle and
// the harness has finished delivering every arrival and completion for the
// current tick; the scheduler may use this dispatch opportunity to compare the
// full batch and return an ActionRun.
type Progress struct {
	JobID         uint64
	RemainingWork uint64
}

// Completion is a deterministic execution event delivered when a job finishes.
type Completion struct {
	JobID uint64
}

// Scheduler is the complete contestant-owned surface for this arena.
//
//   - OnArrival is called once per job at its arrival tick. The scheduler may
//     return an ActionRun when the worker is idle.
//   - OnProgress is called once per running tick with the running job's
//     remaining work. It is also called once with a zero Progress when the
//     worker is idle at the end of a tick, after all same-tick arrivals and
//     completions have been delivered, so the scheduler can dispatch with the
//     full batch visible.
//   - OnComplete is called when the running job finishes. The scheduler may
//     return an ActionRun for the next waiting job.
//   - StateBytes reports the scheduler's current logical state size for the
//     deterministic memory metric.
//
// The harness may deliver several OnArrival calls for the same tick before it
// applies any returned actions. A scheduler that returns an ActionRun should
// record that job as running immediately; only valid actions are ever started
// by the harness.
//
// A correct scheduler never lets a waiting job starve forever: the harness
// fails the run if jobs remain waiting after the event stream has ended.
type Scheduler interface {
	OnArrival(job Job, nowTick uint64) []Action
	OnProgress(progress Progress, nowTick uint64) []Action
	OnComplete(completion Completion, nowTick uint64) []Action
	StateBytes() uint64
}

// Factory constructs a fresh scheduler for one isolated workload run.
type Factory func(cluster Cluster) Scheduler
