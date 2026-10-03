// Package starter contains a deliberately simple, correct FIFO scheduler.
//
// It is intentionally non-optimal: a long job that arrives before a burst of
// short jobs is dispatched first, which maximizes mean flow time on exactly the
// workload shape this arena rewards. The scheduler is still correct — every
// waiting job eventually runs, no action is emitted while the worker is busy,
// and equal arrivals resolve in ascending job ID order because the harness
// delivers arrivals in that order.
package starter

import "github.com/roughstack/challenges/challenges/deadline-scheduler-easy/contract"

type scheduler struct {
	waiting   []contract.Job
	running   uint64
	completed map[uint64]bool
}

// NewScheduler returns a FIFO scheduler for one deterministic workload run.
func NewScheduler(contract.Cluster) contract.Scheduler {
	return &scheduler{completed: make(map[uint64]bool)}
}

func (s *scheduler) OnArrival(job contract.Job, _ uint64) []contract.Action {
	s.waiting = append(s.waiting, job)
	if s.running == 0 {
		return s.dispatch()
	}
	return nil
}

func (s *scheduler) OnProgress(progress contract.Progress, _ uint64) []contract.Action {
	if progress.JobID != 0 {
		s.running = progress.JobID
	}
	return nil
}

func (s *scheduler) OnComplete(completion contract.Completion, _ uint64) []contract.Action {
	s.completed[completion.JobID] = true
	s.running = 0
	return s.dispatch()
}

func (s *scheduler) StateBytes() uint64 {
	running := uint64(0)
	if s.running != 0 {
		running = 1
	}
	return contract.PerJobStateBytes * (uint64(len(s.waiting)) + running)
}

// dispatch marks the head of the waiting queue as running and asks the harness
// to start it. The scheduler optimistically records the running job; a correct
// FIFO scheduler never emits this action while the worker is busy.
func (s *scheduler) dispatch() []contract.Action {
	if s.running != 0 || len(s.waiting) == 0 {
		return nil
	}
	job := s.waiting[0]
	s.waiting = s.waiting[1:]
	s.running = job.ID
	return []contract.Action{{Kind: contract.ActionRun, JobID: job.ID}}
}
