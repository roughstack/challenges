// Package starter is the learner-facing scaffold for a one-worker scheduler.
package starter

import "github.com/roughstack/challenges/challenges/deadline-scheduler-easy/contract"

type scheduler struct {
	// TODO: track waiting and running jobs.
}

func NewScheduler(cluster contract.Cluster) contract.Scheduler {
	return &scheduler{}
}

func (s *scheduler) OnArrival(job contract.Job, nowTick uint64) []contract.Action {
	panic("TODO: record the arrival and dispatch only when idle")
}

func (s *scheduler) OnProgress(progress contract.Progress, nowTick uint64) []contract.Action {
	panic("TODO: reflect the worker's progress")
}

func (s *scheduler) OnComplete(completion contract.Completion, nowTick uint64) []contract.Action {
	panic("TODO: complete the job and decide what runs next")
}

func (s *scheduler) StateBytes() uint64 {
	panic("TODO: account for scheduler state")
}
