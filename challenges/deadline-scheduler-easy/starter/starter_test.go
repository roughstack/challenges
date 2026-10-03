package starter

import (
	"reflect"
	"sort"
	"testing"

	"github.com/roughstack/challenges/challenges/deadline-scheduler-easy/contract"
)

func TestFIFOOrderAndTieBreakByID(t *testing.T) {
	scheduler := NewScheduler(contract.Cluster{Workers: 1})
	jobs := []contract.Job{
		{ID: 3, Arrival: 1, EstimatedWork: 1},
		{ID: 1, Arrival: 1, EstimatedWork: 1},
		{ID: 2, Arrival: 1, EstimatedWork: 1},
	}
	// The harness delivers equal arrivals in ascending job ID order; the FIFO
	// starter preserves that order.
	sort.Slice(jobs, func(i, j int) bool { return jobs[i].ID < jobs[j].ID })

	var actions []contract.Action
	for _, job := range jobs {
		actions = append(actions, scheduler.OnArrival(job, 1)...)
	}
	if len(actions) != 1 || actions[0].JobID != 1 {
		t.Fatalf("first dispatch = %+v, want job 1", actions)
	}
	scheduler.OnProgress(contract.Progress{JobID: 1, RemainingWork: 1}, 1)
	actions = scheduler.OnComplete(contract.Completion{JobID: 1}, 2)
	if len(actions) != 1 || actions[0].JobID != 2 {
		t.Fatalf("second dispatch = %+v, want job 2", actions)
	}
	scheduler.OnProgress(contract.Progress{JobID: 2, RemainingWork: 1}, 2)
	actions = scheduler.OnComplete(contract.Completion{JobID: 2}, 3)
	if len(actions) != 1 || actions[0].JobID != 3 {
		t.Fatalf("third dispatch = %+v, want job 3", actions)
	}
}

func TestNoDispatchWhileBusy(t *testing.T) {
	scheduler := NewScheduler(contract.Cluster{Workers: 1})
	actions := scheduler.OnArrival(contract.Job{ID: 1, Arrival: 1, EstimatedWork: 5}, 1)
	if len(actions) != 1 || actions[0].JobID != 1 {
		t.Fatalf("first dispatch = %+v, want job 1", actions)
	}
	scheduler.OnProgress(contract.Progress{JobID: 1, RemainingWork: 5}, 1)

	// Job 2 arrives while job 1 is running. FIFO must queue it, not preempt.
	actions = scheduler.OnArrival(contract.Job{ID: 2, Arrival: 2, EstimatedWork: 1}, 2)
	if len(actions) != 0 {
		t.Fatalf("busy arrival returned actions %+v, want none", actions)
	}

	actions = scheduler.OnComplete(contract.Completion{JobID: 1}, 6)
	if len(actions) != 1 || actions[0].JobID != 2 {
		t.Fatalf("completion dispatch = %+v, want job 2", actions)
	}
}

func TestStateBytesReflectsWaitingAndRunningJobs(t *testing.T) {
	scheduler := NewScheduler(contract.Cluster{Workers: 1})
	if got := scheduler.StateBytes(); got != 0 {
		t.Fatalf("empty state bytes = %d, want 0", got)
	}
	scheduler.OnArrival(contract.Job{ID: 1, Arrival: 1, EstimatedWork: 1}, 1)
	scheduler.OnArrival(contract.Job{ID: 2, Arrival: 2, EstimatedWork: 1}, 2)
	if got := scheduler.StateBytes(); got != 2*contract.PerJobStateBytes {
		t.Fatalf("state bytes = %d, want %d", got, 2*contract.PerJobStateBytes)
	}
}

func TestStarterActionsAreDeterministic(t *testing.T) {
	first := NewScheduler(contract.Cluster{Workers: 1})
	second := NewScheduler(contract.Cluster{Workers: 1})
	jobs := []contract.Job{
		{ID: 2, Arrival: 1, EstimatedWork: 2},
		{ID: 1, Arrival: 1, EstimatedWork: 1},
	}
	var firstActions, secondActions []contract.Action
	for _, job := range jobs {
		firstActions = append(firstActions, first.OnArrival(job, 1)...)
		secondActions = append(secondActions, second.OnArrival(job, 1)...)
	}
	if !reflect.DeepEqual(firstActions, secondActions) {
		t.Fatalf("same events produced different actions:\n%+v\n%+v", firstActions, secondActions)
	}
}
