package workload

import (
	"bytes"
	"reflect"
	"testing"
)

func TestGenerateIsDeterministicAndCoversActions(t *testing.T) {
	config := Config{Seed: ^uint64(0), Jobs: 400, MaxPayload: 128, NackEvery: 13, ExpireEvery: 31}
	firstJobs, firstActions := Generate(config)
	secondJobs, secondActions := Generate(config)
	if !reflect.DeepEqual(firstJobs, secondJobs) {
		t.Fatal("same seed produced different jobs")
	}
	if !reflect.DeepEqual(firstActions, secondActions) {
		t.Fatal("same seed produced different actions")
	}

	seen := map[Action]bool{}
	for _, action := range firstActions {
		seen[action] = true
	}
	for _, action := range []Action{ActionAck, ActionNack, ActionExpire} {
		if !seen[action] {
			t.Fatalf("action %d was not generated", action)
		}
	}
	for _, job := range firstJobs {
		if len(job.Payload) < 1 || len(job.Payload) > config.MaxPayload {
			t.Fatalf("job %d has out-of-range payload size %d", job.ID, len(job.Payload))
		}
	}
}

func TestPayloadIsDeterministic(t *testing.T) {
	first := Payload(7, 42, 96)
	second := Payload(7, 42, 96)
	if !bytes.Equal(first, second) {
		t.Fatal("same seed/id/size produced different payload bytes")
	}
	if bytes.Equal(first, Payload(8, 42, 96)) {
		t.Fatal("different seed produced identical payload bytes")
	}
	if len(Payload(7, 42, 0)) != 0 {
		t.Fatal("zero-size payload was not empty")
	}
}

func TestGenerateHandlesEmptyConfig(t *testing.T) {
	jobs, actions := Generate(Config{})
	if jobs != nil || actions != nil {
		t.Fatalf("empty config produced %d jobs and %d actions", len(jobs), len(actions))
	}
}
