package contract

import "testing"

func TestConfigLevel0TriggerRunsEffective(t *testing.T) {
	if got := (Config{Level0TriggerRuns: 0}).Level0TriggerRunsEffective(); got != DefaultLevel0TriggerRuns {
		t.Fatalf("zero trigger resolved to %d, want %d", got, DefaultLevel0TriggerRuns)
	}
	if got := (Config{Level0TriggerRuns: 7}).Level0TriggerRunsEffective(); got != 7 {
		t.Fatalf("custom trigger resolved to %d, want 7", got)
	}
}

func TestRunMetaOverlapsKey(t *testing.T) {
	run := RunMeta{Entries: 3, MinKey: 10, MaxKey: 20}
	for _, key := range []uint64{10, 15, 20} {
		if !run.OverlapsKey(key) {
			t.Fatalf("key %d should overlap [10,20]", key)
		}
	}
	for _, key := range []uint64{0, 9, 21, ^uint64(0)} {
		if run.OverlapsKey(key) {
			t.Fatalf("key %d should not overlap [10,20]", key)
		}
	}

	empty := RunMeta{Entries: 0, MinKey: 10, MaxKey: 20}
	if empty.OverlapsKey(10) || empty.OverlapsKey(20) {
		t.Fatal("empty run overlapped a key")
	}
}

func TestRunMetaOverlapsRangeAndRun(t *testing.T) {
	run := RunMeta{Entries: 2, MinKey: 10, MaxKey: 20}
	if !run.OverlapsRange(5, 10) || !run.OverlapsRange(20, 25) || !run.OverlapsRange(5, 25) {
		t.Fatal("boundary range overlap failed")
	}
	if run.OverlapsRange(21, 30) || run.OverlapsRange(0, 9) {
		t.Fatal("disjoint range overlap reported")
	}

	identical := RunMeta{Entries: 2, MinKey: 10, MaxKey: 20}
	if !run.OverlapsRun(identical) {
		t.Fatal("identical ranges should overlap")
	}

	distant := RunMeta{Entries: 2, MinKey: 100, MaxKey: 110}
	if run.OverlapsRun(distant) {
		t.Fatal("distant ranges should not overlap")
	}

	empty := RunMeta{Entries: 0, MinKey: 10, MaxKey: 20}
	if empty.OverlapsRun(run) || run.OverlapsRun(empty) {
		t.Fatal("empty runs should never overlap")
	}
}
