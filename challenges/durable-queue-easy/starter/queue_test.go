package starter

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/roughstack/challenges/challenges/durable-queue-easy/contract"
)

func TestFIFOOrder(t *testing.T) {
	q, err := OpenQueue("", 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for id := uint64(1); id <= 3; id++ {
		if err := q.Enqueue(ctx, contract.Job{ID: id, Payload: []byte{byte(id)}}); err != nil {
			t.Fatal(err)
		}
	}
	for id := uint64(1); id <= 3; id++ {
		lease, err := q.Lease(ctx, id, 1000)
		if err != nil {
			t.Fatal(err)
		}
		if lease.Job.ID != id || len(lease.Job.Payload) != 1 || lease.Job.Payload[0] != byte(id) {
			t.Fatalf("delivery order broken: got job %d with payload %v, want %d", lease.Job.ID, lease.Job.Payload, id)
		}
		if err := q.Ack(ctx, lease.Token); err != nil {
			t.Fatal(err)
		}
	}
}

func TestNackIncrementsAttemptAndReturnsToQueue(t *testing.T) {
	q, _ := OpenQueue("", 1<<20)
	ctx := context.Background()
	if err := q.Enqueue(ctx, contract.Job{ID: 1, Payload: []byte("a")}); err != nil {
		t.Fatal(err)
	}

	first, err := q.Lease(ctx, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if first.Attempt != 1 {
		t.Fatalf("first attempt = %d, want 1", first.Attempt)
	}
	if err := q.Nack(ctx, first.Token); err != nil {
		t.Fatal(err)
	}

	second, err := q.Lease(ctx, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if second.Job.ID != 1 || second.Attempt != 2 {
		t.Fatalf("redelivery mismatch: id=%d attempt=%d", second.Job.ID, second.Attempt)
	}
	if err := q.Ack(ctx, second.Token); err != nil {
		t.Fatal(err)
	}
}

func TestVisibilityBoundaryAndStaleToken(t *testing.T) {
	q, _ := OpenQueue("", 1<<20)
	ctx := context.Background()
	if err := q.Enqueue(ctx, contract.Job{ID: 1, Payload: []byte("a")}); err != nil {
		t.Fatal(err)
	}

	first, err := q.Lease(ctx, 10, 5)
	if err != nil {
		t.Fatal(err)
	}
	if first.Deadline != 15 || first.Attempt != 1 {
		t.Fatalf("first lease = %+v, want deadline 15 attempt 1", first)
	}

	// At tick 14 the visibility window is still open, so the job is not
	// available and Lease must block until the context expires.
	timeoutCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := q.Lease(timeoutCtx, 14, 5); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("lease at tick 14 = %v, want context deadline exceeded", err)
	}

	// At tick 15 the deadline is reached and the job is redelivered.
	second, err := q.Lease(ctx, 15, 5)
	if err != nil {
		t.Fatal(err)
	}
	if second.Job.ID != 1 || second.Attempt != 2 || second.Token == first.Token {
		t.Fatalf("redelivery mismatch: %+v", second)
	}

	// The original token is now stale.
	if err := q.Ack(ctx, first.Token); !errors.Is(err, contract.ErrStaleToken) {
		t.Fatalf("stale token ack = %v, want ErrStaleToken", err)
	}
	if err := q.Ack(ctx, second.Token); err != nil {
		t.Fatal(err)
	}
}

func TestBackpressureCancellationAndBlockThenCancel(t *testing.T) {
	q, _ := OpenQueue("", 10)
	ctx := context.Background()
	if err := q.Enqueue(ctx, contract.Job{ID: 1, Payload: make([]byte, 6)}); err != nil {
		t.Fatal(err)
	}

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := q.Enqueue(canceledCtx, contract.Job{ID: 2, Payload: make([]byte, 6)}); !errors.Is(err, context.Canceled) {
		t.Fatalf("enqueue under backpressure with canceled context = %v, want cancellation", err)
	}

	// Block a producer, then cancel it, and confirm nothing was admitted.
	blockedCtx, blockCancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- q.Enqueue(blockedCtx, contract.Job{ID: 3, Payload: make([]byte, 6)})
	}()
	time.Sleep(50 * time.Millisecond)
	blockCancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("blocked enqueue after cancel = %v, want cancellation", err)
	}

	// Only job 1 was admitted.
	lease, err := q.Lease(ctx, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if lease.Job.ID != 1 {
		t.Fatalf("unexpected job %d admitted", lease.Job.ID)
	}
	if err := q.Ack(ctx, lease.Token); err != nil {
		t.Fatal(err)
	}
	emptyCtx, emptyCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer emptyCancel()
	if _, err := q.Lease(emptyCtx, 0, 1000); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("queue was not empty after cancel: %v", err)
	}
}

func TestZeroCapacityRejectsOversizedJobs(t *testing.T) {
	q, _ := OpenQueue("", 0)
	ctx := context.Background()
	if err := q.Enqueue(ctx, contract.Job{ID: 1, Payload: []byte("x")}); !errors.Is(err, contract.ErrJobTooLarge) {
		t.Fatalf("oversized enqueue = %v, want ErrJobTooLarge", err)
	}
	// A zero-length payload occupies no bytes and fits a zero-capacity queue.
	if err := q.Enqueue(ctx, contract.Job{ID: 2, Payload: nil}); err != nil {
		t.Fatal(err)
	}
	lease, err := q.Lease(ctx, 0, 1000)
	if err != nil || lease.Job.ID != 2 {
		t.Fatalf("lease = %+v err = %v, want job 2", lease, err)
	}
	if err := q.Ack(ctx, lease.Token); err != nil {
		t.Fatal(err)
	}
}

func TestCloseIsIdempotentAndWakesBlockedWaiter(t *testing.T) {
	q, _ := OpenQueue("", 100)
	ctx := context.Background()
	if err := q.Enqueue(ctx, contract.Job{ID: 1, Payload: make([]byte, 100)}); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Lease(ctx, 0, 1<<30); err != nil {
		t.Fatal(err)
	}

	producerErr := make(chan error, 1)
	consumerErr := make(chan error, 1)
	go func() { producerErr <- q.Enqueue(ctx, contract.Job{ID: 2, Payload: []byte{1}}) }()
	go func() { _, err := q.Lease(ctx, 0, 1<<30); consumerErr <- err }()

	// Let both goroutines reach the blocking wait before closing so the test
	// verifies the wake-up path rather than a pre-close fast path.
	time.Sleep(50 * time.Millisecond)

	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	if err := q.Close(); err != nil {
		t.Fatalf("Close is not idempotent: %v", err)
	}
	if err := <-producerErr; !errors.Is(err, contract.ErrClosed) {
		t.Fatalf("blocked producer error = %v, want ErrClosed", err)
	}
	if err := <-consumerErr; !errors.Is(err, contract.ErrClosed) {
		t.Fatalf("blocked consumer error = %v, want ErrClosed", err)
	}
	if err := q.Enqueue(ctx, contract.Job{ID: 3, Payload: []byte{1}}); !errors.Is(err, contract.ErrClosed) {
		t.Fatalf("Enqueue after Close = %v, want ErrClosed", err)
	}
}

func TestStaleAndUnknownTokensAreRejected(t *testing.T) {
	q, _ := OpenQueue("", 1<<20)
	ctx := context.Background()
	if err := q.Enqueue(ctx, contract.Job{ID: 1, Payload: []byte("a")}); err != nil {
		t.Fatal(err)
	}
	lease, err := q.Lease(ctx, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Ack(ctx, lease.Token); err != nil {
		t.Fatal(err)
	}
	if err := q.Ack(ctx, lease.Token); !errors.Is(err, contract.ErrStaleToken) {
		t.Fatalf("duplicate ack = %v, want ErrStaleToken", err)
	}
	if err := q.Nack(ctx, lease.Token); !errors.Is(err, contract.ErrStaleToken) {
		t.Fatalf("stale nack = %v, want ErrStaleToken", err)
	}
	if err := q.Ack(ctx, 999999); !errors.Is(err, contract.ErrStaleToken) {
		t.Fatalf("unknown token ack = %v, want ErrStaleToken", err)
	}
	if err := q.Nack(ctx, 999999); !errors.Is(err, contract.ErrStaleToken) {
		t.Fatalf("unknown token nack = %v, want ErrStaleToken", err)
	}
}

func TestPayloadOwnershipBoundaries(t *testing.T) {
	q, _ := OpenQueue("", 1<<20)
	ctx := context.Background()
	payload := []byte("hello")
	if err := q.Enqueue(ctx, contract.Job{ID: 1, Payload: payload}); err != nil {
		t.Fatal(err)
	}
	payload[0] = 'X'

	first, err := q.Lease(ctx, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if string(first.Job.Payload) != "hello" {
		t.Fatalf("Enqueue retained a mutable caller alias: %q", first.Job.Payload)
	}
	first.Job.Payload[0] = 'Y'
	if err := q.Nack(ctx, first.Token); err != nil {
		t.Fatal(err)
	}
	second, err := q.Lease(ctx, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if string(second.Job.Payload) != "hello" {
		t.Fatalf("Lease returned mutable internal storage: %q", second.Job.Payload)
	}
	if err := q.Ack(ctx, second.Token); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentFIFOProducerConsumer(t *testing.T) {
	q, _ := OpenQueue("", 1<<20)
	const jobs = 300
	go func() {
		for id := 1; id <= jobs; id++ {
			if err := q.Enqueue(context.Background(), contract.Job{ID: uint64(id), Payload: []byte{byte(id)}}); err != nil {
				t.Errorf("enqueue %d: %v", id, err)
				return
			}
		}
	}()

	got := make([]uint64, 0, jobs)
	for len(got) < jobs {
		lease, err := q.Lease(context.Background(), 0, 1<<30)
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, lease.Job.ID)
		if err := q.Ack(context.Background(), lease.Token); err != nil {
			t.Fatal(err)
		}
	}
	for index, id := range got {
		if id != uint64(index+1) {
			t.Fatalf("FIFO violated under concurrency: position %d got job %d", index, id)
		}
	}
}

func TestConcurrentProgressNoLossOrDuplication(t *testing.T) {
	q, _ := OpenQueue("", 1<<20)
	const jobs = 500
	const consumers = 4

	go func() {
		for id := 1; id <= jobs; id++ {
			if err := q.Enqueue(context.Background(), contract.Job{ID: uint64(id), Payload: []byte{byte(id)}}); err != nil {
				t.Errorf("enqueue %d: %v", id, err)
				return
			}
		}
	}()

	var mu sync.Mutex
	acked := make(map[uint64]bool)
	var wg sync.WaitGroup
	for c := 0; c < consumers; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				lease, err := q.Lease(context.Background(), 0, 1<<30)
				if err != nil {
					return
				}
				if err := q.Ack(context.Background(), lease.Token); err != nil {
					t.Errorf("ack job %d: %v", lease.Job.ID, err)
					return
				}
				mu.Lock()
				acked[lease.Job.ID] = true
				mu.Unlock()
			}
		}()
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		mu.Lock()
		done := len(acked) == jobs
		mu.Unlock()
		if done {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for all jobs to complete")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(acked) != jobs {
		t.Fatalf("acked %d jobs, want %d", len(acked), jobs)
	}
	for id := 1; id <= jobs; id++ {
		if !acked[uint64(id)] {
			t.Fatalf("job %d was lost", id)
		}
	}
}
