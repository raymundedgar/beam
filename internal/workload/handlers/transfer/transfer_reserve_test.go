package transfer

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/Beam-Network/beam/internal/workload/domain"
)

func TestTransferReserveIncomingBatchGetsReserveSlot(t *testing.T) {
	// Concurrency = 4, Reserve = 2 -> FCFS = 2, Reserved = 2
	gate := newTransferGate(nil, 4, 2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctxA := withBatch(ctx, "batch-a")
	ctxB := withBatch(ctx, "batch-b")
	ctxC := withBatch(ctx, "batch-c")
	ctxD := withBatch(ctx, "batch-d")

	// 1. Batch A acquires 2 slots, filling FCFS (2/2)
	a1, err := gate.acquire(ctxA)
	if err != nil {
		t.Fatalf("acquire a1: %v", err)
	}
	defer a1.release()
	if a1.IsReserved() {
		t.Fatalf("a1 should be FCFS, got reserved")
	}

	a2, err := gate.acquire(ctxA)
	if err != nil {
		t.Fatalf("acquire a2: %v", err)
	}
	defer a2.release()
	if a2.IsReserved() {
		t.Fatalf("a2 should be FCFS, got reserved")
	}

	// 2. Batch A tries to acquire a 3rd slot -> must block (FCFS is full and Batch A cannot use reserve slot)
	a3Done := make(chan struct{})
	go func() {
		acquireCtx, acquireCancel := context.WithTimeout(ctxA, 100*time.Millisecond)
		defer acquireCancel()
		_, _ = gate.acquire(acquireCtx)
		close(a3Done)
	}()

	select {
	case <-a3Done:
		t.Fatal("a3 should have blocked because FCFS is full and Batch A is already processing")
	case <-time.After(30 * time.Millisecond):
		// Expected to still be blocked
	}

	// 3. Batch B arrives -> gets 1 reserved slot immediately!
	b1, err := gate.acquire(ctxB)
	if err != nil {
		t.Fatalf("acquire b1: %v", err)
	}
	defer b1.release()
	if !b1.IsReserved() {
		t.Fatalf("b1 should be in reserved slot, got FCFS")
	}

	// 4. Batch C arrives -> gets 2nd reserved slot immediately!
	c1, err := gate.acquire(ctxC)
	if err != nil {
		t.Fatalf("acquire c1: %v", err)
	}
	defer c1.release()
	if !c1.IsReserved() {
		t.Fatalf("c1 should be in reserved slot, got FCFS")
	}

	// 5. Batch D arrives -> all 4 slots (2 FCFS + 2 Reserved) are full -> must block!
	dDone := make(chan struct{})
	go func() {
		acquireCtx, acquireCancel := context.WithTimeout(ctxD, 100*time.Millisecond)
		defer acquireCancel()
		_, _ = gate.acquire(acquireCtx)
		close(dDone)
	}()

	select {
	case <-dDone:
		t.Fatal("d1 should have blocked because all FCFS and reserve slots are full")
	case <-time.After(30 * time.Millisecond):
		// Expected to still be blocked
	}
}

func TestTransferReserveSameBatchCannotUseBothReservedSlots(t *testing.T) {
	// Concurrency = 4, Reserve = 2 -> FCFS = 2, Reserved = 2
	gate := newTransferGate(nil, 4, 2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctxA := withBatch(ctx, "batch-a")
	ctxB := withBatch(ctx, "batch-b")
	ctxC := withBatch(ctx, "batch-c")

	// Batch A fills FCFS (2 slots)
	a1, _ := gate.acquire(ctxA)
	defer a1.release()
	a2, _ := gate.acquire(ctxA)
	defer a2.release()

	// Batch B task 1 gets 1st reserved slot
	b1, err := gate.acquire(ctxB)
	if err != nil {
		t.Fatalf("acquire b1: %v", err)
	}
	defer b1.release()
	if !b1.IsReserved() {
		t.Fatal("b1 should be in reserved slot")
	}

	// Batch B task 2 tries to acquire -> must block! Reserved slot 2 is for a DIFFERENT batch!
	b2Done := make(chan struct{})
	go func() {
		acquireCtx, acquireCancel := context.WithTimeout(ctxB, 100*time.Millisecond)
		defer acquireCancel()
		_, _ = gate.acquire(acquireCtx)
		close(b2Done)
	}()

	select {
	case <-b2Done:
		t.Fatal("b2 should block; reserve slot 2 is for a different batch")
	case <-time.After(30 * time.Millisecond):
		// Expected to block
	}

	// Batch C arrives -> gets 2nd reserved slot immediately!
	c1, err := gate.acquire(ctxC)
	if err != nil {
		t.Fatalf("acquire c1: %v", err)
	}
	defer c1.release()
	if !c1.IsReserved() {
		t.Fatal("c1 should be in reserved slot")
	}
}

func TestTransferReserveReleaseFCFSHandoff(t *testing.T) {
	gate := newTransferGate(nil, 4, 2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctxA := withBatch(ctx, "batch-a")
	ctxB := withBatch(ctx, "batch-b")

	// Batch A takes 2 FCFS slots
	a1, _ := gate.acquire(ctxA)
	a2, _ := gate.acquire(ctxA)

	// Batch B task 1 takes reserved slot 1
	b1, _ := gate.acquire(ctxB)
	defer b1.release()

	// Now queue a3 (Batch A) and b2 (Batch B)
	got := make(chan string, 2)
	go func() {
		lease, err := gate.acquire(ctxA)
		if err == nil {
			got <- "a3"
			lease.release()
		}
	}()
	time.Sleep(10 * time.Millisecond)
	go func() {
		lease, err := gate.acquire(ctxB)
		if err == nil {
			got <- "b2"
			lease.release()
		}
	}()
	time.Sleep(20 * time.Millisecond)

	// Release a1 -> FCFS slot frees up -> a3 should be granted first (FIFO)
	a1.release()

	select {
	case first := <-got:
		if first != "a3" {
			t.Fatalf("expected a3 first in FCFS order, got %s", first)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timeout waiting for a3")
	}

	// Release a2 -> next FCFS slot frees up -> b2 should be granted
	a2.release()

	select {
	case second := <-got:
		if second != "b2" {
			t.Fatalf("expected b2 second in FCFS order, got %s", second)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timeout waiting for b2")
	}
}

func TestTransferReserveReleaseReservedSlotHandoff(t *testing.T) {
	gate := newTransferGate(nil, 4, 2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctxA := withBatch(ctx, "batch-a")
	ctxB := withBatch(ctx, "batch-b")
	ctxC := withBatch(ctx, "batch-c")
	ctxD := withBatch(ctx, "batch-d")

	// FCFS slots (2) held by Batch A
	a1, _ := gate.acquire(ctxA)
	defer a1.release()
	a2, _ := gate.acquire(ctxA)
	defer a2.release()

	// Reserved slot 1 held by Batch B
	b1, _ := gate.acquire(ctxB)

	// Reserved slot 2 held by Batch C
	c1, _ := gate.acquire(ctxC)
	defer c1.release()

	// Batch D arrives and queues (all 4 slots held)
	dGot := make(chan *transferSlotLease, 1)
	go func() {
		lease, err := gate.acquire(ctxD)
		if err == nil {
			dGot <- lease
		}
	}()
	time.Sleep(20 * time.Millisecond)

	// Batch B finishes and releases its reserved slot!
	b1.release()

	select {
	case dLease := <-dGot:
		if !dLease.IsReserved() {
			t.Fatal("dLease should have received the reserved slot")
		}
		if dLease.Batch() != "batch-d" {
			t.Fatalf("expected batch-d, got %s", dLease.Batch())
		}
		dLease.release()
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timeout waiting for batch-d to acquire released reserved slot")
	}
}

func TestTransferReserveCancellationDoesNotLeak(t *testing.T) {
	gate := newTransferGate(nil, 4, 2)
	ctxA := withBatch(context.Background(), "batch-a")
	ctxB := withBatch(context.Background(), "batch-b")

	a1, _ := gate.acquire(ctxA)
	a2, _ := gate.acquire(ctxA)
	defer a1.release()
	defer a2.release()

	// Cancelled waiter
	cancelCtx, cancel := context.WithCancel(ctxA)
	done := make(chan error, 1)
	go func() {
		_, err := gate.acquire(cancelCtx)
		done <- err
	}()
	time.Sleep(10 * time.Millisecond)
	cancel()

	err := <-done
	if err != context.Canceled {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	// Make sure Batch B can still acquire a reserved slot cleanly
	b1, err := gate.acquire(ctxB)
	if err != nil {
		t.Fatalf("acquire b1: %v", err)
	}
	if !b1.IsReserved() {
		t.Fatal("b1 should be in reserved slot")
	}
	b1.release()
}

func TestTransferReserveDisabledWhenZero(t *testing.T) {
	// Concurrency = 4, Reserve = 0 -> all 4 are FCFS
	gate := newTransferGate(nil, 4, 0)
	ctxA := withBatch(context.Background(), "batch-a")

	// Batch A should be able to acquire all 4 slots
	leases := make([]*transferSlotLease, 4)
	for i := 0; i < 4; i++ {
		var err error
		leases[i], err = gate.acquire(ctxA)
		if err != nil {
			t.Fatalf("acquire slot %d: %v", i, err)
		}
		if leases[i].IsReserved() {
			t.Fatalf("slot %d should not be reserved", i)
		}
	}
	for _, l := range leases {
		l.release()
	}
}

func TestTransferReserveFullHandlerExecute(t *testing.T) {
	object := bytesRepeat(1024)
	startedReads := make(chan string, 10)
	pause := make(chan struct{})
	var closeOnce sync.Once
	unpause := func() { closeOnce.Do(func() { close(pause) }) }
	t.Cleanup(unpause)

	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		taskName := r.URL.Query().Get("task")
		startedReads <- taskName
		<-pause
		_, _ = w.Write(object)
	}))
	t.Cleanup(source.Close)

	dest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(dest.Close)

	// Concurrency = 3, Reserve = 1 -> FCFS = 2, Reserve = 1
	handler := NewHandler(&http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 10}})
	handler.wait = func(context.Context, time.Duration) error { return nil }
	handler.slots = make(chan struct{}, 3)
	handler.slotGate = newTransferGate(handler, 3, 1)

	makeSpec := func(taskID, batchID string) domain.Spec {
		spec := presignedSpec(t, source.URL+"?task="+taskID, dest.URL, int64(len(object)))
		spec.WorkloadID = taskID
		spec.AttemptID = "1"
		spec.BatchID = batchID
		return spec
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Launch Batch A task 1 and task 2 (these take FCFS slots 1 and 2)
	go func() {
		_, _ = handler.Execute(ctx, makeSpec("a1", "batch-a"))
	}()
	go func() {
		_, _ = handler.Execute(ctx, makeSpec("a2", "batch-a"))
	}()

	// Wait until a1 and a2 are actively reading
	seenA := 0
	for seenA < 2 {
		select {
		case name := <-startedReads:
			if name == "a1" || name == "a2" {
				seenA++
			}
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for a1 and a2 to start reading")
		}
	}

	// Now launch Batch A task 3 (FCFS is full: 2/2, so a3 must block and cannot take reserve slot!)
	go func() {
		_, _ = handler.Execute(ctx, makeSpec("a3", "batch-a"))
	}()

	// Verify a3 does not start reading yet
	select {
	case name := <-startedReads:
		t.Fatalf("unexpected read started: %s (a3 should be blocked)", name)
	case <-time.After(50 * time.Millisecond):
		// Expected: a3 is blocked
	}

	// Now launch Batch B task 1 -> Batch B is incoming, so it takes the 1 reserve slot immediately!
	go func() {
		_, _ = handler.Execute(ctx, makeSpec("b1", "batch-b"))
	}()

	select {
	case name := <-startedReads:
		if name != "b1" {
			t.Fatalf("expected b1 to start reading in reserved slot, got %s", name)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for b1 to acquire reserved slot")
	}

	// Unpause everything so downloads complete
	unpause()
}

func TestTransferReservedSlotsEnvVar(t *testing.T) {
	t.Setenv("BEAM_WORKER_DL_CONCURRENCY", "6")
	t.Setenv("BEAM_WORKER_DL_RESERVE", "3")

	// Concurrency = 6, Reserved = 3 -> FCFS = 3, Reserved = 3
	h := NewHandler(&http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 10}})
	if h.slotGate.reserveLimit != 3 {
		t.Fatalf("expected reserveLimit=3, got %d", h.slotGate.reserveLimit)
	}
	if h.slotGate.fcfsLimit != 3 {
		t.Fatalf("expected fcfsLimit=3, got %d", h.slotGate.fcfsLimit)
	}

	ctx := context.Background()
	ctxA := withBatch(ctx, "batch-a")
	ctxB := withBatch(ctx, "batch-b")
	ctxC := withBatch(ctx, "batch-c")
	ctxD := withBatch(ctx, "batch-d")

	// Batch A fills all 3 FCFS slots
	a1, _ := h.acquireSlot(ctxA)
	defer a1.release()
	a2, _ := h.acquireSlot(ctxA)
	defer a2.release()
	a3, _ := h.acquireSlot(ctxA)
	defer a3.release()

	// 3 different incoming batches each get 1 reserved slot
	b1, err := h.acquireSlot(ctxB)
	if err != nil || !b1.IsReserved() {
		t.Fatalf("b1 failed to acquire reserved slot: %v", err)
	}
	defer b1.release()

	c1, err := h.acquireSlot(ctxC)
	if err != nil || !c1.IsReserved() {
		t.Fatalf("c1 failed to acquire reserved slot: %v", err)
	}
	defer c1.release()

	d1, err := h.acquireSlot(ctxD)
	if err != nil || !d1.IsReserved() {
		t.Fatalf("d1 failed to acquire reserved slot: %v", err)
	}
	defer d1.release()
}
