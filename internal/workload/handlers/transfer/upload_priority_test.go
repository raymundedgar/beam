package transfer

import (
	"context"
	"sync"
	"testing"
	"time"
)

func useUploadPriority(t *testing.T, age time.Duration) {
	t.Helper()
	prev, prevAge := uploadPriority, uploadPriorityAge
	uploadPriority = true
	if age > 0 {
		uploadPriorityAge = age
	}
	t.Cleanup(func() {
		uploadPriority = prev
		uploadPriorityAge = prevAge
	})
}

func leaseFor(gate *uploadGate, task, batch, upload string) *sendLease {
	lease := gate.begin(&transferLog{workload: task, batch: batch})
	lease.setUpload(upload)
	return lease
}

func TestPriorityBackToBackDoesNotDoubleGrant(t *testing.T) {
	useUploadPriority(t, time.Hour)
	var rules []string
	prev := uploadHandoff
	uploadHandoff = func(rule, waiter string, age time.Duration, queued int) {
		rules = append(rules, rule+":"+waiter+":"+age.Round(time.Millisecond).String())
	}
	t.Cleanup(func() { uploadHandoff = prev })

	gate := newUploadGate(make(chan struct{}, 2), make(chan struct{}, 4))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h1 := leaseFor(gate, "holder-1", "held", "held-upload")
	h2 := leaseFor(gate, "holder-2", "held", "held-upload")
	if err := h1.acquireSend(ctx); err != nil || h2.acquireSend(ctx) != nil {
		t.Fatal(err)
	}
	a1 := leaseFor(gate, "batch-a-1", "batch-a", "upload-1")
	a2 := leaseFor(gate, "batch-a-2", "batch-a", "upload-1")
	b1 := leaseFor(gate, "batch-b-1", "batch-b", "upload-2")
	got := make(chan string, 3)
	var wg sync.WaitGroup
	for _, lease := range []*sendLease{a1, a2, b1} {
		lease := lease
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := lease.acquireSend(ctx); err != nil {
				got <- "err"
				return
			}
			got <- lease.task
		}()
	}
	time.Sleep(50 * time.Millisecond)
	h1.releaseSend()
	h2.releaseSend()
	first, second := <-got, <-got
	if first == second || (first == "batch-a-1" && second == "batch-a-2") || (first == "batch-a-2" && second == "batch-a-1") {
		t.Fatalf("both tokens went to one empty batch: %s %s rules=%v", first, second, rules)
	}
	if len(rules) < 2 {
		t.Fatalf("handoffs=%v", rules)
	}
	h1.releaseSend()
	h2.releaseSend()
	a1.releaseSend()
	a2.releaseSend()
	b1.releaseSend()
	cancel()
	wg.Wait()
	gate.end(h1)
	gate.end(h2)
	gate.end(a1)
	gate.end(a2)
	gate.end(b1)
}

func TestPrioritySplitUploadsWhenBatchIsShared(t *testing.T) {
	useUploadPriority(t, time.Hour)
	gate := newUploadGate(make(chan struct{}, 2), make(chan struct{}, 4))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h1 := leaseFor(gate, "h1", "held", "held")
	h2 := leaseFor(gate, "h2", "held", "held")
	if err := h1.acquireSend(ctx); err != nil || h2.acquireSend(ctx) != nil {
		t.Fatal(err)
	}
	u1 := leaseFor(gate, "p1", "same-batch", "upload-a")
	u2 := leaseFor(gate, "p2", "same-batch", "upload-b")
	u3 := leaseFor(gate, "p3", "same-batch", "upload-a")
	got := make(chan string, 3)
	var wg sync.WaitGroup
	for _, lease := range []*sendLease{u1, u2, u3} {
		lease := lease
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := lease.acquireSend(ctx); err != nil {
				got <- "err"
				return
			}
			got <- lease.upload
		}()
	}
	time.Sleep(40 * time.Millisecond)
	h1.releaseSend()
	h2.releaseSend()
	first, second := <-got, <-got
	if first == second {
		t.Fatalf("both tokens went to upload %s", first)
	}
	u1.releaseSend()
	u2.releaseSend()
	u3.releaseSend()
	cancel()
	wg.Wait()
	gate.end(h1)
	gate.end(h2)
	gate.end(u1)
	gate.end(u2)
	gate.end(u3)
}

func TestPriorityAgeBeatsANewEmptyBatch(t *testing.T) {
	useUploadPriority(t, 30*time.Millisecond)
	gate := newUploadGate(make(chan struct{}, 1), make(chan struct{}, 2))
	ctx := context.Background()
	holder := leaseFor(gate, "holder", "held", "held")
	if err := holder.acquireSend(ctx); err != nil {
		t.Fatal(err)
	}
	old := leaseFor(gate, "old-waiter", "busy-batch", "busy-upload")
	gate.markDelivered("busy-batch")
	started := make(chan struct{})
	got := make(chan string, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		close(started)
		if err := old.acquireSend(ctx); err != nil {
			got <- "err"
			return
		}
		got <- old.task
	}()
	<-started
	time.Sleep(40 * time.Millisecond)
	fresh := leaseFor(gate, "fresh-waiter", "empty-batch", "fresh-upload")
	go func() {
		defer wg.Done()
		if err := fresh.acquireSend(ctx); err != nil {
			got <- "err"
			return
		}
		got <- fresh.task
	}()
	time.Sleep(20 * time.Millisecond)
	holder.releaseSend()
	if winner := <-got; winner != old.task {
		t.Fatalf("winner=%s", winner)
	}
	old.releaseSend()
	if winner := <-got; winner != fresh.task {
		t.Fatalf("second=%s", winner)
	}
	fresh.releaseSend()
	wg.Wait()
	gate.end(holder)
	gate.end(old)
	gate.end(fresh)
}

func TestPriorityReacquireAgeIsTheCurrentWait(t *testing.T) {
	useUploadPriority(t, time.Hour)
	gate := newUploadGate(make(chan struct{}, 1), make(chan struct{}, 2))
	ctx := context.Background()
	again := leaseFor(gate, "reacquire", "old-batch", "old-upload")
	if err := again.acquireSend(ctx); err != nil {
		t.Fatal(err)
	}
	empty := leaseFor(gate, "empty", "empty-batch", "empty-upload")
	got := make(chan string, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		if err := empty.acquireSend(ctx); err != nil {
			got <- "err"
			return
		}
		got <- empty.task
	}()
	time.Sleep(30 * time.Millisecond)
	again.releaseSend()
	go func() {
		defer wg.Done()
		if err := again.acquireSend(ctx); err != nil {
			got <- "err"
			return
		}
		got <- again.task
	}()
	if winner := <-got; winner != empty.task {
		t.Fatalf("reacquire beat a waiting empty batch: %s", winner)
	}
	empty.releaseSend()
	if winner := <-got; winner != again.task {
		t.Fatalf("second=%s", winner)
	}
	again.releaseSend()
	wg.Wait()
	gate.end(again)
	gate.end(empty)
}

func TestPriorityCancelBeforeAndAfterGrant(t *testing.T) {
	useUploadPriority(t, time.Hour)
	gate := newUploadGate(make(chan struct{}, 1), make(chan struct{}, 2))
	ctx := context.Background()
	holder := leaseFor(gate, "holder", "held", "held")
	if err := holder.acquireSend(ctx); err != nil {
		t.Fatal(err)
	}
	waitCtx, cancel := context.WithCancel(ctx)
	early := leaseFor(gate, "early", "batch-e", "upload-e")
	errc := make(chan error, 1)
	go func() { errc <- early.acquireSend(waitCtx) }()
	time.Sleep(20 * time.Millisecond)
	cancel()
	if err := <-errc; err == nil {
		t.Fatal("cancelled waiter acquired")
	}
	next := leaseFor(gate, "next", "batch-n", "upload-n")
	got := make(chan struct{})
	go func() {
		if err := next.acquireSend(ctx); err != nil {
			t.Errorf("next: %v", err)
		}
		close(got)
	}()
	time.Sleep(20 * time.Millisecond)
	holder.releaseSend()
	select {
	case <-got:
	case <-time.After(time.Second):
		t.Fatal("token did not reach the next waiter")
	}
	next.releaseSend()
	if len(gate.send) != 0 {
		t.Fatalf("tokens=%d", len(gate.send))
	}
	gate.end(holder)
	gate.end(early)
	gate.end(next)
}

func TestPriorityCancelAfterGrantReturnsTheToken(t *testing.T) {
	useUploadPriority(t, time.Hour)
	gate := newUploadGate(make(chan struct{}, 1), make(chan struct{}, 2))
	lease := leaseFor(gate, "granted", "batch-g", "upload-g")
	w := &sendWaiter{
		lease:    lease,
		grant:    make(chan struct{}, 1),
		queuedAt: time.Now(),
		upload:   "upload-g",
		batch:    "batch-g",
	}
	gate.enqueue(w)
	gate.choose()
	if !w.granted || gate.uploadN["upload-g"] != 1 || gate.batchActive["batch-g"] != 1 {
		t.Fatalf("granted=%v upload=%d batch=%d", w.granted, gate.uploadN["upload-g"], gate.batchActive["batch-g"])
	}
	next := leaseFor(gate, "next", "batch-n", "upload-n")
	got := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := next.acquireSend(context.Background()); err != nil {
			t.Errorf("next: %v", err)
			return
		}
		close(got)
	}()
	time.Sleep(20 * time.Millisecond)
	gate.cancelWait(w, gate.send)
	select {
	case <-got:
	case <-time.After(time.Second):
		t.Fatal("cancelled grant did not return the token")
	}
	if gate.uploadN["upload-g"] != 0 {
		t.Fatalf("upload still in flight: %d", gate.uploadN["upload-g"])
	}
	next.releaseSend()
	wg.Wait()
	gate.end(lease)
	gate.end(next)
	if len(gate.send) != 0 {
		t.Fatalf("tokens=%d", len(gate.send))
	}
}

func TestPriorityStarvedReleaseAndFinalReleaseWakeAWaiter(t *testing.T) {
	useUploadPriority(t, time.Hour)
	prevRelease, prevAhead := uploadStarveRelease, uploadReacquireBytes
	uploadStarveRelease = 30 * time.Millisecond
	uploadReacquireBytes = 4
	t.Cleanup(func() {
		uploadStarveRelease = prevRelease
		uploadReacquireBytes = prevAhead
	})
	ctx := context.Background()
	buf := newFollowBuffer(64)
	gate := newUploadGate(make(chan struct{}, 1), make(chan struct{}, 2))
	holder := leaseFor(gate, "holder", "held", "held-upload")
	body := newSlotBody(ctx, buf.reader(ctx), holder)
	if _, err := buf.Write([]byte("abcd")); err != nil {
		t.Fatal(err)
	}
	if _, err := body.Read(make([]byte, 4)); err != nil {
		t.Fatal(err)
	}
	waiter := leaseFor(gate, "waiter", "empty", "empty-upload")
	got := make(chan struct{})
	go func() {
		if err := waiter.acquireSend(ctx); err != nil {
			t.Errorf("waiter: %v", err)
			return
		}
		close(got)
	}()
	time.Sleep(20 * time.Millisecond)
	readDone := make(chan error, 1)
	go func() {
		_, err := body.Read(make([]byte, 4))
		readDone <- err
	}()
	select {
	case <-got:
	case <-time.After(time.Second):
		t.Fatal("starved release did not hand the token off")
	}
	waiter.releaseSend()
	if _, err := buf.Write([]byte("wxyz")); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-readDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("reacquire did not finish")
	}
	other := leaseFor(gate, "final", "empty-2", "upload-2")
	finalGot := make(chan struct{})
	go func() {
		if err := other.acquireSend(ctx); err != nil {
			t.Errorf("final: %v", err)
			return
		}
		close(finalGot)
	}()
	time.Sleep(20 * time.Millisecond)
	gate.end(holder)
	select {
	case <-finalGot:
	case <-time.After(time.Second):
		t.Fatal("final release did not hand the token off")
	}
	other.releaseSend()
	gate.end(waiter)
	gate.end(other)
	if len(gate.send) != 0 {
		t.Fatalf("tokens=%d", len(gate.send))
	}
}

func TestPriorityTokenConservation(t *testing.T) {
	useUploadPriority(t, time.Hour)
	const capN = 4
	gate := newUploadGate(make(chan struct{}, capN), make(chan struct{}, capN))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			lease := leaseFor(gate, "t", "batch", "upload")
			if i%2 == 0 {
				lease.batch = "other"
				lease.upload = "other-upload"
			}
			for n := 0; n < 20; n++ {
				acquire, stop := context.WithTimeout(ctx, 200*time.Millisecond)
				err := lease.acquireSend(acquire)
				stop()
				if err != nil {
					continue
				}
				if n%5 == 0 {
					wait, cancelWait := context.WithCancel(ctx)
					_ = wait
					cancelWait()
				}
				lease.releaseSend()
			}
			gate.end(lease)
		}(i)
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("conservation test timed out")
	}
	if got := len(gate.send); got != 0 {
		t.Fatalf("tokens held=%d", got)
	}
}

func TestPriorityStressRespectsTimeouts(t *testing.T) {
	useUploadPriority(t, 10*time.Millisecond)
	gate := newUploadGate(make(chan struct{}, 2), make(chan struct{}, 4))
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			lease := leaseFor(gate, "s", "b", "u")
			acquire, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
			err := lease.acquireSend(acquire)
			cancel()
			if err != nil {
				return
			}
			time.Sleep(time.Duration(i) * time.Millisecond)
			lease.releaseSend()
			gate.end(lease)
		}(i)
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("stress test timed out")
	}
	if len(gate.send) != 0 {
		t.Fatalf("tokens=%d", len(gate.send))
	}
}
