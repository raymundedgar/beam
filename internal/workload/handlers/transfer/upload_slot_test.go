package transfer

import (
	"context"
	"crypto/sha256"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestEightSendingUploadsMakeTheNinthWait(t *testing.T) {
	gate := newUploadGate(make(chan struct{}, 8), make(chan struct{}, 16))
	ctx := context.Background()
	holders := make([]*sendLease, 8)
	for i := range holders {
		holders[i] = gate.begin(nil)
		holders[i].task = "sending"
		holders[i].batch = "batch"
		if err := holders[i].acquireSend(ctx); err != nil {
			t.Fatal(err)
		}
	}
	got := make(chan struct{})
	ninth := gate.begin(nil)
	go func() {
		if err := ninth.acquireSend(ctx); err != nil {
			t.Error(err)
		}
		close(got)
	}()
	select {
	case <-got:
		t.Fatal("ninth acquired a token while eight were sending")
	case <-time.After(80 * time.Millisecond):
	}
	holders[0].releaseSend()
	select {
	case <-got:
	case <-time.After(time.Second):
		t.Fatal("ninth was not admitted after a sender released")
	}
	ninth.releaseSend()
	for _, lease := range holders[1:] {
		gate.end(lease)
	}
	gate.end(holders[0])
	if len(gate.send) != 0 || len(gate.open) != 0 {
		t.Fatalf("tokens left send=%d open=%d", len(gate.send), len(gate.open))
	}
}

func TestStarvedUploadReleasesAndDoesNotChurn(t *testing.T) {
	prevRelease, prevAhead := uploadStarveRelease, uploadReacquireBytes
	uploadStarveRelease = 40 * time.Millisecond
	uploadReacquireBytes = 4
	t.Cleanup(func() {
		uploadStarveRelease = prevRelease
		uploadReacquireBytes = prevAhead
	})
	ctx := context.Background()
	buf := newFollowBuffer(64)
	gate := newUploadGate(make(chan struct{}, 2), make(chan struct{}, 2))
	lease := gate.begin(nil)
	body := newSlotBody(ctx, buf.reader(ctx), lease)
	if _, err := buf.Write([]byte("abcd")); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 4)
	if _, err := body.Read(got); err != nil {
		t.Fatal(err)
	}
	if lease.takesN() != 1 || !lease.holding() {
		t.Fatalf("takes=%d held=%v", lease.takesN(), lease.holding())
	}
	time.Sleep(15 * time.Millisecond)
	if !lease.holding() {
		t.Fatal("a short pause released the token")
	}
	// The read is caught up. The next read waits out the starve interval.
	readDone := make(chan error, 1)
	go func() {
		_, err := body.Read(got)
		readDone <- err
	}()
	time.Sleep(80 * time.Millisecond)
	if lease.holding() {
		t.Fatal("a starved upload kept the token")
	}
	if lease.takesN() != 1 {
		t.Fatalf("churn takes=%d", lease.takesN())
	}
	if _, err := buf.Write([]byte("wxyz")); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-readDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("read did not resume")
	}
	if lease.takesN() != 2 {
		t.Fatalf("reacquire takes=%d", lease.takesN())
	}
	gate.end(lease)
	if len(gate.send) != 0 {
		t.Fatalf("token leaked")
	}
}

func TestCancelReturnsUploadTokens(t *testing.T) {
	gate := newUploadGate(make(chan struct{}, 8), make(chan struct{}, 8))
	ctx := context.Background()
	var leases []*sendLease
	for i := 0; i < 8; i++ {
		lease := gate.begin(nil)
		if err := gate.acquireOpen(ctx, lease); err != nil {
			t.Fatal(err)
		}
		if err := lease.acquireSend(ctx); err != nil {
			t.Fatal(err)
		}
		leases = append(leases, lease)
	}
	waitCtx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	waiter := gate.begin(nil)
	go func() {
		errc <- waiter.acquireSend(waitCtx)
	}()
	time.Sleep(30 * time.Millisecond)
	cancel()
	if err := <-errc; err == nil {
		t.Fatal("waiter ignored cancel")
	}
	gate.end(waiter)
	for _, lease := range leases {
		lease.setState("starved")
		gate.end(lease)
	}
	if len(gate.send) != 0 || len(gate.open) != 0 {
		t.Fatalf("send=%d open=%d", len(gate.send), len(gate.open))
	}
}

func TestWaitingForATokenIsNotDiscarded(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(context.Canceled)
	var stalls int
	go watchPutRecent(ctx, cancel, 1<<20, 30*time.Millisecond, 1<<30,
		func() (int64, bool) { return 0, true },
		func() tcpSnap { return tcpSnap{ok: true, unacked: 10} },
		func() bool { return true },
		nil, nil,
		func(float64, int64, tcpSnap, string) { stalls++ },
		nil)
	time.Sleep(150 * time.Millisecond)
	if stalls != 0 {
		t.Fatalf("stalls=%d", stalls)
	}
}

func TestReadWaitsForATokenOutsideTheBufferLock(t *testing.T) {
	prev := uploadReacquireBytes
	uploadReacquireBytes = 4
	defer func() { uploadReacquireBytes = prev }()
	gate := newUploadGate(make(chan struct{}, 1), make(chan struct{}, 2))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	holder := gate.begin(nil)
	if err := holder.acquireSend(ctx); err != nil {
		t.Fatal(err)
	}
	buf := newFollowBuffer(64)
	if _, err := buf.Write([]byte("abcdefgh")); err != nil {
		t.Fatal(err)
	}
	body := newSlotBody(ctx, buf.reader(ctx), gate.begin(nil))
	started := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		close(started)
		_, _ = body.Read(make([]byte, 4))
		close(finished)
	}()
	<-started
	time.Sleep(30 * time.Millisecond)
	done := make(chan time.Duration, 1)
	go func() {
		start := time.Now()
		_, _ = buf.Write([]byte("more"))
		done <- time.Since(start)
	}()
	select {
	case took := <-done:
		if took > 200*time.Millisecond {
			t.Fatalf("GET write blocked for %s", took)
		}
	case <-time.After(time.Second):
		t.Fatal("GET write did not return")
	}
	holder.releaseSend()
	cancel()
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("read did not return")
	}
}

func TestSentBodyIsNotAnIdleClose(t *testing.T) {
	prev := uploadStarveRelease
	uploadStarveRelease = 20 * time.Millisecond
	defer func() { uploadStarveRelease = prev }()
	ctx := context.Background()
	buf := newFollowBuffer(8)
	reader := buf.reader(ctx)
	waited := make(chan struct{})
	go func() {
		_, _ = reader.waitUntil(ctx, 1, 40*time.Millisecond)
		close(waited)
	}()
	<-waited
	lease := &sendLease{takes: 1, held: true}
	body := &slotBody{fr: reader, lease: lease}
	if body.starvedFor() < uploadStarveRelease {
		t.Fatalf("starved=%s", body.starvedFor())
	}
	if body.idleDrop(2 << 20) {
		t.Fatal("bytes already handed to the transport were treated as an idle close")
	}
	if !body.idleDrop(100) {
		t.Fatal("a starved attempt that had sent almost nothing was not an idle close")
	}
	lease.takes = 0
	if !body.idleDrop(0) {
		t.Fatal("a socket that never took the send token was not an idle close")
	}
}

func TestReleasedStarvedTokensAdmitAReadyUpload(t *testing.T) {
	prevRelease, prevAhead := uploadStarveRelease, uploadReacquireBytes
	uploadStarveRelease = 30 * time.Millisecond
	uploadReacquireBytes = 4
	defer func() {
		uploadStarveRelease, uploadReacquireBytes = prevRelease, prevAhead
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	gate := newUploadGate(make(chan struct{}, 8), make(chan struct{}, 8))
	var bodies []*slotBody
	for i := 0; i < 8; i++ {
		buf := newFollowBuffer(8)
		if _, err := buf.Write([]byte("abcd")); err != nil {
			t.Fatal(err)
		}
		body := newSlotBody(ctx, buf.reader(ctx), gate.begin(nil))
		if _, err := body.Read(make([]byte, 4)); err != nil {
			t.Fatal(err)
		}
		bodies = append(bodies, body)
	}
	var wg sync.WaitGroup
	for _, body := range bodies {
		wg.Add(1)
		go func(body *slotBody) {
			defer wg.Done()
			_, _ = body.Read(make([]byte, 4))
		}(body)
	}
	readyBuf := newFollowBuffer(8)
	if _, err := readyBuf.Write([]byte("wxyz")); err != nil {
		t.Fatal(err)
	}
	ready := newSlotBody(ctx, readyBuf.reader(ctx), gate.begin(nil))
	started := time.Now()
	got := make(chan error, 1)
	go func() {
		_, err := ready.Read(make([]byte, 4))
		got <- err
	}()
	select {
	case err := <-got:
		if err != nil {
			t.Fatal(err)
		}
		if took := time.Since(started); took > 250*time.Millisecond {
			t.Fatalf("ready upload waited %s after the others were starved", took)
		}
	case <-time.After(time.Second):
		t.Fatal("ready upload did not get a token")
	}
	deadline := time.Now().Add(time.Second)
	for _, body := range bodies {
		for body.lease.holding() && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		gate.end(body.lease)
	}
	gate.end(ready.lease)
	if len(gate.send) != 0 {
		t.Fatalf("send tokens left: %d", len(gate.send))
	}
	cancel()
	wg.Wait()
}

func TestIdleCloseRestartsOnceAndKeepsTheBytes(t *testing.T) {
	prevBudget, prevAfter, prevRelease := sourceBudget, hedgeAfter, uploadStarveRelease
	sourceBudget, hedgeAfter, uploadStarveRelease = time.Hour, time.Hour, 50*time.Millisecond
	t.Cleanup(func() {
		sourceBudget, hedgeAfter, uploadStarveRelease = prevBudget, prevAfter, prevRelease
	})
	object := bytesRepeat(48 << 10)
	want := sha256.Sum256(object)
	var puts sync.Mutex
	var conns int
	var completed int
	dest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		puts.Lock()
		conns++
		n := conns
		puts.Unlock()
		if n == 1 {
			time.Sleep(200 * time.Millisecond)
			hj, ok := w.(http.Hijacker)
			if !ok {
				t.Errorf("no hijack")
				return
			}
			conn, _, err := hj.Hijack()
			if err != nil {
				t.Errorf("hijack: %v", err)
				return
			}
			_ = conn.Close()
			return
		}
		got, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("put: %v", err)
			return
		}
		if sha256.Sum256(got) != want {
			t.Errorf("bytes=%d", len(got))
		}
		puts.Lock()
		completed++
		puts.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(dest.Close)
	release := make(chan struct{})
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if _, err := w.Write(object[:1024]); err != nil {
			return
		}
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		<-release
		_, _ = w.Write(object[1024:])
	}))
	t.Cleanup(source.Close)
	var logs safeLog
	prev := log.Writer()
	log.SetOutput(&logs)
	t.Cleanup(func() { log.SetOutput(prev) })
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	spec := presignedSpec(t, source.URL, dest.URL, int64(len(object)))
	spec.Resources.MemoryBytes = bufferedBytes(int64(len(object)))
	go func() {
		time.Sleep(900 * time.Millisecond)
		close(release)
	}()
	result, err := retryingHandler().Execute(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["sha256"] != "" && result.BytesProcessed != int64(len(object)) {
		t.Fatalf("bytes=%d", result.BytesProcessed)
	}
	puts.Lock()
	done := completed
	puts.Unlock()
	if done != 1 {
		t.Fatalf("completed puts=%d", done)
	}
	text := logs.String()
	if strings.Contains(text, "transfer put_discard ") {
		t.Fatalf("idle close counted as a discard: %s", text)
	}
	if !strings.Contains(text, "reason=idle_closed") {
		t.Fatalf("missing idle close log: %s", text)
	}
}
