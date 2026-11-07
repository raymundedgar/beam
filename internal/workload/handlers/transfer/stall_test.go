package transfer

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Beam-Network/beam/internal/workload/contracts"
	"github.com/Beam-Network/beam/internal/workload/domain"
)

func withStall(t *testing.T, idle, window time.Duration, floor int64, strikes int) {
	t.Helper()
	previousIdle, previousWindow, previousFloor, previousStrikes := stallIdle, stallWindow, stallFloor, stallStrikes
	stallIdle, stallWindow, stallFloor, stallStrikes = idle, window, floor, strikes
	t.Cleanup(func() {
		stallIdle, stallWindow, stallFloor, stallStrikes = previousIdle, previousWindow, previousFloor, previousStrikes
	})
}

func TestWatchStallAbortsWhenQuiet(t *testing.T) {
	withStall(t, 30*time.Millisecond, time.Second, 0, 2)
	var read atomic.Int64
	ctx, stop := watchStall(context.Background(), &read, nil)
	defer stop()
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("quiet attempt was not aborted")
	}
	if !errors.Is(context.Cause(ctx), errStalled) {
		t.Fatalf("cause=%v", context.Cause(ctx))
	}
}

func TestWatchStallAbortsBelowFloor(t *testing.T) {
	withStall(t, time.Second, 15*time.Millisecond, 1<<30, 2)
	var read atomic.Int64
	read.Store(1)
	ctx, stop := watchStall(context.Background(), &read, nil)
	defer stop()
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("slow attempt was not aborted")
	}
	if !errors.Is(context.Cause(ctx), errStalled) {
		t.Fatalf("cause=%v", context.Cause(ctx))
	}
}

func TestWatchStallKeepsAFastStream(t *testing.T) {
	withStall(t, time.Second, 20*time.Millisecond, 100, 2)
	var read atomic.Int64
	ctx, stop := watchStall(context.Background(), &read, nil)
	defer stop()
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				read.Add(10_000)
			}
		}
	}()
	select {
	case <-ctx.Done():
		close(done)
		t.Fatal("fast stream was aborted")
	case <-time.After(200 * time.Millisecond):
		close(done)
	}
}

func TestWatchStallStopsAtExpectedLength(t *testing.T) {
	withStall(t, 20*time.Millisecond, time.Second, 0, 2)
	var read atomic.Int64
	var expected atomic.Int64
	read.Store(8)
	expected.Store(8)
	ctx, stop := watchStall(context.Background(), &read, &expected)
	defer stop()
	select {
	case <-ctx.Done():
		t.Fatal("completed body was aborted while waiting")
	case <-time.After(80 * time.Millisecond):
	}
}

func TestStalledSourceRetriesTheSignedRequest(t *testing.T) {
	withStall(t, 40*time.Millisecond, time.Second, 0, 2)
	object := []byte("0123456789abcdef")
	var hits atomic.Int32
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) == 1 {
			w.Header().Set("ETag", `"abc"`)
			_, _ = w.Write(object[:4])
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
			<-r.Context().Done()
			return
		}
		if got := r.Header.Get("Range"); got != "" {
			t.Errorf("Range = %q", got)
		}
		if got := r.Header.Get("If-Range"); got != "" {
			t.Errorf("If-Range = %q", got)
		}
		_, _ = w.Write(object)
	}))
	t.Cleanup(source.Close)
	dest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if string(body) != string(object) {
			t.Errorf("destination body = %q", body)
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(dest.Close)

	result, err := retryingHandler().Execute(context.Background(), presignedSpec(t, source.URL, dest.URL, int64(len(object))))
	if err != nil {
		t.Fatal(err)
	}
	if result.BytesProcessed != int64(len(object)) || hits.Load() != 2 {
		t.Fatalf("bytes=%d sourceHits=%d", result.BytesProcessed, hits.Load())
	}
}

func TestStallCancelIsNotAWorkerFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(40 * time.Millisecond)
		cancel()
	}()
	_, err := retryingHandler().retryPresigned(ctx, func() (partResult, error) {
		return partResult{}, errStalled
	})
	if !errors.Is(err, context.Canceled) || errors.Is(err, errStalled) {
		t.Fatalf("err=%v", err)
	}
}

func TestPartsOverlapInsideTheMemoryBudget(t *testing.T) {
	const length = 8
	hold := make(chan struct{})
	var closeHold sync.Once
	release := func() { closeHold.Do(func() { close(hold) }) }
	t.Cleanup(release)
	var inside atomic.Int32
	var peak atomic.Int32
	started := make(chan struct{})
	var once sync.Once
	body := func(w http.ResponseWriter, r *http.Request) {
		n := inside.Add(1)
		for {
			old := peak.Load()
			if n <= old || peak.CompareAndSwap(old, n) {
				break
			}
		}
		if n == 2 {
			once.Do(func() { close(started) })
		}
		<-hold
		inside.Add(-1)
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("01234567"))
	}))
	t.Cleanup(source.Close)
	dest := httptest.NewServer(http.HandlerFunc(body))
	t.Cleanup(dest.Close)
	payload := twoPartPayload(t, source.URL, dest.URL, length)
	spec := domain.Spec{
		Kind: domain.KindTransferMultipart, Payload: payload,
		Resources: domain.Resources{MemoryBytes: 2 * bufferedBytes(length)},
	}
	done := make(chan error, 1)
	go func() {
		_, err := retryingHandler().Execute(context.Background(), spec)
		done <- err
	}()
	select {
	case <-started:
	case err := <-done:
		t.Fatalf("parts finished before overlapping: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("parts did not overlap")
	}
	release()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if peak.Load() != 2 {
		t.Fatalf("peak=%d", peak.Load())
	}
}

func TestPartsStayInsideTheMemoryBudget(t *testing.T) {
	const length = 8
	hold := make(chan struct{})
	var closeHold sync.Once
	release := func() { closeHold.Do(func() { close(hold) }) }
	t.Cleanup(release)
	var inside atomic.Int32
	var peak atomic.Int32
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("01234567"))
	}))
	t.Cleanup(source.Close)
	dest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := inside.Add(1)
		for {
			old := peak.Load()
			if n <= old || peak.CompareAndSwap(old, n) {
				break
			}
		}
		<-hold
		inside.Add(-1)
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(dest.Close)
	spec := domain.Spec{
		Kind: domain.KindTransferMultipart, Payload: twoPartPayload(t, source.URL, dest.URL, length),
		Resources: domain.Resources{MemoryBytes: bufferedBytes(length)},
	}
	done := make(chan error, 1)
	go func() {
		_, err := retryingHandler().Execute(context.Background(), spec)
		done <- err
	}()
	time.Sleep(100 * time.Millisecond)
	if peak.Load() != 1 {
		t.Fatalf("peak=%d", peak.Load())
	}
	release()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if peak.Load() != 1 {
		t.Fatalf("peak=%d", peak.Load())
	}
}

func twoPartPayload(t *testing.T, sourceURL, destURL string, length int64) []byte {
	t.Helper()
	part := func(index int) contracts.TransferPart {
		return contracts.TransferPart{
			Index: index, Offset: 0, Length: length,
			Source:      contracts.HTTPEndpoint{URL: sourceURL},
			Destination: contracts.HTTPEndpoint{URL: destURL, Method: http.MethodPut},
		}
	}
	payload, err := json.Marshal(contracts.MultipartTransfer{Parts: []contracts.TransferPart{part(0), part(1)}})
	if err != nil {
		t.Fatal(err)
	}
	return payload
}
