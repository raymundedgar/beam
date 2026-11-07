package transfer

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Beam-Network/beam/internal/workload/domain"
)

func TestByteCounterRecordsBlockedGaps(t *testing.T) {
	counter := &byteCounter{r: bytes.NewReader(bytes.Repeat([]byte("a"), 8))}
	buf := make([]byte, 4)
	if _, err := counter.Read(buf); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	if _, err := counter.Read(buf); err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if counter.n.Load() != 8 {
		t.Fatalf("bytes=%d", counter.n.Load())
	}
	if counter.blockedFor() < 20*time.Millisecond {
		t.Fatalf("blocked=%s", counter.blockedFor())
	}
	if counter.longestGap() < 20*time.Millisecond {
		t.Fatalf("max_gap=%s", counter.longestGap())
	}
	// A short pause is the normal copy, not a blocked write.
	short := &byteCounter{r: bytes.NewReader(bytes.Repeat([]byte("b"), 8))}
	_, _ = short.Read(buf)
	time.Sleep(time.Millisecond)
	_, _ = short.Read(buf)
	if short.blockedFor() != 0 {
		t.Fatalf("short gap counted as blocked: %s", short.blockedFor())
	}
}

func TestMeasureLogsBothSidesWithoutURLs(t *testing.T) {
	var mu sync.Mutex
	var lines []string
	handler := retryingHandler()
	handler.logf = func(format string, args ...any) {
		mu.Lock()
		lines = append(lines, fmt.Sprintf(format, args...))
		mu.Unlock()
	}
	object := []byte("0123456789abcdef")
	source, dest := measureServers(t, object)
	result, err := handler.Execute(context.Background(), presignedSpec(t, source.URL, dest.URL, int64(len(object))))
	if err != nil {
		t.Fatal(err)
	}
	if result.BytesProcessed != int64(len(object)) {
		t.Fatalf("bytes=%d", result.BytesProcessed)
	}
	mu.Lock()
	defer mu.Unlock()
	joined := strings.Join(lines, "\n")
	if strings.Contains(joined, "http://") || strings.Contains(joined, "https://") {
		t.Fatalf("measurement logged a URL:\n%s", joined)
	}
	for _, want := range []string{
		"side=source", "side=destination", "side=total",
		"status=200", "blocked=", "max_gap=", "resp_wait=", "active=1", "rate_mib=",
	} {
		if !strings.Contains(joined, want) {
			t.Errorf("log missing %q\n%s", want, joined)
		}
	}
}

func measureServers(t *testing.T, object []byte) (*httptest.Server, *httptest.Server) {
	t.Helper()
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(object)
	}))
	t.Cleanup(source.Close)
	dest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(dest.Close)
	return source, dest
}

func TestDestinationWritePauseIsNotAborted(t *testing.T) {
	withStall(t, 30*time.Millisecond, time.Second, 0, 2)
	object := bytes.Repeat([]byte("a"), 32*1024)
	var puts atomic.Int32
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(object)
	}))
	t.Cleanup(source.Close)
	dest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		puts.Add(1)
		buf := make([]byte, 1024)
		_, _ = io.ReadFull(r.Body, buf)
		time.Sleep(80 * time.Millisecond)
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(dest.Close)
	result, err := retryingHandler().Execute(context.Background(), presignedSpec(t, source.URL, dest.URL, int64(len(object))))
	if err != nil {
		t.Fatal(err)
	}
	if result.BytesProcessed != int64(len(object)) || puts.Load() != 1 {
		t.Fatalf("bytes=%d puts=%d", result.BytesProcessed, puts.Load())
	}
}

func TestNextDownloadStartsWhileAnUploadRuns(t *testing.T) {
	const length = 8
	hold := make(chan struct{})
	var closeHold sync.Once
	release := func() { closeHold.Do(func() { close(hold) }) }
	t.Cleanup(release)
	secondDownload := make(chan struct{})
	var once sync.Once
	var gets atomic.Int32
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if gets.Add(1) >= 2 {
			once.Do(func() { close(secondDownload) })
		}
		_, _ = w.Write([]byte("01234567"))
	}))
	t.Cleanup(source.Close)
	dest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-hold
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(dest.Close)
	handler := retryingHandler()
	handler.slots = make(chan struct{}, 1)
	handler.uploads = nil
	spec := domain.Spec{
		Kind: domain.KindTransferMultipart, Payload: twoPartPayload(t, source.URL, dest.URL, length),
		Resources: domain.Resources{MemoryBytes: 2 * bufferedBytes(length)},
	}
	done := make(chan error, 1)
	go func() {
		_, err := handler.Execute(context.Background(), spec)
		done <- err
	}()
	select {
	case <-secondDownload:
	case <-time.After(2 * time.Second):
		t.Fatal("second download waited for the upload")
	}
	release()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestUploadSlotsSerializePuts(t *testing.T) {
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
	handler := retryingHandler()
	handler.slots = nil
	handler.uploads = make(chan struct{}, 1)
	spec := domain.Spec{
		Kind: domain.KindTransferMultipart, Payload: twoPartPayload(t, source.URL, dest.URL, length),
		Resources: domain.Resources{MemoryBytes: 2 * bufferedBytes(length)},
	}
	done := make(chan error, 1)
	go func() {
		_, err := handler.Execute(context.Background(), spec)
		done <- err
	}()
	time.Sleep(80 * time.Millisecond)
	if peak.Load() != 1 {
		t.Fatalf("peak=%d", peak.Load())
	}
	release()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestActiveTransferCount(t *testing.T) {
	base := activeTransfers.Load()
	if got := enterTransfer(); got != base+1 {
		t.Fatalf("enter=%d base=%d", got, base)
	}
	if got := enterTransfer(); got != base+2 {
		t.Fatalf("enter=%d base=%d", got, base)
	}
	leaveTransfer()
	leaveTransfer()
	if activeTransfers.Load() != base {
		t.Fatalf("active=%d base=%d", activeTransfers.Load(), base)
	}
}
