package transfer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestSlowReadStillStartsTheUploadAfterTheFirstMegabyte(t *testing.T) {
	object := bytes.Repeat([]byte{7}, 2<<20)
	want := sha256.Sum256(object)
	var sourceDone atomic.Int64
	var putFirst atomic.Int64
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		buf := make([]byte, 64<<10)
		for off := 0; off < len(object); off += len(buf) {
			end := off + len(buf)
			if end > len(object) {
				end = len(object)
			}
			copy(buf, object[off:end])
			if _, err := w.Write(buf[:end-off]); err != nil {
				sourceDone.Store(time.Now().UnixNano())
				return
			}
			if end == len(object) {
				sourceDone.Store(time.Now().UnixNano())
			}
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			time.Sleep(time.Duration(end-off) * time.Second / (512 << 10))
		}
	}))
	t.Cleanup(source.Close)
	dest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if putFirst.CompareAndSwap(0, time.Now().UnixNano()) {
		}
		got, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("put: %v", err)
			return
		}
		if sha256.Sum256(got) != want {
			t.Errorf("put bytes=%d", len(got))
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(dest.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	spec := presignedSpec(t, source.URL, dest.URL, int64(len(object)))
	spec.Resources.MemoryBytes = bufferedBytes(int64(len(object)))
	result, err := retryingHandler().Execute(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	if result.BytesProcessed != int64(len(object)) {
		t.Fatalf("bytes=%d", result.BytesProcessed)
	}
	if putFirst.Load() == 0 || sourceDone.Load() == 0 || putFirst.Load() >= sourceDone.Load() {
		t.Fatalf("put=%d source=%d", putFirst.Load(), sourceDone.Load())
	}
}

func TestIdleUploadWaitingOnTheReadIsNotDiscarded(t *testing.T) {
	prevBudget, prevAfter := sourceBudget, hedgeAfter
	sourceBudget, hedgeAfter = time.Hour, time.Hour
	t.Cleanup(func() { sourceBudget, hedgeAfter = prevBudget, prevAfter })
	object := bytes.Repeat([]byte{8}, 2<<20)
	want := sha256.Sum256(object)
	var puts atomic.Int32
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if _, err := w.Write(object[:1<<20]); err != nil {
			return
		}
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		time.Sleep(3500 * time.Millisecond)
		_, _ = w.Write(object[1<<20:])
	}))
	t.Cleanup(source.Close)
	dest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		puts.Add(1)
		got, err := io.ReadAll(r.Body)
		if err != nil {
			return
		}
		if sha256.Sum256(got) != want {
			t.Errorf("put bytes=%d", len(got))
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(dest.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	spec := presignedSpec(t, source.URL, dest.URL, int64(len(object)))
	spec.Resources.MemoryBytes = bufferedBytes(int64(len(object)))
	if _, err := retryingHandler().Execute(ctx, spec); err != nil {
		t.Fatal(err)
	}
	if puts.Load() != 1 {
		t.Fatalf("puts=%d, a waiting upload was discarded", puts.Load())
	}
}

func TestTruncatedDownloadDoesNotCompleteABadUpload(t *testing.T) {
	object := bytes.Repeat([]byte{9}, 64<<10)
	want := sha256.Sum256(object)
	var hits atomic.Int32
	var bad atomic.Int32
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if hits.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("nope"))
			return
		}
		_, _ = w.Write(object)
	}))
	t.Cleanup(source.Close)
	dest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, err := io.ReadAll(r.Body)
		if err != nil {
			return
		}
		if sha256.Sum256(got) != want {
			bad.Add(1)
			t.Errorf("completed put bytes=%d", len(got))
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(dest.Close)
	spec := presignedSpec(t, source.URL, dest.URL, int64(len(object)))
	spec.Resources.MemoryBytes = bufferedBytes(int64(len(object)))
	if _, err := retryingHandler().Execute(context.Background(), spec); err != nil {
		t.Fatal(err)
	}
	if hits.Load() < 2 || bad.Load() != 0 {
		t.Fatalf("hits=%d bad=%d", hits.Load(), bad.Load())
	}
}

func TestDifferentETagAbortsTheUpload(t *testing.T) {
	header := make(http.Header)
	header.Set("ETag", `"b"`)
	if err := (&sourceStamp{etag: "a", set: true}).observe(&http.Response{StatusCode: 200, Header: header}); err == nil || resetReason(err) != "etag" {
		t.Fatalf("err=%v", err)
	}
	prevBudget, prevAfter := sourceBudget, hedgeAfter
	sourceBudget, hedgeAfter = 2*time.Second, 200*time.Millisecond
	readMedian = false
	t.Cleanup(func() {
		sourceBudget, hedgeAfter = prevBudget, prevAfter
		readMedian = true
	})
	object := bytes.Repeat([]byte{2}, 2<<20)
	var hits atomic.Int32
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := hits.Add(1)
		if n == 1 {
			w.Header().Set("ETag", `"aaaa"`)
			_, _ = w.Write(object[:1<<20])
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			<-r.Context().Done()
			return
		}
		w.Header().Set("ETag", `"bbbb"`)
		_, _ = w.Write(object)
	}))
	t.Cleanup(source.Close)
	var good atomic.Int32
	dest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, err := io.ReadAll(r.Body)
		if err != nil || !bytes.Equal(got, object) {
			return
		}
		good.Add(1)
		w.Header().Set("ETag", `"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"`)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(dest.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	spec := presignedSpec(t, source.URL, dest.URL, int64(len(object)))
	spec.Resources.MemoryBytes = bufferedBytes(int64(len(object)))
	if _, err := retryingHandler().Execute(ctx, spec); err != nil {
		t.Fatal(err)
	}
	if hits.Load() < 3 || good.Load() != 1 {
		t.Fatalf("hits=%d good=%d", hits.Load(), good.Load())
	}
}

func TestValidReadReplacementDoesNotResetTheUpload(t *testing.T) {
	prevBudget, prevAfter := sourceBudget, hedgeAfter
	sourceBudget, hedgeAfter = 2*time.Second, 200*time.Millisecond
	readMedian = false
	t.Cleanup(func() {
		sourceBudget, hedgeAfter = prevBudget, prevAfter
		readMedian = true
	})
	object := bytes.Repeat([]byte{5}, 2<<20)
	var hits atomic.Int32
	var completed atomic.Int32
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"same"`)
		if hits.Add(1) == 1 {
			_, _ = w.Write(object[:1<<20])
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			<-r.Context().Done()
			return
		}
		_, _ = w.Write(object)
	}))
	t.Cleanup(source.Close)
	dest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, err := io.ReadAll(r.Body)
		if err != nil || !bytes.Equal(got, object) {
			return
		}
		completed.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(dest.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	spec := presignedSpec(t, source.URL, dest.URL, int64(len(object)))
	spec.Resources.MemoryBytes = bufferedBytes(int64(len(object)))
	if _, err := retryingHandler().Execute(ctx, spec); err != nil {
		t.Fatal(err)
	}
	if hits.Load() < 2 || completed.Load() != 1 {
		t.Fatalf("hits=%d completed=%d", hits.Load(), completed.Load())
	}
}
