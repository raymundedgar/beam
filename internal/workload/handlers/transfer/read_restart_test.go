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

func TestRestartKeepsBytesAlreadyBufferedAndTheRunningUpload(t *testing.T) {
	prevBudget, prevAfter := sourceBudget, hedgeAfter
	sourceBudget, hedgeAfter = time.Hour, time.Hour
	t.Cleanup(func() {
		sourceBudget, hedgeAfter = prevBudget, prevAfter
	})
	readMedian = true

	object := bytes.Repeat([]byte{4}, 8<<20)
	want := sha256.Sum256(object)
	var gets, puts atomic.Int32
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := gets.Add(1)
		flusher, _ := w.(http.Flusher)
		if n == 1 {
			buf := make([]byte, 64<<10)
			off := 0
			for off < len(object) {
				if r.Context().Err() != nil {
					return
				}
				end := off + len(buf)
				if end > len(object) {
					end = len(object)
				}
				if _, err := w.Write(object[off:end]); err != nil {
					return
				}
				if flusher != nil {
					flusher.Flush()
				}
				off = end
				time.Sleep(40 * time.Millisecond)
			}
			return
		}
		_, _ = w.Write(object)
	}))
	t.Cleanup(source.Close)
	host := hostOf(source.URL)
	readHosts.mu.Lock()
	saved := readHosts.m
	if readHosts.m == nil {
		readHosts.m = map[string][]float64{}
	}
	readHosts.m[host] = []float64{20 << 20}
	readHosts.mu.Unlock()
	t.Cleanup(func() {
		readHosts.mu.Lock()
		readHosts.m = saved
		readHosts.mu.Unlock()
	})

	dest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		puts.Add(1)
		got, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("put read: %v", err)
			return
		}
		if sha256.Sum256(got) != want {
			t.Errorf("put bytes=%d", len(got))
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(dest.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
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
	if gets.Load() < 2 {
		t.Fatalf("gets=%d, the slow read was not restarted", gets.Load())
	}
	if puts.Load() < 1 {
		t.Fatal("the upload never started")
	}
}
