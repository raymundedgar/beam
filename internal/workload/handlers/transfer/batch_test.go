package transfer

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestReadHedgeKeepsTheFasterGet(t *testing.T) {
	previous := hedgeRead
	hedgeRead = true
	t.Cleanup(func() { hedgeRead = previous })
	object := bytesRepeat(16 << 10)
	var reads atomic.Int32
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if reads.Add(1) == 1 {
			select {
			case <-r.Context().Done():
			case <-time.After(3 * time.Second):
				t.Errorf("the slower GET was not cancelled")
			}
			return
		}
		_, _ = w.Write(object)
	}))
	t.Cleanup(source.Close)
	dest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(dest.Close)
	handler := NewHandler(&http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 4}})
	handler.wait = func(context.Context, time.Duration) error { return nil }
	spec := presignedSpec(t, source.URL, dest.URL, int64(len(object)))
	spec.BatchID = "batch-hedge"
	spec.AttemptID = "only"
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	started := time.Now()
	result, err := handler.Execute(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	if result.BytesProcessed != int64(len(object)) {
		t.Fatalf("bytes=%d", result.BytesProcessed)
	}
	if time.Since(started) > 2*time.Second {
		t.Fatalf("hedge waited for the slow GET: %s", time.Since(started).Round(time.Millisecond))
	}
}
