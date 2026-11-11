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

func TestUploadStartsWhileTheDownloadContinues(t *testing.T) {
	withHedge(t, 200*time.Millisecond, 1<<20)
	object := bytesRepeat(2 << 20)
	var sourceDone atomic.Int64
	var putFirst atomic.Int64
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if _, err := w.Write(object[:1<<20]); err != nil {
			return
		}
		w.(http.Flusher).Flush()
		time.Sleep(400 * time.Millisecond)
		_, _ = w.Write(object[1<<20:])
		sourceDone.Store(time.Now().UnixNano())
	}))
	t.Cleanup(source.Close)
	dest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf := make([]byte, 1)
		if _, err := r.Body.Read(buf); err != nil {
			t.Errorf("read upload: %v", err)
			return
		}
		putFirst.Store(time.Now().UnixNano())
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(dest.Close)
	handler := retryingHandler()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := handler.Execute(ctx, presignedSpec(t, source.URL, dest.URL, int64(len(object))))
	if err != nil {
		t.Fatal(err)
	}
	if result.BytesProcessed != int64(len(object)) {
		t.Fatalf("bytes=%d", result.BytesProcessed)
	}
	if putFirst.Load() == 0 || sourceDone.Load() == 0 || putFirst.Load() >= sourceDone.Load() {
		t.Fatalf("upload first byte %d, download finished %d", putFirst.Load(), sourceDone.Load())
	}
}
