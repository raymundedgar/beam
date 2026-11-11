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

func TestOverlapKeepsTheUploadWhileTheDownloadPauses(t *testing.T) {
	withHedge(t, 100*time.Millisecond, 1<<20)
	previous := putAckQuiet
	putAckQuiet = 50 * time.Millisecond
	t.Cleanup(func() { putAckQuiet = previous })
	object := bytes.Repeat([]byte{7}, 2<<20)
	var puts atomic.Int32
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(object[:1<<20])
		w.(http.Flusher).Flush()
		time.Sleep(300 * time.Millisecond)
		_, _ = w.Write(object[1<<20:])
	}))
	t.Cleanup(source.Close)
	dest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		puts.Add(1)
		body, _ := io.ReadAll(r.Body)
		sum := sha256.Sum256(body)
		want := sha256.Sum256(object)
		if !bytes.Equal(sum[:], want[:]) {
			t.Errorf("uploaded %d bytes", len(body))
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(dest.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := retryingHandler().Execute(ctx, presignedSpec(t, source.URL, dest.URL, int64(len(object))))
	if err != nil {
		t.Fatal(err)
	}
	if result.BytesProcessed != int64(len(object)) || puts.Load() != 1 {
		t.Fatalf("bytes=%d puts=%d", result.BytesProcessed, puts.Load())
	}
}

func TestOverlapDoesNotUploadAReadThatWillBeReplaced(t *testing.T) {
	buf := newFollowBuffer(50 << 20)
	if _, err := buf.Write(bytes.Repeat([]byte{1}, 8<<20)); err != nil {
		t.Fatal(err)
	}
	if overlapReady(buf, time.Now().Add(-2*time.Second), 50<<20) {
		t.Fatal("an 8 MiB read of 50 MiB after 2s would be discarded, but an upload was started")
	}
	if _, err := buf.Write(bytes.Repeat([]byte{1}, 32<<20)); err != nil {
		t.Fatal(err)
	}
	if !overlapReady(buf, time.Now().Add(-2*time.Second), 50<<20) {
		t.Fatal("an upload was held after 40 of 50 MiB were buffered")
	}
	fast := newFollowBuffer(50 << 20)
	if _, err := fast.Write(bytes.Repeat([]byte{1}, 20<<20)); err != nil {
		t.Fatal(err)
	}
	if !overlapReady(fast, time.Now().Add(-1500*time.Millisecond), 50<<20) {
		t.Fatal("a read that can finish inside the budget did not start an upload")
	}
}

func TestOverlapCancelUnblocksTheDownload(t *testing.T) {
	withHedge(t, 200*time.Millisecond, 1<<20)
	object := bytesRepeat(2 << 20)
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(object[:1<<20])
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	t.Cleanup(source.Close)
	dest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(dest.Close)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := retryingHandler().Execute(ctx, presignedSpec(t, source.URL, dest.URL, int64(len(object))))
		done <- err
	}()
	time.Sleep(200 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("cancelled transfer returned success")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("cancel did not unblock the overlapped transfer")
	}
}
