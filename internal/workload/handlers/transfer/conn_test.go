package transfer

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestRetryUsesTheFastIdleConnection(t *testing.T) {
	object := bytesRepeat(32 << 10)
	var mu sync.Mutex
	var fastAddr string
	second := make(chan struct{})
	holdFast := make(chan struct{})
	holdSlow := make(chan struct{})
	var reusedFast bool
	dest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		addr := r.RemoteAddr
		mu.Lock()
		if fastAddr == "" {
			fastAddr = addr
			mu.Unlock()
			<-holdFast
			w.WriteHeader(http.StatusOK)
			return
		}
		if addr != fastAddr {
			mu.Unlock()
			select {
			case <-second:
			default:
				close(second)
			}
			<-holdSlow
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		reusedFast = true
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(dest.Close)
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(object)
	}))
	t.Cleanup(source.Close)
	transport := &http.Transport{MaxIdleConns: 8, MaxIdleConnsPerHost: 4, IdleConnTimeout: time.Minute}
	t.Cleanup(transport.CloseIdleConnections)
	handler := NewHandler(&http.Client{Transport: transport})
	handler.wait = func(context.Context, time.Duration) error { return nil }
	spec := presignedSpec(t, source.URL, dest.URL, int64(len(object)))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	errc := make(chan error, 2)
	go func() { _, err := handler.Execute(ctx, spec); errc <- err }()
	go func() { _, err := handler.Execute(ctx, spec); errc <- err }()
	select {
	case <-second:
	case <-ctx.Done():
		t.Fatal("the second upload never opened its own connection")
	}
	close(holdFast)
	time.Sleep(50 * time.Millisecond)
	close(holdSlow)
	for i := 0; i < 2; i++ {
		if err := <-errc; err != nil {
			t.Fatal(err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if !reusedFast {
		t.Fatal("retry did not use the idle connection from the finished task")
	}
}
