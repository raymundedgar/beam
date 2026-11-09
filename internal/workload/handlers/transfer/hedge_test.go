package transfer

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func withHedge(t *testing.T, after time.Duration, minBytes int64) {
	t.Helper()
	prevAfter, prevMin := hedgeAfter, hedgeMinBytes
	hedgeAfter, hedgeMinBytes = after, minBytes
	t.Cleanup(func() {
		hedgeAfter, hedgeMinBytes = prevAfter, prevMin
	})
}

func TestTooSlowLeavesAFinishableRate(t *testing.T) {
	const total = 2 << 20
	if tooSlow(total, total, time.Second, sourceBudget) {
		t.Fatal("finished body was slow")
	}
	// 2 MiB in 1.5s is 1.33 MiB/s. Finishing 2 MiB in 8s needs 0.25 MiB/s.
	if tooSlow(total/2, total, 1500*time.Millisecond, 8*time.Second) {
		t.Fatal("half the body in 1.5s can finish inside 8s")
	}
	// 2 MiB at 2 MiB/s needs 8s and has only moved 1 MiB after 500ms?
	// 64 KiB in 1.5s cannot finish 2 MiB in 8s.
	if !tooSlow(64<<10, total, 1500*time.Millisecond, 8*time.Second) {
		t.Fatal("64 KiB in 1.5s should be replaced")
	}
	if tooSlow(64<<10, 16, time.Second, sourceBudget) {
		t.Fatal("objects under hedgeMinBytes are not judged")
	}
}

func TestFastSourceDoesNotOpenASecondGet(t *testing.T) {
	withHedge(t, 2*time.Second, 8)
	object := bytesRepeat(32 << 10)
	var hits atomic.Int32
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if r.Header.Get("Range") != "" || r.Header.Get("If-Range") != "" {
			t.Errorf("signed request gained Range %q If-Range %q", r.Header.Get("Range"), r.Header.Get("If-Range"))
		}
		_, _ = w.Write(object)
	}))
	t.Cleanup(source.Close)
	dest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(dest.Close)
	_, err := retryingHandler().Execute(context.Background(), presignedSpec(t, source.URL, dest.URL, int64(len(object))))
	if err != nil {
		t.Fatal(err)
	}
	if hits.Load() != 1 {
		t.Fatalf("source hits=%d", hits.Load())
	}
}

func TestSlowSourceStartsASecondGet(t *testing.T) {
	withHedge(t, 40*time.Millisecond, 8)
	withStall(t, 5*time.Second, time.Second, 0, 2)
	object := bytesRepeat(32 << 10)
	var hits atomic.Int32
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := hits.Add(1)
		if r.Header.Get("Range") != "" || r.Header.Get("If-Range") != "" {
			t.Errorf("signed request gained Range %q If-Range %q", r.Header.Get("Range"), r.Header.Get("If-Range"))
		}
		if n == 1 {
			_, _ = w.Write(object[:64])
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			<-r.Context().Done()
			return
		}
		_, _ = w.Write(object)
	}))
	t.Cleanup(source.Close)
	var puts atomic.Int32
	dest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		puts.Add(1)
		body, _ := io.ReadAll(r.Body)
		if string(body) != string(object) {
			t.Errorf("destination length=%d", len(body))
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(dest.Close)
	result, err := retryingHandler().Execute(context.Background(), presignedSpec(t, source.URL, dest.URL, int64(len(object))))
	if err != nil {
		t.Fatal(err)
	}
	if result.BytesProcessed != int64(len(object)) || hits.Load() != 2 || puts.Load() != 1 {
		t.Fatalf("bytes=%d hits=%d puts=%d", result.BytesProcessed, hits.Load(), puts.Load())
	}
}

func TestSlowSourceKeepsReplacingUntilAFastGet(t *testing.T) {
	withHedge(t, 30*time.Millisecond, 8)
	withStall(t, 5*time.Second, time.Second, 0, 2)
	object := bytesRepeat(32 << 10)
	var hits atomic.Int32
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := hits.Add(1)
		if r.Header.Get("Range") != "" || r.Header.Get("If-Range") != "" {
			t.Errorf("signed request gained Range %q If-Range %q", r.Header.Get("Range"), r.Header.Get("If-Range"))
		}
		if n < 3 {
			_, _ = w.Write(object[:32])
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
		body, _ := io.ReadAll(r.Body)
		if string(body) != string(object) {
			t.Errorf("destination length=%d", len(body))
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(dest.Close)
	result, err := retryingHandler().Execute(context.Background(), presignedSpec(t, source.URL, dest.URL, int64(len(object))))
	if err != nil {
		t.Fatal(err)
	}
	if result.BytesProcessed != int64(len(object)) || hits.Load() != 3 {
		t.Fatalf("bytes=%d hits=%d", result.BytesProcessed, hits.Load())
	}
}

func TestPutAckStallReplacesAFrozenPeer(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	var written atomic.Int64
	written.Store(8 << 20)
	go watchPutAck(ctx, cancel, &written, 50<<20, 40*time.Millisecond, 3<<20, func() (int64, bool) {
		return 1000, true
	}, nil)
	select {
	case <-ctx.Done():
		if context.Cause(ctx) != errReplaced {
			t.Fatalf("cause=%v", context.Cause(ctx))
		}
	case <-time.After(time.Second):
		t.Fatal("frozen acknowledgements were kept")
	}
}

func TestPutAckProgressAndHalfwayAreKept(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(context.Canceled)
	var written atomic.Int64
	written.Store(8 << 20)
	acked := int64(1000)
	go watchPutAck(ctx, cancel, &written, 50<<20, 30*time.Millisecond, 3<<20, func() (int64, bool) {
		acked += 1 << 20
		return acked, true
	}, nil)
	select {
	case <-ctx.Done():
		if context.Cause(ctx) == errReplaced {
			t.Fatal("replaced while acknowledgements moved")
		}
	case <-time.After(150 * time.Millisecond):
	}

	halfCtx, halfCancel := context.WithCancelCause(context.Background())
	defer halfCancel(context.Canceled)
	var samples atomic.Int32
	go watchPutAck(halfCtx, halfCancel, &written, 50<<20, 30*time.Millisecond, 3<<20, func() (int64, bool) {
		if samples.Add(1) == 1 {
			return 1000, true
		}
		return 30 << 20, true
	}, nil)
	select {
	case <-halfCtx.Done():
		if context.Cause(halfCtx) == errReplaced {
			t.Fatal("replaced after half the object was acknowledged")
		}
	case <-time.After(150 * time.Millisecond):
	}
}

func TestSlowAckRateIsReplaced(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	var written atomic.Int64
	written.Store(8 << 20)
	var n atomic.Int64
	n.Store(1000)
	go watchPutAck(ctx, cancel, &written, 50<<20, 30*time.Millisecond, 3<<20, func() (int64, bool) {
		n.Add(1000)
		return n.Load(), true
	}, nil)
	select {
	case <-ctx.Done():
		if context.Cause(ctx) != errReplaced {
			t.Fatalf("cause=%v", context.Cause(ctx))
		}
	case <-time.After(time.Second):
		t.Fatal("trickle of acknowledgements was kept")
	}
}

func TestSlowUploadIsReplaced(t *testing.T) {
	withHedge(t, 2*time.Second, 8)
	previous := putAckQuiet
	putAckQuiet = 40 * time.Millisecond
	t.Cleanup(func() { putAckQuiet = previous })
	object := bytesRepeat(256 << 10)
	var puts atomic.Int32
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(object)
	}))
	t.Cleanup(source.Close)
	dest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := puts.Add(1)
		if n == 1 {
			time.Sleep(200 * time.Millisecond)
			_, _ = io.Copy(io.Discard, r.Body)
			w.WriteHeader(http.StatusGatewayTimeout)
			return
		}
		body, _ := io.ReadAll(r.Body)
		if len(body) != len(object) {
			t.Errorf("destination length=%d", len(body))
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(dest.Close)
	handler := retryingHandler()
	handler.client = smallSendBufferClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := handler.Execute(ctx, presignedSpec(t, source.URL, dest.URL, int64(len(object))))
	if err != nil {
		t.Fatal(err)
	}
	if result.BytesProcessed != int64(len(object)) || puts.Load() != 2 {
		t.Fatalf("bytes=%d puts=%d", result.BytesProcessed, puts.Load())
	}
}

func smallSendBufferClient(t *testing.T) *http.Client {
	t.Helper()
	dialer := &net.Dialer{Control: func(_, _ string, raw syscall.RawConn) error {
		var sockErr error
		err := raw.Control(func(fd uintptr) {
			sockErr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_SNDBUF, 4096)
		})
		if err != nil {
			return err
		}
		return sockErr
	}}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialContext = dialer.DialContext
	transport.ExpectContinueTimeout = 0
	return &http.Client{Transport: transport}
}

func TestBytesAckedOnTCP(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := ln.Accept()
		if err == nil {
			accepted <- conn
		}
	}()
	client, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	server := <-accepted
	defer server.Close()
	_, _ = client.Write([]byte("abcdef"))
	buf := make([]byte, 6)
	_, _ = io.ReadFull(server, buf)
	deadline := time.Now().Add(time.Second)
	for {
		acked, ok := bytesAcked(client)
		if ok && acked >= 6 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("acked=%d ok=%v", acked, ok)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func bytesRepeat(n int) []byte {
	body := make([]byte, n)
	for i := range body {
		body[i] = byte(i)
	}
	return body
}
