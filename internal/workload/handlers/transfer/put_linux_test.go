//go:build linux

package transfer

import (
	"bytes"
	"context"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestSndBufSetDoublesAndLocksBelowAutotune(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	accepted := make(chan net.Conn, 1)
	go func() {
		c, err := ln.Accept()
		if err == nil {
			accepted <- c
		}
	}()
	client, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	srv := <-accepted
	t.Cleanup(func() { _ = srv.Close() })

	raw, err := client.(*net.TCPConn).SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	wmemMax := readIntFile(t, "/proc/sys/net/core/wmem_max")
	var before, after int
	if err := raw.Control(func(fd uintptr) {
		before, _ = unix.GetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_SNDBUF)
		if err := unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_SNDBUF, 1<<20); err != nil {
			t.Errorf("setsockopt: %v", err)
		}
		after, _ = unix.GetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_SNDBUF)
	}); err != nil {
		t.Fatal(err)
	}
	t.Logf("autotune=%d after_1MiB_set=%d wmem_max=%d", before, after, wmemMax)
	want := 2 << 20
	if want > wmemMax*2 {
		want = wmemMax * 2
	}
	if after != want {
		t.Fatalf("SO_SNDBUF=%d, want %d after setting 1 MiB", after, want)
	}
	if before > after {
		t.Logf("locking 1 MiB shrank the buffer from %d to %d", before, after)
	}
}

func TestTailStallKeepsTheUploadUntilEightSeconds(t *testing.T) {
	if !putPolicy {
		t.Skip("discard policy is off")
	}
	object := bytes.Repeat([]byte{3}, 8<<20)
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(object)
	}))
	t.Cleanup(source.Close)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var accepts atomic.Int32
	stopAt := int64(len(object) * 9 / 10)
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf := make([]byte, 32<<10)
		var n int64
		for n < stopAt {
			read, err := r.Body.Read(buf)
			n += int64(read)
			if err != nil {
				return
			}
		}
		<-r.Context().Done()
	})}
	t.Cleanup(func() { _ = srv.Close() })
	go func() { _ = srv.Serve(windowListener{Listener: ln, accepts: &accepts}) }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	spec := presignedSpec(t, source.URL, "http://"+ln.Addr().String(), int64(len(object)))
	spec.Resources.MemoryBytes = bufferedBytes(int64(len(object)))
	_, err = retryingHandler().Execute(ctx, spec)
	if err == nil {
		t.Fatal("the stalled tail completed")
	}
	// One spare upload can open while the first is below the acknowledgement
	// floor. A discard would open a third connection before the 8s tail wait.
	if accepts.Load() > 2 {
		t.Fatalf("accepts=%d, the tail stall opened another upload before 8s", accepts.Load())
	}
}

func TestSmallReceiveWindowHoldsAZeroPeerWindow(t *testing.T) {
	if !putPolicy {
		t.Skip("discard policy is off")
	}
	object := bytes.Repeat([]byte{7}, 2<<20)
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(object)
	}))
	t.Cleanup(source.Close)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var accepts atomic.Int32
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
			return
		case <-time.After(8 * time.Second):
		}
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	})}
	t.Cleanup(func() { _ = srv.Close() })
	go func() { _ = srv.Serve(windowListener{Listener: ln, accepts: &accepts}) }()

	var logs safeLog
	prev := log.Writer()
	log.SetOutput(&logs)
	t.Cleanup(func() { log.SetOutput(prev) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = retryingHandler().Execute(ctx, presignedSpec(t, source.URL, "http://"+ln.Addr().String(), int64(len(object))))
	if err == nil {
		t.Fatal("a window that never drains completed")
	}
	if !strings.Contains(logs.String(), "decision=hold_rwnd") {
		t.Fatalf("a zero peer window was not held\n%s", logs.String())
	}
	if strings.Contains(logs.String(), "transfer put_discard ") {
		t.Fatalf("a zero peer window was discarded\n%s", logs.String())
	}
}

type safeLog struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *safeLog) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *safeLog) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

type windowListener struct {
	net.Listener
	accepts *atomic.Int32
}

func (l windowListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	l.accepts.Add(1)
	if tcp, ok := c.(*net.TCPConn); ok {
		_ = tcp.SetReadBuffer(4096)
	}
	return c, nil
}

func readIntFile(t *testing.T, path string) int {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	n, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil {
		t.Fatal(err)
	}
	return n
}
