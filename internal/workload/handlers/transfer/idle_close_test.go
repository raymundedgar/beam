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

func TestIdleDropCoversALongStarvedUploadThatAlreadySentBytes(t *testing.T) {
	prevRelease, prevAfter := uploadStarveRelease, uploadIdleDropAfter
	uploadStarveRelease, uploadIdleDropAfter = 10*time.Millisecond, 60*time.Millisecond
	t.Cleanup(func() { uploadStarveRelease, uploadIdleDropAfter = prevRelease, prevAfter })

	buf := newFollowBuffer(0)
	reader := buf.reader(context.Background())
	waited := make(chan struct{})
	go func() {
		_, _ = reader.waitUntil(context.Background(), 1, 80*time.Millisecond)
		close(waited)
	}()
	<-waited
	body := &slotBody{fr: reader, lease: &sendLease{takes: 1, held: true}}
	if body.starvedFor() < uploadIdleDropAfter {
		t.Fatalf("starved=%s", body.starvedFor())
	}
	// 15 MiB sent is how far the failing uploads had got before the stall.
	if !body.idleDrop(15 << 20) {
		t.Fatal("an upload starved past the idle limit was not read as an idle close")
	}
	uploadIdleDropAfter = time.Hour
	if body.idleDrop(15 << 20) {
		t.Fatal("a short starve after a large send was read as an idle close")
	}
}

func TestFrozenForIgnoresEmptyFinishedAndFailedBuffers(t *testing.T) {
	buf := newFollowBuffer(8)
	if buf.frozenFor() != 0 {
		t.Fatal("an empty buffer has no upload waiting on it")
	}
	_, _ = buf.Write([]byte("ab"))
	time.Sleep(30 * time.Millisecond)
	if buf.frozenFor() < 20*time.Millisecond {
		t.Fatalf("frozen=%s", buf.frozenFor())
	}
	_, _ = buf.Write([]byte("c"))
	if buf.frozenFor() > 20*time.Millisecond {
		t.Fatalf("a write did not restart the freeze clock: %s", buf.frozenFor())
	}
	buf.finish()
	time.Sleep(30 * time.Millisecond)
	if buf.frozenFor() != 0 {
		t.Fatal("a finished buffer is not frozen")
	}
}

// R2 closed PUTs that waited on a stalled download, after most of a part had
// been sent. That used to fail the part. The part must be sent again from the
// buffer once the download resumes.
func TestPeerClosingAStarvedPutAfterMegabytesIsRetried(t *testing.T) {
	prevBudget, prevAfter := sourceBudget, hedgeAfter
	prevDrop := uploadIdleDropAfter
	sourceBudget, hedgeAfter = time.Hour, time.Hour
	uploadIdleDropAfter = 200 * time.Millisecond
	t.Cleanup(func() {
		sourceBudget, hedgeAfter = prevBudget, prevAfter
		uploadIdleDropAfter = prevDrop
	})

	object := bytes.Repeat([]byte{6}, 3<<20)
	want := sha256.Sum256(object)
	const head = 3 << 19 // 1.5 MiB, past the 1 MiB an idle close used to need
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if _, err := w.Write(object[:head]); err != nil {
			return
		}
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		time.Sleep(1500 * time.Millisecond)
		_, _ = w.Write(object[head:])
	}))
	t.Cleanup(source.Close)

	var puts, dropped atomic.Int32
	dest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := puts.Add(1)
		if n == 1 {
			rc := http.NewResponseController(w)
			chunk := make([]byte, 64<<10)
			total := 0
			for {
				_ = rc.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
				read, err := r.Body.Read(chunk)
				total += read
				if err != nil {
					if total < 1<<20 {
						t.Errorf("first put saw %d bytes before the pause", total)
					}
					conn, _, herr := rc.Hijack()
					if herr == nil {
						dropped.Add(1)
						_ = conn.Close()
					}
					return
				}
			}
		}
		got, err := io.ReadAll(r.Body)
		if err != nil {
			return
		}
		if sha256.Sum256(got) != want {
			t.Errorf("retried put bytes=%d", len(got))
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
		t.Fatalf("part failed after the peer dropped an idle upload: %v", err)
	}
	if result.BytesProcessed != int64(len(object)) {
		t.Fatalf("bytes=%d", result.BytesProcessed)
	}
	if dropped.Load() != 1 || puts.Load() != 2 {
		t.Fatalf("dropped=%d puts=%d", dropped.Load(), puts.Load())
	}
}

// A download that an upload is following stops being replaced once its buffer
// has been still for readFreezeMax. Every replacement re-reads the prefix, so
// continuing to replace a slow but moving socket froze the upload until the
// peer dropped it. Here the replacement is as slow as the first socket, so it
// can never get past the prefix inside the 2s the median check gives it.
func TestFrozenBufferStopsReplacingTheDownload(t *testing.T) {
	prevBudget, prevAfter := sourceBudget, hedgeAfter
	prevFreeze, prevCap, prevMedian, prevKeep := readFreezeMax, readRestartCap, readMedian, readKeepMinRate
	sourceBudget, hedgeAfter = time.Hour, time.Hour
	readFreezeMax, readRestartCap, readMedian, readKeepMinRate = time.Millisecond, 3, true, 64<<20
	t.Cleanup(func() {
		sourceBudget, hedgeAfter = prevBudget, prevAfter
		readFreezeMax, readRestartCap, readMedian, readKeepMinRate = prevFreeze, prevCap, prevMedian, prevKeep
	})

	object := bytes.Repeat([]byte{3}, 12<<20)
	var gets atomic.Int32
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		delay := 15 * time.Millisecond // about 4 MiB/s
		if gets.Add(1) > 1 {
			delay = 20 * time.Millisecond // a replacement that is no faster
		}
		flusher, _ := w.(http.Flusher)
		const step = 64 << 10
		for off := 0; off < len(object); off += step {
			if r.Context().Err() != nil {
				return
			}
			if _, err := w.Write(object[off : off+step]); err != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
			time.Sleep(delay)
		}
	}))
	t.Cleanup(source.Close)
	host := hostOf(source.URL)
	readHosts.mu.Lock()
	saved := readHosts.m
	readHosts.m = map[string][]float64{host: {200 << 20}}
	readHosts.mu.Unlock()
	t.Cleanup(func() {
		readHosts.mu.Lock()
		readHosts.m = saved
		readHosts.mu.Unlock()
	})

	dest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(dest.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	spec := presignedSpec(t, source.URL, dest.URL, int64(len(object)))
	spec.Resources.MemoryBytes = bufferedBytes(int64(len(object)))
	if _, err := retryingHandler().Execute(ctx, spec); err != nil {
		t.Fatal(err)
	}
	// Without the guard every allowed restart is used and one more GET finishes.
	if got := gets.Load(); got > 3 {
		t.Fatalf("gets=%d, the download was still being replaced after its buffer froze", got)
	}
}
