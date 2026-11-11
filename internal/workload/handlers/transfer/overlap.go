package transfer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Beam-Network/beam/internal/workload/contracts"
)

func readInto(r io.Reader, buf *followBuffer, total int64, ctx context.Context) ([]byte, error) {
	tmp := make([]byte, 256<<10)
	var got int64
	for got < total {
		if err := ctx.Err(); err != nil {
			return buf.bytes(), err
		}
		n, err := r.Read(tmp)
		if n > 0 {
			if _, werr := buf.Write(tmp[:n]); werr != nil {
				return buf.bytes(), werr
			}
			got += int64(n)
		}
		if err != nil {
			return buf.bytes(), err
		}
		if n == 0 {
			return buf.bytes(), io.ErrNoProgress
		}
	}
	return buf.bytes(), nil
}

// followBuffer is one download attempt. Upload readers block until those bytes
// are written, the download finishes, or the attempt is abandoned. The signed
// PUT cannot resume, so a discarded upload reads this buffer again from the
// start instead of opening another GET.
type followBuffer struct {
	mu   sync.Mutex
	cond *sync.Cond
	buf  []byte
	done bool
	err  error
}

func newFollowBuffer(n int64) *followBuffer {
	capn := int(n)
	if n < 0 || int64(capn) != n {
		capn = 0
	}
	b := &followBuffer{buf: make([]byte, 0, capn)}
	b.cond = sync.NewCond(&b.mu)
	return b
}

func (b *followBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.err != nil {
		return 0, b.err
	}
	if b.done {
		return 0, io.ErrClosedPipe
	}
	b.buf = append(b.buf, p...)
	b.cond.Broadcast()
	return len(p), nil
}

func (b *followBuffer) finish() {
	b.mu.Lock()
	if b.err == nil {
		b.done = true
	}
	b.cond.Broadcast()
	b.mu.Unlock()
}

func (b *followBuffer) fail(err error) {
	if err == nil {
		err = io.ErrClosedPipe
	}
	b.mu.Lock()
	if b.err == nil && !b.done {
		b.err = err
	}
	b.cond.Broadcast()
	b.mu.Unlock()
}

func (b *followBuffer) Len() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return int64(len(b.buf))
}

func (b *followBuffer) bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf
}

func (b *followBuffer) reader(ctx context.Context) *followReader {
	return &followReader{b: b, ctx: ctx}
}

// followReader is one upload's view of a download buffer. waiting is set while
// the next byte has not been downloaded yet, so the acknowledgement watcher
// does not discard the socket for a pause the source caused.
type followReader struct {
	b       *followBuffer
	ctx     context.Context
	off     int
	waiting atomic.Bool
}

func (r *followReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	r.b.mu.Lock()
	defer r.b.mu.Unlock()
	for {
		if r.off < len(r.b.buf) {
			r.waiting.Store(false)
			n := copy(p, r.b.buf[r.off:])
			r.off += n
			return n, nil
		}
		if err := r.b.err; err != nil {
			r.waiting.Store(false)
			return 0, err
		}
		if r.b.done {
			r.waiting.Store(false)
			return 0, io.EOF
		}
		if err := r.ctx.Err(); err != nil {
			r.waiting.Store(false)
			return 0, err
		}
		r.waiting.Store(true)
		stop := context.AfterFunc(r.ctx, func() { r.b.cond.Broadcast() })
		r.b.cond.Wait()
		stop()
	}
}

// overlapReady reports that this download is worth an upload slot. A read the
// watcher is about to discard is not: the upload would only be cancelled, and
// the slot is needed by a slice that can finish.
func overlapReady(buf *followBuffer, started time.Time, total int64) bool {
	n := buf.Len()
	if total > 0 && n >= total {
		return true
	}
	if n <= 0 {
		return false
	}
	if earlyPut {
		// A result can only be reported once the PUT finishes, so start it as
		// soon as a megabyte is buffered, or half a second after the first byte.
		if n >= earlyPutBytes {
			return true
		}
		return n > 0 && time.Since(started) >= earlyPutAfter
	}
	elapsed := time.Since(started)
	if mostlyReceived(n, total) {
		return true
	}
	// The download watcher makes its keep-or-replace decision at hedgeAfter,
	// against sourceBudget. Starting earlier uses a looser test and opens an
	// upload for a read that is about to be discarded.
	if elapsed < hedgeAfter {
		return false
	}
	return !rateTooSlow(n, total, elapsed, sourceBudget, hedgeMinBytes)
}

type putOutcome struct {
	result partResult
	err    error
}

// transferOverlapped uploads a part while its download is still arriving.
// The download slot is released when the read finishes, including when the
// upload is still sending the last bytes. The caller holds that slot.
func (h *Handler) transferOverlapped(ctx context.Context, part contracts.TransferPart) (partResult, error) {
	readCtx, readCancel := context.WithCancel(ctx)
	defer readCancel()
	pub := make(chan *followBuffer, 1)
	type completed struct {
		buf  *followBuffer
		body []byte
		err  error
	}
	doneCh := make(chan completed, 1)
	go func() {
		var last *followBuffer
		body, err := h.readSourcePublish(readCtx, part, func(buf *followBuffer) {
			last = buf
			select {
			case pub <- buf:
			case <-readCtx.Done():
			}
		})
		doneCh <- completed{last, body, err}
	}()

	release := true
	defer func() {
		if release {
			h.releaseSlot()
		}
	}()

	var (
		cur       *followBuffer
		curAt     time.Time
		putCancel context.CancelCauseFunc
		putCh     chan putOutcome
		putBuf    *followBuffer
		early     *putOutcome
		getDone   bool
		getBody   []byte
	)
	stopPut := func() {
		if putCancel == nil {
			return
		}
		putCancel(errReplaced)
		<-putCh
		putCancel = nil
		putCh = nil
		putBuf = nil
	}
	defer stopPut()
	startPut := func(buf *followBuffer) {
		if buf == nil || putBuf == buf {
			return
		}
		stopPut()
		pctx, cancel := context.WithCancelCause(ctx)
		ch := make(chan putOutcome, 1)
		putCancel = cancel
		putCh = ch
		putBuf = buf
		traceFrom(ctx).putStarted()
		go func() {
			res, err := h.putStream(pctx, part, func(c context.Context) (io.Reader, func() bool) {
				reader := buf.reader(c)
				return reader, reader.waiting.Load
			}, part.Length, "")
			ch <- putOutcome{res, err}
		}()
	}
	hashed := func(outcome putOutcome) (partResult, error) {
		if outcome.err != nil {
			return outcome.result, outcome.err
		}
		sum := sha256.Sum256(getBody)
		outcome.result.sha256 = hex.EncodeToString(sum[:])
		putCancel = nil
		putCh = nil
		putBuf = nil
		return outcome.result, nil
	}

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	var done <-chan completed = doneCh
	for {
		var wait <-chan putOutcome
		if putCh != nil {
			wait = putCh
		}
		select {
		case <-ctx.Done():
			return partResult{}, ctx.Err()
		case buf := <-pub:
			if cur != nil && cur != buf {
				cur.fail(errReplaced)
				early = nil
				stopPut()
			}
			if putBuf != buf {
				curAt = time.Now()
			}
			cur = buf
		case g := <-done:
			done = nil
			getDone = true
			getBody = g.body
			h.releaseSlot()
			release = false
			if g.err != nil {
				if cur != nil {
					cur.fail(g.err)
				}
				stopPut()
				if ctx.Err() != nil {
					return partResult{}, ctx.Err()
				}
				return partResult{}, g.err
			}
			if g.buf != nil {
				cur = g.buf
			}
			if early != nil {
				return hashed(*early)
			}
			startPut(cur)
		case <-ticker.C:
			if getDone || cur == nil || putBuf == cur || early != nil {
				continue
			}
			if overlapReady(cur, curAt, part.Length) {
				startPut(cur)
			}
		case outcome := <-wait:
			if putCancel != nil {
				putCancel(context.Canceled)
			}
			putCancel = nil
			putCh = nil
			mine := putBuf
			putBuf = nil
			if ctx.Err() != nil && outcome.err != nil {
				return partResult{}, ctx.Err()
			}
			if outcome.err == nil {
				if getDone && mine == cur {
					return hashed(outcome)
				}
				saved := outcome
				early = &saved
				continue
			}
			if getDone {
				return outcome.result, outcome.err
			}
			if errors.Is(outcome.err, context.Canceled) || errors.Is(outcome.err, errReplaced) {
				continue
			}
			// The download attempt was abandoned and another read is coming.
			// A terminal upload error before the download finishes is still
			// the upload's answer when no later read replaces it.
			if mine == cur {
				saved := outcome
				early = &saved
			}
		}
	}
}
