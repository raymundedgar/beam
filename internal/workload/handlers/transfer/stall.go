package transfer

import (
	"context"
	"io"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// stallIdle aborts an attempt that has moved no bytes for this long.
	stallIdle = 5 * time.Second
	// stallWindow is one throughput sample. stallStrikes consecutive samples
	// under stallFloor abort the attempt. The floor stays off: a slow transfer
	// that is still moving must be allowed to finish. Aborting it made the
	// worker report a failure before Beam cancelled the offer.
	stallWindow  = 2 * time.Second
	stallFloor   = int64(0)
	stallStrikes = 2
)

// errStalled retries the original request. It is not a task result. The attempt
// continues until it finishes or the offer context ends.
type stallError struct{}

func (stallError) Error() string   { return "transfer stalled" }
func (stallError) Timeout() bool   { return true }
func (stallError) Temporary() bool { return true }

var errStalled error = stallError{}

// blockedReadGap is the smallest pause between body reads that counts as the
// transport blocking. Shorter gaps are the normal copy loop.
const blockedReadGap = 5 * time.Millisecond

// byteCounter counts bytes as they move and reports the first one.
// Gaps between Read calls are time the HTTP stack spent outside this reader,
// which for a memory body is time blocked in the socket write. Time inside
// Read is the underlying source.
type byteCounter struct {
	r        io.Reader
	n        atomic.Int64
	once     sync.Once
	onFirst  func()
	seen     bool
	last     time.Time
	blocked  time.Duration
	maxGap   time.Duration
	readWait time.Duration
}

func (c *byteCounter) Read(p []byte) (int, error) {
	now := time.Now()
	if !c.seen {
		c.seen = true
	} else if gap := now.Sub(c.last); gap >= blockedReadGap {
		c.blocked += gap
		if gap > c.maxGap {
			c.maxGap = gap
		}
	}
	started := time.Now()
	n, err := c.r.Read(p)
	c.readWait += time.Since(started)
	c.last = time.Now()
	if n > 0 {
		c.n.Add(int64(n))
		if c.onFirst != nil {
			c.once.Do(c.onFirst)
		}
	}
	return n, err
}

func (c *byteCounter) blockedFor() time.Duration {
	if c == nil {
		return 0
	}
	return c.blocked
}

func (c *byteCounter) longestGap() time.Duration {
	if c == nil {
		return 0
	}
	return c.maxGap
}

func (c *byteCounter) waitedInRead() time.Duration {
	if c == nil {
		return 0
	}
	return c.readWait
}

// watchStall cancels the returned context with errStalled when read stops
// moving or stays under the throughput floor. Once expected bytes have moved,
// the watch stays quiet so the attempt can wait for the HTTP response. The
// stop function ends the watch.
func watchStall(parent context.Context, read *atomic.Int64, expected *atomic.Int64) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancelCause(parent)
	idle, window, floor, strikesNeeded := stallIdle, stallWindow, stallFloor, stallStrikes
	go func() {
		interval := min(idle, window) / 4
		if interval < time.Millisecond {
			interval = time.Millisecond
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		last := read.Load()
		lastMove := time.Now()
		windowStart := lastMove
		windowBytes := last
		strikes := 0
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				n := read.Load()
				var want int64
				if expected != nil {
					want = expected.Load()
				}
				if want > 0 && n >= want {
					last = n
					lastMove = now
					windowStart = now
					windowBytes = n
					strikes = 0
					continue
				}
				if n != last {
					last = n
					lastMove = now
				} else if now.Sub(lastMove) >= idle {
					cancel(errStalled)
					return
				}
				if now.Sub(windowStart) < window {
					continue
				}
				elapsed := now.Sub(windowStart).Seconds()
				moved := n - windowBytes
				windowStart = now
				windowBytes = n
				if floor > 0 && n > 0 && elapsed > 0 && float64(moved)/elapsed < float64(floor) {
					strikes++
					if strikes >= strikesNeeded {
						cancel(errStalled)
						return
					}
					continue
				}
				strikes = 0
			}
		}
	}()
	return ctx, func() { cancel(context.Canceled) }
}

func stalled(parent, attempt context.Context) bool {
	return parent.Err() == nil && context.Cause(attempt) == errStalled
}

var activeTransfers atomic.Int32

func enterTransfer() int32 { return activeTransfers.Add(1) }

func leaveTransfer() { activeTransfers.Add(-1) }

type transferLog struct {
	workload string
	attempt  string
	started  time.Time
	once     sync.Once
	printf   func(string, ...any)
}

func (t *transferLog) out(format string, args ...any) {
	if t == nil {
		return
	}
	printf := t.printf
	if printf == nil {
		printf = log.Printf
	}
	printf(format, args...)
}

func (t *transferLog) admitted(parts int) {
	t.out("transfer admitted workload=%s attempt=%s parts=%d active=%d", t.workload, t.attempt, parts, activeTransfers.Load())
}

func (t *transferLog) moved(part int, side string) {
	if t == nil {
		return
	}
	t.once.Do(func() {
		t.out("transfer first_byte workload=%s attempt=%s part=%d side=%s after=%s",
			t.workload, t.attempt, part, side, time.Since(t.started).Round(time.Millisecond))
	})
}

func (t *transferLog) stalled(part int, side string, offset int64) {
	t.out("transfer stalled workload=%s attempt=%s part=%d side=%s offset=%d active=%d",
		t.workload, t.attempt, part, side, offset, activeTransfers.Load())
}

func (t *transferLog) partDone(part int, bytes int64) {
	if t == nil {
		return
	}
	dur := time.Since(t.started)
	t.out("transfer part_done workload=%s attempt=%s part=%d bytes=%d after=%s",
		t.workload, t.attempt, part, bytes, dur.Round(time.Millisecond))
	t.pace(part, "total", 0, bytes, dur, 0, 0, 0, 0)
}

// pace records one side of a part. dur is the HTTP exchange. blocked and max_gap
// are pauses between body reads. resp_wait is time until response headers for a
// source, and time from the last body read until the response for a destination.
// read_wait is time spent inside the body Read.
func (t *transferLog) pace(part int, side string, status int, n int64, dur, blocked, maxGap, resp, readWait time.Duration) {
	rate := 0.0
	if dur > 0 && n > 0 {
		rate = float64(n) / dur.Seconds() / (1024 * 1024)
	}
	t.out("transfer pace workload=%s attempt=%s part=%d side=%s status=%d bytes=%d dur=%s rate_mib=%.2f blocked=%s max_gap=%s resp_wait=%s read_wait=%s active=%d",
		t.workload, t.attempt, part, side, status, n,
		dur.Round(time.Millisecond), rate,
		blocked.Round(time.Millisecond), maxGap.Round(time.Millisecond),
		resp.Round(time.Millisecond), readWait.Round(time.Millisecond),
		activeTransfers.Load())
}

type traceKey struct{}

func withTrace(ctx context.Context, trace *transferLog) context.Context {
	return context.WithValue(ctx, traceKey{}, trace)
}

func traceFrom(ctx context.Context) *transferLog {
	trace, _ := ctx.Value(traceKey{}).(*transferLog)
	return trace
}
