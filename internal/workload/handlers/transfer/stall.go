package transfer

import (
	"context"
	"io"
	"log"
	"strings"
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
	workload    string
	attempt     string
	batch       string
	started     time.Time
	once        sync.Once
	printf      func(string, ...any)
	mu          sync.Mutex
	firstAt     time.Time
	putAt       time.Time
	ackAt       time.Time
	slotWait    time.Duration
	slotStarved time.Duration
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
		t.mu.Lock()
		t.firstAt = time.Now()
		t.mu.Unlock()
		t.out("transfer first_byte workload=%s attempt=%s part=%d side=%s after=%s",
			t.workload, t.attempt, part, side, time.Since(t.started).Round(time.Millisecond))
	})
}

func (t *transferLog) firstAck() {
	if t == nil {
		return
	}
	t.mu.Lock()
	if t.ackAt.IsZero() {
		t.ackAt = time.Now()
	}
	t.mu.Unlock()
}

func (t *transferLog) putStarted() {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.putAt.IsZero() {
		t.putAt = time.Now()
	}
}

func (t *transferLog) putSample(upload, part string, bytesPerSec float64, acked int64) {
	if t == nil {
		return
	}
	t.out("transfer put_sample workload=%s attempt=%s upload=%s part_no=%s acked=%d rate_1s=%.2f",
		t.workload, t.attempt, upload, part, acked, bytesPerSec/(1024*1024))
}

func (t *transferLog) holdExpired(upload, part string, age time.Duration, status int, rate float64, dur time.Duration, wnd bool) {
	if t == nil {
		return
	}
	// status, rate, and dur are the attempt that ran after the hold expired.
	t.out("transfer hold_expired workload=%s attempt=%s upload=%s part_no=%s hold_age_ms=%d status=%d rate_mib=%.2f dur_ms=%d wnd0=%t build=%s",
		t.workload, t.attempt, upload, part, age.Milliseconds(), status, rate, dur.Milliseconds(), wnd, buildID)
}

func (t *transferLog) putDiscard(upload, part, reason string, bytesPerSec float64, acked int64, snap tcpSnap) {
	if t == nil {
		return
	}
	t.out("transfer put_discard workload=%s attempt=%s upload=%s part_no=%s reason=%s acked=%d rate_1s=%.2f snd_wnd=%d rto_us=%d backoff=%d unacked=%d",
		t.workload, t.attempt, upload, part, reason, acked, bytesPerSec/(1024*1024), snap.sndWnd, snap.rtoUs, snap.backoff, snap.unacked)
}

func (t *transferLog) putWindow(upload, part string, snap tcpSnap, bytesPerSec float64, decision string, progress, total int64, holdFor time.Duration) {
	if t == nil {
		return
	}
	t.out("transfer put_window workload=%s attempt=%s upload=%s part_no=%s snd_wnd=%d rto_us=%d backoff=%d unacked=%d rate_1s=%.2f decision=%s progress=%d total=%d hold_age=%s build=%s",
		t.workload, t.attempt, upload, part, snap.sndWnd, snap.rtoUs, snap.backoff, snap.unacked, bytesPerSec/(1024*1024), decision, progress, total, holdFor.Round(time.Millisecond), buildID)
}

func (t *transferLog) readRestart(host string, have int64, bytesPerSec, median float64, below int) {
	if t == nil {
		return
	}
	t.out("transfer read_restart workload=%s attempt=%s host=%s have=%d rate_1s=%.2f median=%.2f keep_mib=%d below=%d build=%s",
		t.workload, t.attempt, host, have, bytesPerSec/(1024*1024), median/(1024*1024), readKeepMinRate>>20, below, buildID)
}

func (t *transferLog) readKeep(host, reason string, have int64, bytesPerSec, median float64, below int) {
	if t == nil {
		return
	}
	t.out("transfer read_keep workload=%s attempt=%s host=%s reason=%s have=%d rate_1s=%.2f median=%.2f keep_mib=%d below=%d build=%s",
		t.workload, t.attempt, host, reason, have, bytesPerSec/(1024*1024), median/(1024*1024), readKeepMinRate>>20, below, buildID)
}

func (t *transferLog) readFrozen(have int64, idle time.Duration) {
	if t == nil {
		return
	}
	t.out("transfer read_frozen workload=%s attempt=%s have=%d idle=%s build=%s",
		t.workload, t.attempt, have, idle.Round(time.Millisecond), buildID)
}

func (t *transferLog) readMedianEmpty(host string) {
	if t == nil {
		return
	}
	t.out("transfer read_median workload=%s attempt=%s host=%s samples=0 build=%s",
		t.workload, t.attempt, host, buildID)
}

func (t *transferLog) addSlotWait(d time.Duration) {
	if t == nil || d < 0 {
		return
	}
	t.mu.Lock()
	t.slotWait += d
	t.mu.Unlock()
}

func (t *transferLog) addSlotStarved(d time.Duration) {
	if t == nil || d <= 0 {
		return
	}
	t.mu.Lock()
	t.slotStarved += d
	t.mu.Unlock()
}

func (t *transferLog) readRetry(n int64) {
	if t == nil {
		return
	}
	t.out("transfer read_retry workload=%s attempt=%s bytes=%d build=%s", t.workload, t.attempt, n, buildID)
}

func (t *transferLog) sourceReset(reason string) {
	if t == nil {
		return
	}
	t.out("transfer reset workload=%s attempt=%s reason=%s build=%s", t.workload, t.attempt, reason, buildID)
}

func (t *transferLog) etagMismatch(upload, part, etag, md5sum string) {
	if t == nil {
		return
	}
	t.out("ERROR transfer etag_mismatch workload=%s attempt=%s upload=%s part_no=%s etag=%s md5=%s",
		t.workload, t.attempt, upload, part, etag, md5sum)
}

func (t *transferLog) readPeer(remote, family, colo string, n int64, dur time.Duration) {
	if t == nil {
		return
	}
	rate := 0.0
	if dur > 0 {
		rate = float64(n) / dur.Seconds() / (1024 * 1024)
	}
	t.out("transfer read_peer workload=%s attempt=%s remote=%s family=%s colo=%s bytes=%d dur=%s rate_mib=%.2f",
		t.workload, t.attempt, remote, family, colo, n, dur.Round(time.Millisecond), rate)
}

func (t *transferLog) readSkip(n int64) {
	if t == nil || n <= 0 {
		return
	}
	t.out("transfer read_skip workload=%s attempt=%s bytes=%d build=%s", t.workload, t.attempt, n, buildID)
}

func (t *transferLog) uploadWait(waited time.Duration, holders string) {
	if t == nil {
		return
	}
	t.out("transfer upload_wait workload=%s attempt=%s batch=%s waited=%s holders=%s",
		t.workload, t.attempt, t.batch, waited.Round(time.Millisecond), holders)
}

func (t *transferLog) idleClosed(upload, part string) {
	if t == nil {
		return
	}
	t.out("transfer put_idle_closed workload=%s attempt=%s upload=%s part_no=%s reason=idle_closed build=%s",
		t.workload, t.attempt, upload, part, buildID)
}

func (t *transferLog) putSlow(bucket, upload, part string, dur time.Duration, status int, header map[string][]string) {
	if t == nil || dur < slowPutAfter {
		return
	}
	ray, fields := putResponseEvidence(header)
	t.out("transfer put_slow workload=%s attempt=%s dur=%s status=%d bucket=%s upload=%s part_no=%s cf_ray=%s %s",
		t.workload, t.attempt, dur.Round(time.Millisecond), status, bucket, upload, part, ray, fields)
}

func (t *transferLog) putAttempt(remote, colo, bucket, upload, part, reason string, snap tcpSnap, acked int64, starved time.Duration) {
	if t == nil {
		return
	}
	t.out("transfer put_attempt workload=%s attempt=%s remote=%s colo=%s rtt_us=%d cwnd=%d retrans=%d delivery_rate=%d rwnd_limited_us=%d sndbuf_limited_us=%d sndbuf=%d acked=%d slot_starved=%s bucket=%s upload=%s part_no=%s reason=%s",
		t.workload, t.attempt, remote, colo, snap.rttUs, snap.cwnd, snap.retrans, snap.deliveryRate,
		snap.rwndLimited, snap.sndbufLimited, snap.sndbuf, acked, starved.Round(time.Millisecond), bucket, upload, part, reason)
}

func (t *transferLog) putProbe(kept int, rateA, rateB int64) {
	t.out("transfer put_probe workload=%s attempt=%s kept=%d rate0=%d rate1=%d",
		t.workload, t.attempt, kept, rateA, rateB)
}

func (t *transferLog) readRace(winner int, lens []int64) {
	if t == nil {
		return
	}
	t.out("transfer read_race workload=%s attempt=%s kept=%d bytes=%v", t.workload, t.attempt, winner, lens)
}

func (t *transferLog) hedgeRead(kept int, first, second int64) {
	t.out("transfer hedge_read workload=%s attempt=%s kept=%d bytes0=%d bytes1=%d",
		t.workload, t.attempt, kept, first, second)
}

func (t *transferLog) reported(batch string, leader, cut bool) {
	if t == nil {
		return
	}
	t.mu.Lock()
	first, putAt, ackAt := t.firstAt, t.putAt, t.ackAt
	slotWait, slotStarved := t.slotWait, t.slotStarved
	t.mu.Unlock()
	ttfb, putStart, ack := time.Duration(-1), time.Duration(-1), time.Duration(-1)
	if !first.IsZero() {
		ttfb = first.Sub(t.started)
	}
	if !putAt.IsZero() {
		putStart = putAt.Sub(t.started)
	}
	if !ackAt.IsZero() {
		ack = ackAt.Sub(t.started)
	}
	if batch == "" {
		batch = "-"
	}
	t.out("transfer report workload=%s attempt=%s batch=%s first_deliverer=%t ttfb=%s put_start=%s ack=%s result=%s cut=%t slot_wait=%s slot_starved=%s %s",
		t.workload, t.attempt, batch, leader,
		reportDur(ttfb), reportDur(putStart), reportDur(ack), time.Since(t.started).Round(time.Millisecond), cut,
		slotWait.Round(time.Millisecond), slotStarved.Round(time.Millisecond), policyStamp())
}

func reportDur(d time.Duration) string {
	if d < 0 {
		return "-"
	}
	return d.Round(time.Millisecond).String()
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
	t.pace(part, "total", 0, bytes, dur, 0, 0, 0, 0, "-")
}

func regionLabel(region string) string {
	region = strings.TrimSpace(strings.ToLower(region))
	if region == "" || len(region) > 8 {
		return "-"
	}
	for _, r := range region {
		if r < 'a' || r > 'z' {
			return "-"
		}
	}
	return region
}

// pace records one side of a part. dur is the HTTP exchange. blocked and max_gap
// are pauses between body reads. resp_wait is time until response headers for a
// source, and time from the last body read until the response for a destination.
// read_wait is time spent inside the body Read.
func (t *transferLog) pace(part int, side string, status int, n int64, dur, blocked, maxGap, resp, readWait time.Duration, region string) {
	rate := 0.0
	if dur > 0 && n > 0 {
		rate = float64(n) / dur.Seconds() / (1024 * 1024)
	}
	t.out("transfer pace workload=%s attempt=%s part=%d side=%s status=%d bytes=%d dur=%s rate_mib=%.2f blocked=%s max_gap=%s resp_wait=%s read_wait=%s region=%s active=%d",
		t.workload, t.attempt, part, side, status, n,
		dur.Round(time.Millisecond), rate,
		blocked.Round(time.Millisecond), maxGap.Round(time.Millisecond),
		resp.Round(time.Millisecond), readWait.Round(time.Millisecond),
		regionLabel(region), activeTransfers.Load())
}

// pacePut is the destination line plus TCP acknowledgement evidence.
// acked is bytes_acked since the upload was first sampled. known is false
// when the socket does not expose it. ackAge is how long that count has
// been unchanged. upload is a short hash of the multipart upload id, and
// partNo is the part number from the signed URL. The signed query is not logged.
func (t *transferLog) pacePut(part int, status int, n int64, dur, blocked, maxGap, resp, readWait time.Duration, acked int64, known bool, ackAge time.Duration, upload, partNo, region string) {
	if !known {
		acked = -1
		ackAge = 0
	}
	rate := 0.0
	if dur > 0 && n > 0 {
		rate = float64(n) / dur.Seconds() / (1024 * 1024)
	}
	ackRate := 0.0
	if known && dur > 0 && acked > 0 {
		ackRate = float64(acked) / dur.Seconds() / (1024 * 1024)
	}
	t.out("transfer pace workload=%s attempt=%s part=%d side=%s status=%d bytes=%d dur=%s rate_mib=%.2f blocked=%s max_gap=%s resp_wait=%s read_wait=%s acked=%d ack_mib=%.2f ack_age=%s upload=%s part_no=%s region=%s active=%d",
		t.workload, t.attempt, part, "destination", status, n,
		dur.Round(time.Millisecond), rate,
		blocked.Round(time.Millisecond), maxGap.Round(time.Millisecond),
		resp.Round(time.Millisecond), readWait.Round(time.Millisecond),
		acked, ackRate, ackAge.Round(time.Millisecond), upload, partNo, regionLabel(region),
		activeTransfers.Load())
}

func (t *transferLog) replaced(part int, side, reason string, written, acked int64) {
	if t == nil {
		return
	}
	t.out("transfer replace workload=%s attempt=%s part=%d side=%s reason=%s written=%d acked=%d active=%d",
		t.workload, t.attempt, part, side, reason, written, acked, activeTransfers.Load())
}

type traceKey struct{}

func withTrace(ctx context.Context, trace *transferLog) context.Context {
	return context.WithValue(ctx, traceKey{}, trace)
}

func traceFrom(ctx context.Context) *transferLog {
	trace, _ := ctx.Value(traceKey{}).(*transferLog)
	return trace
}
