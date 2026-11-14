package transfer

import (
	"context"
	"errors"
	"sync/atomic"
	"time"
)

// hedgeAfter is how long a download may run before its rate is judged.
// sourceBudget is the time that download may take. Eight seconds keeps a
// connection near 8–12 MiB/s; four seconds discarded one of those and the
// replacement then took the rest of the offer. hedgeMinBytes skips the check
// for tiny parts and tests.
// maxSourceReplacements caps how many slow connections are discarded before
// the last one is allowed to finish. Eight replacements per task, judged at
// 800ms, opened connections that had not sent a byte and slowed the whole wave.
// The 12:37 AM wave was cancelled on the sixth download, still reading at
// 0.5–2 MiB/s, while other tasks in that wave finished a download at 14–39.
// maxSourceReplacements is how many times the 1.5s probe may discard a slow
// download. BEAM_WORKER_READ_PROBE_REPLACEMENTS overrides it. The median
// restart cap is separate and is usually higher.
var maxSourceReplacements = 6

// readFreezeMax is how long a download that an upload is following may go
// without buffering a new byte before it stops being replaced. A replacement
// repeats the signed GET from byte 0 and the buffer cannot grow until it has
// re-read the bytes already held, so each replacement freezes the upload for
// that whole prefix. Replacing a moving socket over and over starved the
// uploads of 14-19s, long enough for R2 to drop them, and the part failed.
// Past this point the current socket is left to finish.
var readFreezeMax = 3 * time.Second

// putReplacements caps how many times a slow upload is discarded. The signed
// request is repeated from the start on a new connection. The cap stops a
// path that is slow every time from restarting until the offer ends with
// nothing delivered. The 12:43 AM wave used all four discards, and the upload
// that remained was still frozen for 14 seconds.
const putReplacements = 5

var (
	hedgeAfter    = 1500 * time.Millisecond
	sourceBudget  = 8 * time.Second
	hedgeMinBytes = int64(1 << 20)
	// putAckQuiet is how long acknowledgements are observed before the rate is
	// judged. putAckMinRate is the slowest acknowledged rate that is kept.
	// Four seconds used up the 8:43 AM take-back: the slow uploads had only
	// acknowledged about 3 MB, and the replacement was cancelled two seconds
	// later. Uploads that finished in that wave did so in under four seconds.
	// An upload that has already acknowledged putAckKeep is kept.
	putAckQuiet   = 2 * time.Second
	putAckMinRate = int64(3 << 20)
	// slowPutAfter is how long an upload runs before its response headers are
	// logged. The ray and request id are what Cloudflare can trace.
	slowPutAfter = 10 * time.Second
)

// errReplaced closes a download that cannot finish inside sourceBudget.
// The signed GET is repeated. It is not a task result.
var errReplaced = errors.New("slow source connection closed")

// errHoldExpired discards an upload whose peer window stayed closed. It is
// not one of the slow-ack discards, and the part is sent again from the buffer.
var errHoldExpired = errors.New("upload hold expired")

// errSourceReset aborts the upload because the download was not a valid
// continuation. It is not an upload discard and does not use the discard cap.
var errSourceReset = errors.New("source reset")

// errReadRetry repeats the signed GET after a valid 206 dropped mid-body.
// The buffered prefix stays, and the upload keeps waiting on it.
var errReadRetry = errors.New("read retry")

// errETagMismatch is a part ETag that is not the MD5 of the bytes sent.
var errETagMismatch = errors.New("etag mismatch")

// errIdleClosed is an early PUT the peer dropped while the reader was waiting
// on the download. The part is sent again from the first buffered byte and the
// attempt does not use a discard.
var errIdleClosed = errors.New("idle upload closed")

// tooSlow reports that moved bytes cannot reach total inside budget at the
// rate observed over elapsed. A finished body is never slow.
func tooSlow(moved, total int64, elapsed, budget time.Duration) bool {
	return rateTooSlow(moved, total, elapsed, budget, hedgeMinBytes)
}

func rateTooSlow(moved, total int64, elapsed, budget time.Duration, minBytes int64) bool {
	if total < minBytes || budget <= 0 || elapsed <= 0 {
		return false
	}
	if moved >= total {
		return false
	}
	need := float64(total) / budget.Seconds()
	rate := float64(moved) / elapsed.Seconds()
	return rate < need
}

// watchSourceRate cancels the download when its average rate can no longer
// finish inside budget. The check repeats, so a burst followed by a crawl is
// not kept. A download that already holds about 80% of the object is kept.
func watchSourceRate(ctx context.Context, cancel context.CancelCauseFunc, moved *atomic.Int64, total int64, probe, budget time.Duration, minBytes int64) {
	if cancel == nil || probe <= 0 {
		return
	}
	early := probe / 2
	if early < time.Millisecond {
		early = probe
	}
	timer := time.NewTimer(early)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return
	case <-timer.C:
	}
	if !keepMeasuredRate(budget, moved.Load(), early) && !mostlyReceived(moved.Load(), total) && moved.Load() > 0 && rateTooSlow(moved.Load(), total, early, budget*2, minBytes) {
		cancel(errReplaced)
		return
	}
	if early < probe {
		rest := time.NewTimer(probe - early)
		select {
		case <-ctx.Done():
			rest.Stop()
			return
		case <-rest.C:
		}
	}
	interval := probe
	if interval > 500*time.Millisecond {
		interval = 500 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	started := time.Now().Add(-probe)
	for {
		elapsed := time.Since(started)
		if !keepMeasuredRate(budget, moved.Load(), elapsed) && !mostlyReceived(moved.Load(), total) && rateTooSlow(moved.Load(), total, elapsed, budget, minBytes) {
			cancel(errReplaced)
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// putAckKeep is how much acknowledged data is worth more than a restart.
// The 7:43 wave aborted an upload after 21 MB had been acknowledged, and the
// new connection then took 19 seconds.
const putAckKeep = 16 << 20

// putAckTailFactor is how much longer a proven upload may sit quiet before
// its last unacknowledged window is discarded. Two seconds is enough to drop
// a socket that has barely started. The same two seconds also discarded
// connections that had already acknowledged about 30 MB at 8 MiB/s; the
// replacements then took 33 seconds. A proven upload has to be quiet for
// four probe intervals, which is 8 seconds at the production probe.
const putAckTailFactor = 4

func pastHalfway(moved, total int64) bool {
	return total > 0 && moved*2 >= total
}

// mostlyReceived is the download keep line. Half of a 50 MB object at 3 MiB/s
// still has about 8 seconds left, and the 12:43 AM wave kept those reads for
// 14–16 seconds. Another download from the same region finished at 33 MiB/s.
// Eighty percent still keeps the 7:43 case that was already at 41 MB of 49.
func mostlyReceived(moved, total int64) bool {
	return total > 0 && moved*5 >= total*4
}

// replaceSlowUpload reports whether a slow upload should be discarded for a
// new connection. A restart needs the probe interval plus time for a fast
// copy. Inside that window the current connection is kept so the bytes
// already sent are not thrown away.
func putDiscardCap() int {
	if !putPolicy {
		return putReplacements
	}
	if putMaxDiscards < 0 {
		return 2
	}
	return putMaxDiscards
}

func replaceSlowUpload(ctx context.Context, replaced int) bool {
	if replaced >= putDiscardCap() {
		return false
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return true
	}
	return time.Until(deadline) >= putAckQuiet+2*time.Second
}

// watchPutAck cancels an upload whose acknowledgements cannot finish the
// object. acked reports TCP bytes_acked. A nil cancel only records samples,
// so the final attempt is still visible in the pace line. An upload that has
// already acknowledged putAckKeep, or half the object, is kept.
func watchPutAck(ctx context.Context, cancel context.CancelCauseFunc, written *atomic.Int64, total int64, quiet time.Duration, minRate int64, acked func() (int64, bool), onStall func(written, acked int64), paused func() bool) {
	if quiet <= 0 || acked == nil {
		return
	}
	interval := quiet / 8
	if interval > 500*time.Millisecond {
		interval = 500 * time.Millisecond
	}
	if interval < time.Millisecond {
		interval = time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	var base int64
	var last int64
	var baseAt time.Time
	var lastMove time.Time
	haveBase := false
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			n, ok := acked()
			if !ok {
				continue
			}
			if !haveBase {
				haveBase = true
				base, last = n, n
				baseAt, lastMove = now, now
				continue
			}
			if n != last {
				last = n
				lastMove = now
			}
			// An overlapped upload stops sending while the download has not
			// produced the next bytes. That gap is not a slow socket.
			if paused != nil && paused() {
				lastMove = now
				base, baseAt = n, now
				continue
			}
			progress := n - base
			// The body can sit fully queued in a 16 MB window while the peer
			// has stopped acknowledging. The 9:43 upload reached 32 MB
			// acknowledged, then the remainder did not move for 18 seconds.
			// Watching used to stop once the buffer was full. A connection
			// that has already passed the keep line is allowed a longer pause:
			// the 4:37 wave threw away 30 MB acknowledged at 8 MiB/s after a
			// two second gap, and the new sockets were slower.
			silence := now.Sub(lastMove)
			tailQuiet := quiet
			if progress >= putAckKeep || pastHalfway(progress, total) {
				tailQuiet = quiet * putAckTailFactor
			}
			if silence >= tailQuiet && total > 0 && written.Load() >= total && progress+(1<<20) < total {
				if onStall != nil {
					onStall(written.Load(), progress)
				}
				if cancel != nil {
					cancel(errReplaced)
				}
				return
			}
			if progress >= putAckKeep || pastHalfway(progress, total) {
				continue
			}
			elapsed := now.Sub(baseAt)
			if elapsed < quiet || elapsed <= 0 || progress >= total {
				continue
			}
			if minRate > 0 && float64(progress)/elapsed.Seconds() < float64(minRate) {
				if onStall != nil {
					onStall(written.Load(), progress)
				}
				if cancel != nil {
					cancel(errReplaced)
				}
				return
			}
		}
	}
}
