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
// The 8:43 AM wave was cancelled on the fifth download, still reading at
// 1–2 MiB/s, while other tasks in that wave finished a download at 26–45.
const maxSourceReplacements = 5

// putReplacements caps how many times a slow upload is discarded. The signed
// request is repeated from the start on a new connection. The cap stops a
// path that is slow every time from restarting until the offer ends with
// nothing delivered. The 10:43 wave's single retry acknowledged about 2 MB
// and then did not move for 22 seconds.
const putReplacements = 4

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
)

// errReplaced closes a download that cannot finish inside sourceBudget.
// The signed GET is repeated. It is not a task result.
var errReplaced = errors.New("slow source connection closed")

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
// not kept. A download that already holds half the object is kept: the 7:43
// wave dropped one at 41 MB of 49 MB, and the replacement took 13 seconds.
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
	if !pastHalfway(moved.Load(), total) && moved.Load() > 0 && rateTooSlow(moved.Load(), total, early, budget*2, minBytes) {
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
		if !pastHalfway(moved.Load(), total) && rateTooSlow(moved.Load(), total, time.Since(started), budget, minBytes) {
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

func pastHalfway(moved, total int64) bool {
	return total > 0 && moved*2 >= total
}

// replaceSlowUpload reports whether a slow upload should be discarded for a
// new connection. A restart needs the probe interval plus time for a fast
// copy. Inside that window the current connection is kept so the bytes
// already sent are not thrown away.
func replaceSlowUpload(ctx context.Context, replaced int) bool {
	if replaced >= putReplacements {
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
func watchPutAck(ctx context.Context, cancel context.CancelCauseFunc, written *atomic.Int64, total int64, quiet time.Duration, minRate int64, acked func() (int64, bool), onStall func(written, acked int64)) {
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
			progress := n - base
			// The body can sit fully queued in a 16 MB window while the peer
			// has stopped acknowledging. The 9:43 upload reached 32 MB
			// acknowledged, then the remainder did not move for 18 seconds.
			// Watching used to stop once the buffer was full.
			frozen := now.Sub(lastMove) >= quiet
			if frozen && total > 0 && written.Load() >= total && progress+(1<<20) < total {
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
