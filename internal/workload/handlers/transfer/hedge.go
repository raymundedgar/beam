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
const maxSourceReplacements = 3

// putReplacements is how many times an upload with frozen acknowledgements is
// sent again. The 5:43 AM wave aborted twice, at about 4 MiB/s of bytes handed
// to the kernel, and the third attempt then took 15 seconds. One replacement
// is the hedge; the attempt after it is left to finish.
const putReplacements = 1

var (
	hedgeAfter    = 1500 * time.Millisecond
	sourceBudget  = 8 * time.Second
	hedgeMinBytes = int64(1 << 20)
	// putAckQuiet is how long TCP acknowledgements may sit still before that
	// upload is sent again. Bytes queued in the send buffer are not progress.
	// Dean and Barroso's hedged requests are for the tail, and they warn that
	// a duplicate adds load when the first request is already well underway,
	// so an upload that has already been acknowledged past halfway is kept.
	putAckQuiet = 4 * time.Second
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
// finish inside budget. A connection that has already delivered bytes and is
// far below that pace is closed at the halfway mark. The check then repeats,
// so a burst followed by a crawl is not kept for the whole object.
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
	if moved.Load() > 0 && rateTooSlow(moved.Load(), total, early, budget*2, minBytes) {
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
		if rateTooSlow(moved.Load(), total, time.Since(started), budget, minBytes) {
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

// watchPutAck cancels an upload whose peer has stopped acknowledging data.
// acked reports TCP bytes_acked. A missing sample is ignored, because judging
// bytes handed to the kernel treats a full send buffer as a fast upload and
// then discards it. Once half the object has been acknowledged, the attempt
// is kept: restarting it costs more than finishing it.
func watchPutAck(ctx context.Context, cancel context.CancelCauseFunc, written *atomic.Int64, total int64, quiet time.Duration, acked func() (int64, bool), onStall func(written, acked int64)) {
	if cancel == nil || quiet <= 0 || acked == nil {
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
	var lastMove time.Time
	haveBase := false
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			if total > 0 && written.Load() >= total {
				return
			}
			n, ok := acked()
			if !ok {
				continue
			}
			if !haveBase {
				haveBase = true
				base = n
				last = n
				lastMove = now
				continue
			}
			if n != last {
				last = n
				lastMove = now
				continue
			}
			if now.Sub(lastMove) < quiet {
				continue
			}
			if total > 0 && n-base >= total/2 {
				continue
			}
			if onStall != nil {
				onStall(written.Load(), n-base)
			}
			cancel(errReplaced)
			return
		}
	}
}
