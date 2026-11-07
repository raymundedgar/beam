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

// putReplacements is how many times a slow upload is sent again. The attempt
// after that is left to finish.
const putReplacements = 2

var (
	hedgeAfter    = 1500 * time.Millisecond
	sourceBudget  = 8 * time.Second
	hedgeMinBytes = int64(1 << 20)
	// putProbe is the first time an upload's average rate is judged.
	// putBudget keeps a connection near 4–8 MiB/s. The copies that ran past
	// 20 seconds stayed near 2 MiB/s, and a quiet-reader check closed uploads
	// that were still draining a full send buffer.
	putProbe  = 3 * time.Second
	putBudget = 12 * time.Second
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

// watchPutRate cancels the upload when the bytes handed to the connection
// cannot finish the object inside budget. The average is what matters: a
// reader can pause while the kernel drains a full buffer on a healthy upload.
func watchPutRate(ctx context.Context, cancel context.CancelCauseFunc, moved *atomic.Int64, total int64, probe, budget time.Duration, minBytes int64) {
	watchSourceRate(ctx, cancel, moved, total, probe, budget, minBytes)
}
