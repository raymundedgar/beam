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

// putReplacements is how many times an upload with frozen acknowledgements is
// sent again. The 5:43 AM wave aborted twice, at about 4 MiB/s of bytes handed
// to the kernel, and the third attempt then took 15 seconds. One replacement
// is the hedge; the attempt after it is left to finish.
const putReplacements = 1

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
	var baseAt time.Time
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
				baseAt = now
				continue
			}
			progress := n - base
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
