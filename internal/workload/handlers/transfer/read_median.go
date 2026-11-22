package transfer

import (
	"context"
	"net"
	"net/url"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// hostRates keeps the last few completed download rates for each source host.
type hostRates struct {
	mu sync.Mutex
	m  map[string][]float64
}

const hostRateKeep = 8

func (h *hostRates) observe(host string, bytesPerSec float64) {
	if host == "" || host == "-" || bytesPerSec <= 0 || loopback(host) {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.m == nil {
		h.m = make(map[string][]float64)
	}
	rates := append(h.m[host], bytesPerSec)
	if len(rates) > hostRateKeep {
		rates = rates[len(rates)-hostRateKeep:]
	}
	h.m[host] = rates
}

func (h *hostRates) median(host string) float64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	rates := h.m[host]
	if len(rates) == 0 {
		return 0
	}
	cp := append([]float64(nil), rates...)
	sort.Float64s(cp)
	mid := len(cp) / 2
	if len(cp)%2 == 1 {
		return cp[mid]
	}
	return (cp[mid-1] + cp[mid]) / 2
}

var readHosts hostRates

func loopback(host string) bool {
	name, _, err := net.SplitHostPort(host)
	if err != nil {
		name = host
	}
	return name == "localhost" || name == "127.0.0.1" || name == "::1"
}

func hostOf(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Host == "" {
		return "-"
	}
	return parsed.Host
}

// readRestartOverhead is the handshake added to a full re-read. The signed
// GET cannot resume, so a restart pays for the whole object again.
const readRestartOverhead = 200 * time.Millisecond

// readPace counts replacement sockets that were not faster than the one they
// replaced. Two in a row stops further median restarts.
type readPace struct {
	mu      sync.Mutex
	prev    float64
	misses  int
	stopped bool
}

func (p *readPace) done() bool {
	if p == nil {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.stopped
}

func (p *readPace) block(rate float64) bool {
	if p == nil {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped {
		return true
	}
	// Below the keep rate the search must continue. Two slow sockets in a
	// row are not a reason to give up on finding a faster one.
	if rate < float64(readKeepMinRate) {
		p.misses = 0
		p.prev = rate
		return false
	}
	if p.prev > 0 && rate <= p.prev {
		p.misses++
		if p.misses >= 2 {
			p.stopped = true
			return true
		}
	} else {
		p.misses = 0
	}
	p.prev = rate
	return false
}

// readRestartWorth is true when finishing the remainder at the current wire
// rate costs more than 1.5 times a full re-read at the host median. A socket
// that is 95% done stays, because the re-read starts at byte 0.
// readFastEnough reports that this socket is already quick enough to finish
// a part inside the usual batch cut, so replacing it costs more than it saves.
func readFastEnough(moved int64, elapsed time.Duration) bool {
	if readKeepMinRate <= 0 || moved <= 0 || elapsed <= 0 {
		return false
	}
	return float64(moved)/elapsed.Seconds() >= float64(readKeepMinRate)
}

// keepMeasuredRate applies that keep to the 8s probe. A test budget of a
// millisecond still replaces a socket that cannot meet it. The production
// budget is 8s, which is shorter than the batch cut, so a steady 5 MiB/s
// read would otherwise be discarded.
func keepMeasuredRate(budget time.Duration, moved int64, elapsed time.Duration) bool {
	return budget >= time.Second && readFastEnough(moved, elapsed)
}

// medianOpen keeps the median restart available for one try past the cap.
// After the cap, a socket under the keep rate was left to finish: the 01:43
// reads ran at 1.5 MiB/s for 32s. The extra try still stops when a
// replacement is not faster than the socket it replaced.
func medianOpen(replacements int, pace *readPace, frozen bool) bool {
	if frozen || (pace != nil && pace.done()) {
		return false
	}
	return replacements < readRestartCap+1
}

func readRestartWorth(have, total int64, current, median float64) bool {
	if median <= 0 || current <= 0 || total <= 0 || have >= total || have < 0 {
		return false
	}
	remain := float64(total - have)
	finish := remain / current
	again := float64(total)/median + readRestartOverhead.Seconds()
	return finish > readMedianFactor*again
}

func restartFitsLease(ctx context.Context, total int64, median float64) bool {
	deadline, ok := ctx.Deadline()
	if !ok || median <= 0 || total <= 0 {
		return ok || median > 0
	}
	need := time.Duration(float64(total)/median*float64(time.Second)) + readRestartOverhead
	return time.Until(deadline) > need
}

// watchReadMedian cancels a download when a full re-read at the host median
// would finish the part sooner than the current socket. The fixed 8s budget
// still applies separately and stops at 80%. This check does not.
func watchReadMedian(ctx context.Context, cancel context.CancelCauseFunc, moved *atomic.Int64, have func() int64, total int64, host string, pace *readPace) {
	if cancel == nil || !readMedian || host == "" || host == "-" || have == nil {
		return
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	var prev int64
	var prevAt time.Time
	var saw bool
	var notedEmpty bool
	var below int
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		now := time.Now()
		n := moved.Load()
		if !saw {
			prev, prevAt, saw = n, now, true
			continue
		}
		delta := n - prev
		elapsed := now.Sub(prevAt)
		prev, prevAt = n, now
		if delta < 0 {
			delta = 0
		}
		got := have()
		if got >= total || got < hedgeMinBytes || elapsed <= 0 {
			continue
		}
		med := readHosts.median(host)
		if med <= 0 {
			if !notedEmpty {
				notedEmpty = true
				traceFrom(ctx).readMedianEmpty(host)
			}
			continue
		}
		rate := float64(delta) / elapsed.Seconds()
		if readFastEnough(delta, elapsed) {
			below = 0
			traceFrom(ctx).readKeep(host, "rate", got, rate, med, below)
			continue
		}
		below++
		if !readRestartWorth(got, total, rate, med) {
			continue
		}
		if !restartFitsLease(ctx, total, med) {
			traceFrom(ctx).readKeep(host, "lease", got, rate, med, below)
			continue
		}
		if pace != nil && pace.block(rate) {
			traceFrom(ctx).readKeep(host, "thrash", got, rate, med, below)
			continue
		}
		traceFrom(ctx).readRestart(host, got, rate, med, below)
		cancel(errReplaced)
		return
	}
}
