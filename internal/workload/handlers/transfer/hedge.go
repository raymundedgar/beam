package transfer

import (
	"context"
	"net"
	"time"
)

// hedgeAfter is how long a connection may run before its rate is judged.
// copyBudget is the time one side (the download or the upload) may take and
// still leave room for the other side inside a 10 second offer. hedgeMinBytes
// skips the check for the small objects used in tests and for tiny parts.
var (
	hedgeAfter    = 1500 * time.Millisecond
	copyBudget    = 8 * time.Second
	ackIdle       = 1500 * time.Millisecond
	hedgeMinBytes = int64(1 << 20)
)

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

// watchAck cancels the attempt when acknowledged TCP bytes are too slow.
// Userspace writes are not the signal: they stop once the send buffer is full
// even while the peer is still draining it. The last attempt passes allow
// false and is left to finish. Durations are copies so a test can change the
// package values without racing this goroutine.
func watchAck(ctx context.Context, cancel context.CancelCauseFunc, connReady <-chan net.Conn, total int64, after, idle, budget time.Duration, minBytes int64) {
	if cancel == nil {
		return
	}
	timer := time.NewTimer(after)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return
	case <-timer.C:
	}
	var conn net.Conn
	select {
	case conn = <-connReady:
	case <-ctx.Done():
		return
	case <-time.After(200 * time.Millisecond):
		return
	}
	acked, ok := bytesAcked(conn)
	if !ok {
		return
	}
	if rateTooSlow(acked, total, after, budget, minBytes) {
		cancel(errStalled)
		return
	}
	idleTimer := time.NewTimer(idle)
	defer idleTimer.Stop()
	select {
	case <-ctx.Done():
		return
	case <-idleTimer.C:
	}
	later, ok := bytesAcked(conn)
	if !ok {
		return
	}
	if rateTooSlow(later, total, after+idle, budget, minBytes) {
		cancel(errStalled)
	}
}
