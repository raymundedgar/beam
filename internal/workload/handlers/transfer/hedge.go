package transfer

import (
	"context"
	"errors"
	"net"
	"time"
)

// hedgeAfter is how long a download may run before its rate is judged.
// sourceBudget is the time that download may take and still leave the upload
// inside Beam's cut. hedgeMinBytes skips the check for tiny parts and tests.
// maxSourceReplacements caps how many slow connections are discarded before
// the last one is allowed to finish.
const maxSourceReplacements = 8

var (
	hedgeAfter    = 800 * time.Millisecond
	sourceBudget  = 4 * time.Second
	hedgeMinBytes = int64(1 << 20)
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

// watchAck cancels the attempt when acknowledged TCP bytes stop increasing.
// A low average rate is not enough: at 1.5 seconds a live upload can already
// have 20 MiB in the send buffer while acknowledgements lag, and closing it
// restarts the body under Beam's deadline. The last attempt does not call
// this. Durations are arguments so a test can change the package values
// without racing this goroutine.
func watchAck(ctx context.Context, cancel context.CancelCauseFunc, connReady <-chan net.Conn, total int64, after, idle time.Duration) {
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
	if !ok || acked >= total {
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
	if !ok || later >= total {
		return
	}
	if later-acked < 1<<20 {
		cancel(errStalled)
	}
}
