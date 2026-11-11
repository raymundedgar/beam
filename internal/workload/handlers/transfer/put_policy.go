package transfer

import (
	"context"
	"net"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Beam-Network/beam/internal/workload/contracts"
	"golang.org/x/sys/unix"
)

// tcpSnap is one TCP_INFO sample. Fields stay zero when the socket does not expose them.
type tcpSnap struct {
	acked         int64
	rttUs         uint32
	cwnd          uint32
	retrans       uint32
	deliveryRate  uint64
	rwndLimited   uint64
	sndbufLimited uint64
	ok            bool
}

// putPace remembers the last discarded upload's recent rate. A replacement that
// is not faster is kept, and no further socket is discarded for this part.
type putPace struct {
	prev atomic.Int64
	stop atomic.Bool
}

func (p *putPace) notFaster(bytesPerSec int64) bool {
	if p == nil || bytesPerSec < 0 {
		return false
	}
	prev := p.prev.Load()
	if prev > 0 && bytesPerSec <= prev {
		p.stop.Store(true)
		return true
	}
	return false
}

func (p *putPace) discarded(bytesPerSec int64) {
	if p == nil {
		return
	}
	if bytesPerSec < 0 {
		bytesPerSec = 0
	}
	p.prev.Store(bytesPerSec)
}

func (p *putPace) stopped() bool {
	return p != nil && p.stop.Load()
}

// ackWindows is the last-second acknowledgement rate. A discard needs two
// consecutive windows under minRate. The rate is not averaged from the start.
type ackWindows struct {
	mu     sync.Mutex
	at     time.Time
	base   int64
	have   bool
	lows   int
	latest float64
}

func (w *ackWindows) push(now time.Time, acked int64, window time.Duration, minRate float64) (sampled bool, rate float64, discard bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.have {
		w.have = true
		w.at, w.base = now, acked
		return false, 0, false
	}
	elapsed := now.Sub(w.at)
	if elapsed < window || elapsed <= 0 {
		return false, 0, false
	}
	rate = float64(acked-w.base) / elapsed.Seconds()
	if rate < 0 {
		rate = 0
	}
	w.latest = rate
	w.at, w.base = now, acked
	if minRate > 0 && rate < minRate {
		w.lows++
	} else {
		w.lows = 0
	}
	return true, rate, w.lows >= 2
}

func (w *ackWindows) reset(now time.Time, acked int64) {
	w.mu.Lock()
	w.at, w.base, w.have, w.lows = now, acked, true, 0
	w.mu.Unlock()
}

// watchPutRecent cancels only after two consecutive ~1s windows stay under
// minRate. cancel may be nil; samples are still reported. A replacement that
// is not faster than the socket it replaced is kept.
func watchPutRecent(ctx context.Context, cancel context.CancelCauseFunc, total int64, window time.Duration, minRate int64, acked func() (int64, bool), paused func() bool, pace *putPace, onSample func(rate float64, acked int64), onStall func(rate float64, acked int64)) {
	if window <= 0 || acked == nil {
		return
	}
	tick := window / 4
	if tick > 250*time.Millisecond {
		tick = 250 * time.Millisecond
	}
	if tick < time.Millisecond {
		tick = time.Millisecond
	}
	ticker := time.NewTicker(tick)
	defer ticker.Stop()
	var windows ackWindows
	attemptAcked := int64(-1)
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			n, ok := acked()
			if !ok {
				continue
			}
			if attemptAcked < 0 {
				attemptAcked = n
			}
			if paused != nil && paused() {
				windows.reset(now, n)
				continue
			}
			progress := n - attemptAcked
			if total > 0 && (progress >= putAckKeep || pastHalfway(progress, total)) {
				windows.reset(now, n)
				continue
			}
			sampled, rate, discard := windows.push(now, n, window, float64(minRate))
			if !sampled {
				continue
			}
			if onSample != nil {
				onSample(rate, n)
			}
			if !discard || cancel == nil {
				continue
			}
			if pace.notFaster(int64(rate)) {
				continue
			}
			pace.discarded(int64(rate))
			if onStall != nil {
				onStall(rate, n)
			}
			cancel(errReplaced)
			return
		}
	}
}

func tuneUploadSocket(conn net.Conn) {
	if !putPolicy || putSndBuf <= 0 {
		return
	}
	conn = netConn(conn)
	raw, ok := conn.(syscall.Conn)
	if !ok {
		return
	}
	sys, err := raw.SyscallConn()
	if err != nil {
		return
	}
	_ = sys.Control(func(fd uintptr) {
		_ = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_SNDBUF, putSndBuf)
	})
}

func discardUploadSocket(conn net.Conn) {
	conn = netConn(conn)
	if conn == nil {
		return
	}
	if putLinger {
		if tcp, ok := conn.(*net.TCPConn); ok {
			_ = tcp.SetLinger(0)
		}
	}
	_ = conn.Close()
}

func remoteIP(conn net.Conn) string {
	if conn == nil || conn.RemoteAddr() == nil {
		return "-"
	}
	host, _, err := net.SplitHostPort(conn.RemoteAddr().String())
	if err != nil {
		return conn.RemoteAddr().String()
	}
	return host
}

func cfColo(header map[string][]string) string {
	if header == nil {
		return "-"
	}
	ray := ""
	for key, values := range header {
		if strings.EqualFold(key, "Cf-Ray") && len(values) > 0 {
			ray = values[0]
			break
		}
	}
	if ray == "" {
		return "-"
	}
	if i := strings.LastIndex(ray, "-"); i >= 0 && i+1 < len(ray) {
		colo := ray[i+1:]
		if len(colo) > 0 && len(colo) <= 8 {
			return colo
		}
	}
	return "-"
}

// putParallel leaves the current upload running and starts a second one on the
// same bytes. The faster acknowledgement rate is kept. The other socket is
// reset. This is off unless BEAM_WORKER_PUT_PARALLEL_PROBE=1.
func (h *Handler) putParallel(ctx context.Context, part contracts.TransferPart, open bodyOpen, size int64, digest string) (partResult, error) {
	type outcome struct {
		result partResult
		err    error
	}
	ctxA, cancelA := context.WithCancelCause(ctx)
	defer cancelA(context.Canceled)
	var progA atomic.Int64
	chA := make(chan outcome, 1)
	go func() {
		res, err := h.putPresignedOnce(ctxA, part, open, size, digest, false, &progA, nil)
		chA <- outcome{res, err}
	}()
	wait := time.NewTimer(2 * putWindow)
	select {
	case <-ctx.Done():
		cancelA(context.Canceled)
		<-chA
		return partResult{bytes: size}, ctx.Err()
	case out := <-chA:
		wait.Stop()
		return out.result, out.err
	case <-wait.C:
	}
	ctxB, cancelB := context.WithCancelCause(ctx)
	defer cancelB(context.Canceled)
	var progB atomic.Int64
	chB := make(chan outcome, 1)
	go func() {
		res, err := h.putPresignedOnce(ctxB, part, open, size, digest, false, &progB, nil)
		chB <- outcome{res, err}
	}()
	wait.Reset(putWindow)
	select {
	case <-ctx.Done():
		cancelA(context.Canceled)
		cancelB(context.Canceled)
		return partResult{bytes: size}, ctx.Err()
	case out := <-chA:
		cancelB(errReplaced)
		return out.result, out.err
	case out := <-chB:
		cancelA(errReplaced)
		return out.result, out.err
	case <-wait.C:
	}
	rateA, rateB := progA.Load(), progB.Load()
	if rateB > rateA {
		traceFrom(ctx).putProbe(1, rateA, rateB)
		cancelA(errReplaced)
		out := <-chB
		return out.result, out.err
	}
	traceFrom(ctx).putProbe(0, rateA, rateB)
	cancelB(errReplaced)
	out := <-chA
	return out.result, out.err
}

func bucketOf(rawURL string) string {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return "-"
	}
	path := strings.Trim(parsed.Path, "/")
	if path == "" {
		return "-"
	}
	seg, _, _ := strings.Cut(path, "/")
	if seg == "" {
		return "-"
	}
	if len(seg) > 80 {
		seg = seg[:80]
	}
	return seg
}
