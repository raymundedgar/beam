package transfer

import (
	"context"
	"net"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unicode"

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
	sndbuf        int
	sndWnd        uint32
	sndMss        uint32
	rtoUs         uint32
	backoff       uint8
	unacked       uint32
	ok            bool
}

// putPace remembers the last discarded upload's recent rate. A replacement that
// is not faster is kept, and no further socket is discarded for this part.
type putPace struct {
	prev         atomic.Int64
	stop         atomic.Bool
	holdDiscards atomic.Int32
	pendingHold  atomic.Int64
	reportHold   atomic.Int64
	sawWnd       atomic.Bool
}

func (p *putPace) holds() int {
	if p == nil {
		return 0
	}
	return int(p.holdDiscards.Load())
}

func (p *putPace) markHold(age time.Duration) {
	if p == nil {
		return
	}
	p.holdDiscards.Add(1)
	p.pendingHold.Store(int64(age))
}

func (p *putPace) takeHold() (time.Duration, bool) {
	if p == nil {
		return 0, false
	}
	age := p.pendingHold.Swap(0)
	if age == 0 {
		return 0, false
	}
	return time.Duration(age), true
}

func (p *putPace) noteWindow() {
	if p != nil {
		p.sawWnd.Store(true)
	}
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

func (w *ackWindows) push(now time.Time, acked int64, window time.Duration, minRate float64) (sampled bool, rate float64, lows int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.have {
		w.have = true
		w.at, w.base = now, acked
		return false, 0, 0
	}
	elapsed := now.Sub(w.at)
	if elapsed < window || elapsed <= 0 {
		return false, 0, 0
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
	return true, rate, w.lows
}

// putSlowStreak is how many consecutive slow seconds discard an upload.
// A socket that has already acknowledged 16 MiB, or half the object, can sit
// through a short pause. Four slow seconds means the rate really stayed low.
func putSlowStreak(progress, total int64) int {
	if total > 0 && (progress >= putAckKeep || pastHalfway(progress, total)) {
		return putAckTailFactor
	}
	return 2
}

// putInTail is the last part of an upload. A zero-ack stall there waits 8s,
// because the 08:09 task had already acknowledged 40 MiB of 43 and the next
// sockets were slower.
func putInTail(progress, total int64) bool {
	if total <= 0 || progress <= 0 {
		return false
	}
	if progress*100 >= total*85 {
		return true
	}
	return total > 4<<20 && total-progress < 4<<20
}

const putTailZero = 8

// tinyPeerWindow is an advertised window that cannot accept a useful burst.
// A zero window and a window under 16 KiB are the peer's receive buffer, so
// a new socket does not move the bytes. putHoldCap is how long that hold
// lasts before the socket is discarded anyway.
const tinyPeerWindow = 16 << 10

// putHoldCap is how long a non-tail upload waits on a closed peer window
// before it is discarded. The tail always uses putHoldTailCap. Set with
// BEAM_WORKER_UL_HOLD_MAX. putHoldDiscardMax is separate from the slow-ack
// discard cap.
var putHoldCap = 25 * time.Second

// putHoldResponseCap is how long a fully written body may sit with a closed
// window waiting for the response. On successful PUTs the last acknowledgement
// was 0.44s before the response at the median and 2.7s at p95. A 4s cap covers
// that body and still drops the 10–12s waits that ended with no status.
var putHoldResponseCap = 4 * time.Second

const putHoldTailCap = 25 * time.Second

const putHoldDiscardMax = 2

func holdLimit(progress, total int64) time.Duration {
	if putInTail(progress, total) || putHoldCap <= 0 {
		return putHoldTailCap
	}
	return putHoldCap
}

func peerWindowBlocked(s tcpSnap) bool {
	if !s.ok {
		return false
	}
	return s.sndWnd < tinyPeerWindow
}

func (w *ackWindows) reset(now time.Time, acked int64) {
	w.mu.Lock()
	w.at, w.base, w.have, w.lows = now, acked, true, 0
	w.mu.Unlock()
}

// watchPutRecent cancels only after consecutive ~1s windows stay under minRate.
// A socket that has already moved 16 MiB needs four slow seconds. Two slow
// seconds discard a socket that has not. A replacement that is not faster than
// the socket it replaced is kept.
func watchPutRecent(ctx context.Context, cancel context.CancelCauseFunc, total int64, window time.Duration, minRate int64, acked func() (int64, bool), snapFn func() tcpSnap, paused func() bool, bodyDone func() bool, pace *putPace, onSample func(rate float64, acked int64), onStall func(rate float64, acked int64, snap tcpSnap, decision string, holdFor time.Duration, progress int64), onLate func(), allowDiscard bool) {
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
	var zeros int
	lateSent := false
	// held counts only ticks where the window is closed and this attempt still
	// has body bytes to send. respHeld counts a fully written body waiting for
	// the response. An open window clears both, including ticks that skip the
	// rate sample.
	var held, respHeld time.Duration
	var prev time.Time
	var loggedHold time.Duration
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
			dt := time.Duration(0)
			if !prev.IsZero() {
				dt = now.Sub(prev)
				if dt < 0 {
					dt = 0
				}
			}
			prev = now
			progress := n - attemptAcked
			var snap tcpSnap
			if snapFn != nil {
				snap = snapFn()
			}
			if snap.ok && !peerWindowBlocked(snap) {
				held = 0
				respHeld = 0
				loggedHold = 0
			} else if snap.ok && peerWindowBlocked(snap) {
				// A closed window with the body still queued counts even when
				// every byte already handed to TCP has been acknowledged.
				sending := (paused == nil || !paused()) && (bodyDone == nil || !bodyDone())
				if bodyDone != nil && bodyDone() {
					held = 0
					loggedHold = 0
					respHeld += dt
					if cancel != nil && respHeld >= putHoldResponseCap && (pace == nil || pace.holds() < putHoldDiscardMax) {
						if pace != nil {
							pace.markHold(respHeld)
						}
						if onStall != nil {
							onStall(0, n, snap, "hold_expired", respHeld, progress)
						}
						cancel(errHoldExpired)
						return
					}
				} else if sending {
					held += dt
					if pace != nil {
						pace.noteWindow()
					}
					limit := holdLimit(progress, total)
					if cancel != nil && held >= limit && (pace == nil || pace.holds() < putHoldDiscardMax) {
						if pace != nil {
							pace.markHold(held)
						}
						if onStall != nil {
							onStall(0, n, snap, "hold_expired", held, progress)
						}
						cancel(errHoldExpired)
						return
					}
					if onStall != nil && held-loggedHold >= time.Second {
						onStall(0, n, snap, "hold_rwnd", held, progress)
						loggedHold = held
					}
				}
				continue
			}
			if paused != nil && paused() {
				windows.reset(now, n)
				continue
			}
			sampled, rate, lows := windows.push(now, n, window, float64(minRate))
			if !sampled {
				continue
			}
			if onSample != nil {
				onSample(rate, n)
			}
			if rate == 0 && snap.ok && snap.unacked == 0 && !putInTail(progress, total) {
				windows.reset(now, n)
				zeros = 0
				continue
			}
			if rate == 0 {
				zeros++
			} else {
				zeros = 0
			}
			if onLate != nil && !lateSent && zeros >= 2 {
				lateSent = true
				onLate()
			}
			stall := lows >= putSlowStreak(progress, total)
			if rate == 0 && putInTail(progress, total) {
				stall = zeros >= putTailZero
			}
			// A late probe keeps this socket up through a 2s silence. A zero
			// window still discards after the hold cap above.
			if onLate != nil && rate == 0 && zeros < putTailZero {
				stall = false
			}
			if !stall || cancel == nil || !allowDiscard {
				continue
			}
			if pace.notFaster(int64(rate)) {
				continue
			}
			pace.discarded(int64(rate))
			if onStall != nil && !peerWindowBlocked(snap) {
				onStall(rate, n, snap, "discard", 0, progress)
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

func ipFamilyOf(host string) string {
	ip := net.ParseIP(host)
	if ip == nil {
		return "-"
	}
	if ip.To4() != nil {
		return "v4"
	}
	return "v6"
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

// putResponseEvidence is the response fields worth sending to Cloudflare for
// an upload that ran longer than slowPutAfter. Request secrets and anything
// that looks like a presigned query are left out.
func putResponseEvidence(header map[string][]string) (ray, fields string) {
	if len(header) == 0 {
		return "-", "headers=-"
	}
	keys := make([]string, 0, len(header))
	for key := range header {
		if skipResponseHeader(key) {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, key := range keys {
		value := sanitizeLogValue(headerValue(header, key))
		if value == "" || strings.Contains(strings.ToLower(value), "x-amz-signature") {
			continue
		}
		if strings.EqualFold(key, "Cf-Ray") {
			ray = value
		}
		if b.Len() > 0 {
			b.WriteByte(' ')
		}
		b.WriteString(logHeaderKey(key))
		b.WriteByte('=')
		b.WriteString(value)
	}
	if ray == "" {
		ray = "-"
	}
	if b.Len() == 0 {
		return ray, "headers=-"
	}
	return ray, b.String()
}

func headerValue(header map[string][]string, name string) string {
	for key, values := range header {
		if strings.EqualFold(key, name) && len(values) > 0 {
			return values[0]
		}
	}
	return ""
}

func skipResponseHeader(key string) bool {
	switch strings.ToLower(key) {
	case "authorization", "proxy-authorization", "cookie", "set-cookie", "x-amz-security-token":
		return true
	default:
		return false
	}
}

func logHeaderKey(key string) string {
	return strings.ToLower(strings.ReplaceAll(key, "-", "_"))
}

func sanitizeLogValue(value string) string {
	value = strings.TrimSpace(value)
	if len(value) > 180 {
		value = value[:180]
	}
	return strings.Map(func(r rune) rune {
		if unicode.IsControl(r) || r == ' ' {
			return '_'
		}
		return r
	}, value)
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

// putParallel leaves the first upload alone unless its first second of
// acknowledgements is under the floor. Only then does it open a second socket
// on the same bytes. The faster of the two is kept. This is off unless
// BEAM_WORKER_PUT_PARALLEL_PROBE=1.
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
		res, err := h.putPresignedOnce(ctxA, part, open, size, digest, false, &progA, nil, nil)
		chA <- outcome{res, err}
	}()
	wait := time.NewTimer(putWindow)
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
	floor := putAckMinRate * int64(putWindow) / int64(time.Second)
	if progA.Load() >= floor {
		out := <-chA
		return out.result, out.err
	}
	baseA := progA.Load()
	ctxB, cancelB := context.WithCancelCause(ctx)
	defer cancelB(context.Canceled)
	var progB atomic.Int64
	chB := make(chan outcome, 1)
	go func() {
		res, err := h.putPresignedOnce(ctxB, part, open, size, digest, false, &progB, nil, nil)
		chB <- outcome{res, err}
	}()
	wait.Reset(putWindow)
	select {
	case <-ctx.Done():
		cancelA(context.Canceled)
		cancelB(context.Canceled)
		<-chA
		<-chB
		return partResult{bytes: size}, ctx.Err()
	case out := <-chA:
		cancelB(errReplaced)
		traceFrom(ctx).putProbe(0, progA.Load()-baseA, progB.Load())
		return out.result, out.err
	case out := <-chB:
		cancelA(errReplaced)
		traceFrom(ctx).putProbe(1, progA.Load()-baseA, progB.Load())
		return out.result, out.err
	case <-wait.C:
	}
	deltaA := progA.Load() - baseA
	deltaB := progB.Load()
	if deltaB > deltaA {
		traceFrom(ctx).putProbe(1, deltaA, deltaB)
		cancelA(errReplaced)
		out := <-chB
		return out.result, out.err
	}
	traceFrom(ctx).putProbe(0, deltaA, deltaB)
	cancelB(errReplaced)
	out := <-chA
	return out.result, out.err
}

// putLate leaves the current upload up and, after two seconds with no
// acknowledgements, starts a second one on the same bytes. The faster socket
// is kept. Off unless BEAM_WORKER_PUT_LATE_PROBE=1.
func (h *Handler) putLate(ctx context.Context, part contracts.TransferPart, open bodyOpen, size int64, digest string) (partResult, error) {
	type outcome struct {
		result partResult
		err    error
	}
	ctxA, cancelA := context.WithCancelCause(ctx)
	defer cancelA(context.Canceled)
	var progA atomic.Int64
	chA := make(chan outcome, 1)
	probe := make(chan struct{}, 1)
	go func() {
		res, err := h.putPresignedOnce(ctxA, part, open, size, digest, true, &progA, nil, func() {
			select {
			case probe <- struct{}{}:
			default:
			}
		})
		chA <- outcome{res, err}
	}()
	select {
	case <-ctx.Done():
		cancelA(context.Canceled)
		<-chA
		return partResult{bytes: size}, ctx.Err()
	case out := <-chA:
		return out.result, out.err
	case <-probe:
	}
	baseA := progA.Load()
	ctxB, cancelB := context.WithCancelCause(ctx)
	defer cancelB(context.Canceled)
	var progB atomic.Int64
	chB := make(chan outcome, 1)
	go func() {
		res, err := h.putPresignedOnce(ctxB, part, open, size, digest, false, &progB, nil, nil)
		chB <- outcome{res, err}
	}()
	wait := time.NewTimer(putWindow)
	defer wait.Stop()
	select {
	case <-ctx.Done():
		cancelA(context.Canceled)
		cancelB(context.Canceled)
		<-chA
		<-chB
		return partResult{bytes: size}, ctx.Err()
	case out := <-chA:
		cancelB(errReplaced)
		traceFrom(ctx).putProbe(0, progA.Load()-baseA, progB.Load())
		return out.result, out.err
	case out := <-chB:
		cancelA(errReplaced)
		traceFrom(ctx).putProbe(1, progA.Load()-baseA, progB.Load())
		return out.result, out.err
	case <-wait.C:
	}
	deltaA := progA.Load() - baseA
	deltaB := progB.Load()
	if deltaB > deltaA {
		traceFrom(ctx).putProbe(1, deltaA, deltaB)
		cancelA(errReplaced)
		out := <-chB
		return out.result, out.err
	}
	traceFrom(ctx).putProbe(0, deltaA, deltaB)
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
