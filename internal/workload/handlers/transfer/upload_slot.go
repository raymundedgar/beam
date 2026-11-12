package transfer

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
)

// A sending PUT holds one token. After it has been starved for
// uploadStarveRelease it gives the token back. It takes one again only when
// uploadReacquireBytes are buffered ahead of the reader, or the download has
// finished, so a trickle does not take and return the token on every chunk.
var (
	uploadStarveRelease  = 400 * time.Millisecond
	uploadReacquireBytes = int64(1 << 20)
	uploadWaitLog        = time.Second
)

// uploadGate is the send-token channel plus the cap on open PUT sockets.
// send is copied from the handler when the gate is built for tests that do
// not have one. A handler's uploads field is read at acquire time so a test
// can replace the channel.
type uploadGate struct {
	h    *Handler
	send chan struct{}
	open chan struct{}
	mu   sync.Mutex
	live []*sendLease
}

func newUploadGate(send, open chan struct{}) *uploadGate {
	if open == nil {
		open = make(chan struct{}, defaultTransferSlots)
	}
	return &uploadGate{send: send, open: open}
}

func (g *uploadGate) sendCh() chan struct{} {
	if g == nil {
		return nil
	}
	if g.h != nil {
		return g.h.uploads
	}
	return g.send
}

func (g *uploadGate) begin(trace *transferLog) *sendLease {
	task, batch := "-", "-"
	if trace != nil {
		if trace.workload != "" {
			task = trace.workload
		}
		if trace.batch != "" {
			batch = trace.batch
		}
	}
	lease := &sendLease{gate: g, task: task, batch: batch, state: "starved", since: time.Now()}
	g.mu.Lock()
	g.live = append(g.live, lease)
	g.mu.Unlock()
	return lease
}

func (g *uploadGate) end(lease *sendLease) {
	if lease == nil {
		return
	}
	lease.releaseSend()
	lease.mu.Lock()
	opened := lease.opened
	lease.opened = false
	lease.mu.Unlock()
	if opened && g.open != nil {
		<-g.open
	}
	g.mu.Lock()
	kept := g.live[:0]
	for _, cur := range g.live {
		if cur != lease {
			kept = append(kept, cur)
		}
	}
	g.live = kept
	g.mu.Unlock()
}

func (g *uploadGate) acquireOpen(ctx context.Context, lease *sendLease) error {
	if g == nil || g.open == nil {
		return nil
	}
	if err := g.take(ctx, g.open, lease, false); err != nil {
		return err
	}
	lease.mu.Lock()
	lease.opened = true
	lease.mu.Unlock()
	return nil
}

func (g *uploadGate) holders() string {
	g.mu.Lock()
	defer g.mu.Unlock()
	parts := make([]string, 0, len(g.live))
	now := time.Now()
	for _, lease := range g.live {
		if lease == nil || !lease.holding() {
			continue
		}
		task, batch, state, since := lease.snapshot()
		parts = append(parts, fmt.Sprintf("%s/%s/%s/%s", shortID(task), shortID(batch), state, now.Sub(since).Round(time.Millisecond)))
	}
	if len(parts) == 0 {
		return "-"
	}
	return strings.Join(parts, " ")
}

func (g *uploadGate) take(ctx context.Context, ch chan struct{}, lease *sendLease, send bool) error {
	if ch == nil {
		return nil
	}
	start := time.Now()
	timer := time.NewTimer(uploadWaitLog)
	defer timer.Stop()
	select {
	case ch <- struct{}{}:
		if send {
			traceFrom(ctx).addSlotWait(time.Since(start))
		}
		return nil
	case <-timer.C:
		if send {
			traceFrom(ctx).uploadWait(time.Since(start), g.holders())
		}
	case <-ctx.Done():
		if send {
			traceFrom(ctx).addSlotWait(time.Since(start))
		}
		return ctx.Err()
	}
	select {
	case ch <- struct{}{}:
		if send {
			traceFrom(ctx).addSlotWait(time.Since(start))
		}
		return nil
	case <-ctx.Done():
		if send {
			traceFrom(ctx).addSlotWait(time.Since(start))
		}
		return ctx.Err()
	}
}

type sendLease struct {
	gate   *uploadGate
	task   string
	batch  string
	mu     sync.Mutex
	state  string
	since  time.Time
	held   bool
	opened bool
	takes  int
	taken  chan struct{}
}

func (l *sendLease) acquireSend(ctx context.Context) error {
	if l == nil {
		return nil
	}
	if l.holding() {
		return nil
	}
	ch := l.gate.sendCh()
	if err := l.gate.take(ctx, ch, l, true); err != nil {
		return err
	}
	l.mu.Lock()
	l.held = true
	l.taken = ch
	l.takes++
	if l.state != "stalled" {
		l.state = "sending"
		l.since = time.Now()
	}
	l.mu.Unlock()
	return nil
}

func (l *sendLease) releaseSend() {
	if l == nil {
		return
	}
	l.mu.Lock()
	held := l.held
	ch := l.taken
	l.held = false
	l.taken = nil
	l.mu.Unlock()
	if held && ch != nil {
		<-ch
	}
}

func (l *sendLease) holding() bool {
	if l == nil {
		return false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.held
}

func (l *sendLease) setState(state string) {
	if l == nil || state == "" {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.state == state {
		return
	}
	l.state = state
	l.since = time.Now()
}

func (l *sendLease) snapshot() (task, batch, state string, since time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.task, l.batch, l.state, l.since
}

func (l *sendLease) takesN() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.takes
}

func shortID(id string) string {
	if id == "" {
		return "-"
	}
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

type errorReader struct{ err error }

func (e errorReader) Read([]byte) (int, error) { return 0, e.err }

// slotBody hands the send token back while the download has caught the reader,
// and takes it again only after a megabyte is buffered or the download finishes.
type slotBody struct {
	ctx       context.Context
	fr        *followReader
	lease     *sendLease
	starvedAt time.Time
}

func newSlotBody(ctx context.Context, fr *followReader, lease *sendLease) *slotBody {
	return &slotBody{ctx: ctx, fr: fr, lease: lease}
}

func (s *slotBody) Read(p []byte) (int, error) {
	if s == nil || s.fr == nil {
		return 0, io.ErrClosedPipe
	}
	for {
		if err := s.ctx.Err(); err != nil {
			return 0, err
		}
		ahead, done, berr := s.fr.pending()
		if berr != nil && ahead == 0 {
			return 0, berr
		}
		if done && ahead == 0 {
			return 0, io.EOF
		}
		sendNow := done || int64(ahead) >= uploadReacquireBytes || s.lease.holding()
		if ahead > 0 && sendNow {
			s.starvedAt = time.Time{}
			if !s.lease.holding() {
				if err := s.lease.acquireSend(s.ctx); err != nil {
					return 0, err
				}
			}
			s.lease.setState("sending")
			return s.fr.Read(p)
		}
		if ahead == 0 {
			if s.starvedAt.IsZero() {
				s.starvedAt = time.Now()
			}
			if s.lease.holding() && time.Since(s.starvedAt) >= uploadStarveRelease {
				s.lease.releaseSend()
				s.lease.setState("starved")
			}
			limit := time.Duration(0)
			if s.lease.holding() {
				limit = uploadStarveRelease - time.Since(s.starvedAt)
				if limit < time.Millisecond {
					limit = time.Millisecond
				}
			}
			if _, err := s.fr.waitUntil(s.ctx, 1, limit); err != nil {
				return 0, err
			}
			continue
		}
		if _, err := s.fr.waitUntil(s.ctx, int(uploadReacquireBytes), 0); err != nil {
			return 0, err
		}
	}
}

func (s *slotBody) paused() bool {
	if s == nil || s.fr == nil {
		return false
	}
	return !s.lease.holding() || s.fr.starved()
}

func (s *slotBody) starvedFor() time.Duration {
	if s == nil || s.fr == nil {
		return 0
	}
	return s.fr.starvedFor()
}

// idleDrop reports that this attempt failed before it had sent a meaningful
// amount. A PUT that already handed the transport a megabyte failed while it
// was sending, and that stays a normal error.
func (s *slotBody) idleDrop(written int64) bool {
	if s == nil || s.lease == nil {
		return false
	}
	if s.lease.takesN() == 0 {
		return true
	}
	return s.starvedFor() >= uploadStarveRelease && written < uploadReacquireBytes
}

type starveBitKey struct{}
