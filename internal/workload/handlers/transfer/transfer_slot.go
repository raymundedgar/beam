package transfer

import (
	"context"
	"log"
	"sync"
	"time"
)

var (
	transferHandoff = func(rule, waiter, batch string, queued int) {
		log.Printf("transfer slot_handoff rule=%s waiter=%s batch=%s queued=%d", rule, waiter, batch, queued)
	}
)

type transferSlotLease struct {
	gate       *transferGate
	task       string
	batch      string
	isReserved bool
	tokenHeld  bool
	once       sync.Once
}

func (l *transferSlotLease) release() {
	if l == nil || l.gate == nil {
		return
	}
	l.once.Do(func() {
		l.gate.release(l)
	})
}

func (l *transferSlotLease) IsReserved() bool {
	if l == nil {
		return false
	}
	return l.isReserved
}

func (l *transferSlotLease) Batch() string {
	if l == nil {
		return ""
	}
	return l.batch
}

type transferWaiter struct {
	gate     *transferGate
	task     string
	batch    string
	queuedAt time.Time
	grant    chan *transferSlotLease
	granted  bool
	lease    *transferSlotLease
}

type transferGate struct {
	h            *Handler
	mu           sync.Mutex
	concurrency  int
	reserveLimit int
	fcfsLimit    int

	fcfsActive      int
	reservedBatches map[string]*transferSlotLease
	batchActive     map[string]int

	waiters []*transferWaiter
}

func newTransferGate(h *Handler, concurrency, reserveLimit int) *transferGate {
	g := &transferGate{
		h:               h,
		concurrency:     concurrency,
		reserveLimit:    reserveLimit,
		reservedBatches: make(map[string]*transferSlotLease),
		batchActive:     make(map[string]int),
	}
	g.updateLimitsLocked(concurrency, reserveLimit)
	return g
}

func (g *transferGate) updateLimitsLocked(concurrency, reserveLimit int) {
	g.concurrency = concurrency
	if concurrency < 0 {
		g.reserveLimit = 0
		g.fcfsLimit = -1
		return
	}
	if reserveLimit < 0 {
		reserveLimit = 0
	}
	if concurrency <= reserveLimit {
		if concurrency > 1 {
			reserveLimit = concurrency - 1
		} else {
			reserveLimit = 0
		}
	}
	g.reserveLimit = reserveLimit
	g.fcfsLimit = concurrency - reserveLimit
}

func (g *transferGate) sendSlotToken(lease *transferSlotLease) {
	if g.h == nil || g.h.slots == nil {
		return
	}
	select {
	case g.h.slots <- struct{}{}:
		lease.tokenHeld = true
	default:
	}
}

func (g *transferGate) acquire(ctx context.Context) (*transferSlotLease, error) {
	task, batch := taskAndBatchFrom(ctx)

	g.mu.Lock()
	if g.h != nil {
		if g.h.slots == nil {
			g.mu.Unlock()
			return &transferSlotLease{gate: g, task: task, batch: batch}, nil
		}
		if cap(g.h.slots) != g.concurrency {
			g.updateLimitsLocked(cap(g.h.slots), g.reserveLimit)
		}
	}
	if g.concurrency < 0 {
		g.mu.Unlock()
		return &transferSlotLease{gate: g, task: task, batch: batch}, nil
	}

	// 1. Can we grant an FCFS slot immediately?
	// Only grant immediately if no waiters are queued, ensuring FIFO ordering.
	if len(g.waiters) == 0 && g.fcfsActive < g.fcfsLimit {
		lease := &transferSlotLease{gate: g, task: task, batch: batch, isReserved: false}
		g.fcfsActive++
		if batch != "" && batch != "-" {
			g.batchActive[batch]++
		}
		g.sendSlotToken(lease)
		g.mu.Unlock()
		return lease, nil
	}

	// 2. Can we grant a Reserved slot immediately?
	// Criteria:
	// - Available reserved slot capacity (len(reservedBatches) < reserveLimit)
	// - Valid batch identifier (batch != "" && batch != "-")
	// - Batch does not already occupy a reserved slot (reservedBatches[batch] == nil)
	// - Batch currently has no task in progress (batchActive[batch] == 0)
	if g.reserveLimit > 0 && len(g.reservedBatches) < g.reserveLimit && batch != "" && batch != "-" {
		if g.reservedBatches[batch] == nil && g.batchActive[batch] == 0 {
			lease := &transferSlotLease{gate: g, task: task, batch: batch, isReserved: true}
			g.reservedBatches[batch] = lease
			g.batchActive[batch]++
			g.sendSlotToken(lease)
			if transferHandoff != nil {
				transferHandoff("reserve", task, batch, len(g.waiters))
			}
			g.mu.Unlock()
			return lease, nil
		}
	}

	// 3. Queue waiter
	w := &transferWaiter{
		gate:     g,
		task:     task,
		batch:    batch,
		queuedAt: time.Now(),
		grant:    make(chan *transferSlotLease, 1),
	}
	g.waiters = append(g.waiters, w)
	g.mu.Unlock()

	select {
	case lease := <-w.grant:
		return lease, nil
	case <-ctx.Done():
		g.cancelWait(w)
		return nil, ctx.Err()
	}
}

func (g *transferGate) cancelWait(w *transferWaiter) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !w.granted {
		kept := g.waiters[:0]
		for _, cur := range g.waiters {
			if cur != w {
				kept = append(kept, cur)
			}
		}
		g.waiters = kept
		return
	}
	select {
	case lease := <-w.grant:
		g.releaseLocked(lease)
	default:
		if w.lease != nil {
			g.releaseLocked(w.lease)
		}
	}
}

func (g *transferGate) release(lease *transferSlotLease) {
	if lease == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.releaseLocked(lease)
}

func (g *transferGate) releaseLocked(lease *transferSlotLease) {
	if lease == nil {
		return
	}
	if lease.isReserved {
		delete(g.reservedBatches, lease.batch)
	} else {
		if g.fcfsActive > 0 {
			g.fcfsActive--
		}
	}
	if lease.batch != "" && lease.batch != "-" {
		if g.batchActive[lease.batch] > 0 {
			g.batchActive[lease.batch]--
			if g.batchActive[lease.batch] == 0 {
				delete(g.batchActive, lease.batch)
			}
		}
	}
	if lease.tokenHeld && g.h != nil && g.h.slots != nil {
		select {
		case <-g.h.slots:
		default:
		}
		lease.tokenHeld = false
	}
	g.scheduleLocked()
}

func (g *transferGate) scheduleLocked() {
	for len(g.waiters) > 0 {
		progress := false

		// 1. Check if the oldest waiter can take an FCFS slot
		if g.fcfsActive < g.fcfsLimit {
			w := g.waiters[0]
			g.waiters = g.waiters[1:]
			lease := &transferSlotLease{gate: g, task: w.task, batch: w.batch, isReserved: false}
			g.fcfsActive++
			if w.batch != "" && w.batch != "-" {
				g.batchActive[w.batch]++
			}
			g.sendSlotToken(lease)
			w.granted = true
			w.lease = lease
			w.grant <- lease
			if transferHandoff != nil {
				transferHandoff("fcfs", w.task, w.batch, len(g.waiters))
			}
			progress = true
			continue
		}

		// 2. Check if any waiter qualifies for an open reserved slot
		if g.reserveLimit > 0 && len(g.reservedBatches) < g.reserveLimit {
			wIdx := -1
			for i, w := range g.waiters {
				if w.batch != "" && w.batch != "-" &&
					g.reservedBatches[w.batch] == nil &&
					g.batchActive[w.batch] == 0 {
					wIdx = i
					break
				}
			}
			if wIdx >= 0 {
				w := g.waiters[wIdx]
				g.waiters = append(g.waiters[:wIdx], g.waiters[wIdx+1:]...)
				lease := &transferSlotLease{gate: g, task: w.task, batch: w.batch, isReserved: true}
				g.reservedBatches[w.batch] = lease
				g.batchActive[w.batch]++
				g.sendSlotToken(lease)
				w.granted = true
				w.lease = lease
				w.grant <- lease
				if transferHandoff != nil {
					transferHandoff("reserve", w.task, w.batch, len(g.waiters))
				}
				progress = true
				continue
			}
		}

		if !progress {
			break
		}
	}
}

type batchContextKey struct{}

func withBatch(ctx context.Context, batch string) context.Context {
	return context.WithValue(ctx, batchContextKey{}, batch)
}

func batchFrom(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if b, ok := ctx.Value(batchContextKey{}).(string); ok && b != "" {
		return b
	}
	if t := traceFrom(ctx); t != nil && t.batch != "" {
		return t.batch
	}
	return ""
}

func taskAndBatchFrom(ctx context.Context) (task, batch string) {
	task, batch = "-", "-"
	if ctx == nil {
		return task, batch
	}
	if b := batchFrom(ctx); b != "" {
		batch = b
	}
	if trace := traceFrom(ctx); trace != nil {
		if trace.workload != "" {
			task = trace.workload
		}
		if trace.batch != "" && batch == "-" {
			batch = trace.batch
		}
	}
	return task, batch
}
