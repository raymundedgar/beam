package transfer

import (
	"context"
	"sync"
	"time"
)

// batchPriority holds the other parts of a BeamCore task_offer_batch until the
// first one to start has finished, or until deliverWait. The part is complete
// on this worker when Execute returns, which is when the result is handed off
// to be reported. Tasks with an empty batch id are not gated.
type batchPriority struct {
	mu      sync.Mutex
	batches map[string]*batchWait
}

type batchWait struct {
	leader string
	done   chan struct{}
	once   sync.Once
}

func newBatchPriority() *batchPriority {
	return &batchPriority{batches: make(map[string]*batchWait)}
}

// begin returns whether this attempt is the batch's first deliverer. The
// returned function reports that the leader has finished and must be called
// once. Followers block until that report or deliverWait.
func (b *batchPriority) begin(ctx context.Context, batch, attempt string) (bool, func()) {
	if b == nil || batch == "" {
		return true, func() {}
	}
	b.mu.Lock()
	st, ok := b.batches[batch]
	if !ok {
		st = &batchWait{leader: attempt, done: make(chan struct{})}
		b.batches[batch] = st
		b.mu.Unlock()
		return true, func() {
			st.once.Do(func() { close(st.done) })
			b.mu.Lock()
			if current := b.batches[batch]; current == st {
				delete(b.batches, batch)
			}
			b.mu.Unlock()
		}
	}
	done := st.done
	b.mu.Unlock()
	timer := time.NewTimer(deliverWait)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
	case <-ctx.Done():
	}
	return false, func() {}
}
