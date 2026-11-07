package transfer

import (
	"context"
	"sync"

	"github.com/Beam-Network/beam/internal/workload/contracts"
)

// memoryGate bounds how many part buffers are in flight. A budget of zero runs
// one part at a time. A part larger than the budget still runs, alone.
type memoryGate struct {
	mu     sync.Mutex
	cond   *sync.Cond
	free   int64
	budget int64
	serial chan struct{}
}

func newMemoryGate(budget int64) *memoryGate {
	gate := &memoryGate{budget: budget, free: budget}
	gate.cond = sync.NewCond(&gate.mu)
	if budget <= 0 {
		gate.serial = make(chan struct{}, 1)
	}
	return gate
}

func bufferedBytes(length int64) int64 {
	if length < 0 {
		length = 0
	}
	return length + contracts.TransferBufferOverheadBytes
}

func (g *memoryGate) acquire(ctx context.Context, need int64) (int64, error) {
	if g.serial != nil {
		select {
		case g.serial <- struct{}{}:
			return 0, nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	if need > g.budget {
		need = g.budget
	}
	if need < 0 {
		need = 0
	}
	stopped := make(chan struct{})
	defer close(stopped)
	go func() {
		select {
		case <-ctx.Done():
			g.cond.Broadcast()
		case <-stopped:
		}
	}()
	g.mu.Lock()
	defer g.mu.Unlock()
	for g.free < need {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		g.cond.Wait()
	}
	g.free -= need
	return need, nil
}

func (g *memoryGate) release(held int64) {
	if g.serial != nil {
		<-g.serial
		return
	}
	g.mu.Lock()
	g.free += held
	g.mu.Unlock()
	g.cond.Broadcast()
}
