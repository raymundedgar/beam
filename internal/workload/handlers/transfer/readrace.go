package transfer

import (
	"context"
	"time"

	"github.com/Beam-Network/beam/internal/workload/contracts"
)

type leaderKey struct{}

func withLeader(ctx context.Context, leader bool) context.Context {
	return context.WithValue(ctx, leaderKey{}, leader)
}

func isLeader(ctx context.Context) bool {
	leader, _ := ctx.Value(leaderKey{}).(bool)
	return leader
}

// readSourceRace runs two identical signed GETs and keeps the one that has
// moved more bytes after hedgeReadAfter, or the first one to finish. The
// other is cancelled. The signed request, including Range, is unchanged.
func (h *Handler) readSourceRace(ctx context.Context, part contracts.TransferPart, publish func(*followBuffer)) ([]byte, error) {
	type leg struct {
		ctx    context.Context
		cancel context.CancelFunc
		buf    *followBuffer
		body   []byte
		err    error
		done   chan struct{}
	}
	legs := make([]leg, 2)
	for i := range legs {
		legs[i].ctx, legs[i].cancel = context.WithCancel(ctx)
		legs[i].buf = newFollowBuffer(part.Length)
		legs[i].done = make(chan struct{})
	}
	for i := range legs {
		go func(i int) {
			defer close(legs[i].done)
			body, err := h.readSourceOnce(legs[i].ctx, part, false, legs[i].buf)
			legs[i].body, legs[i].err = body, err
			if err != nil {
				legs[i].buf.fail(err)
			} else {
				legs[i].buf.finish()
			}
		}(i)
	}
	defer func() {
		for i := range legs {
			legs[i].cancel()
		}
	}()
	winner := -1
	timer := time.NewTimer(hedgeReadAfter)
	defer timer.Stop()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for winner < 0 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
			winner = 0
			if legs[1].buf.Len() > legs[0].buf.Len() {
				winner = 1
			}
		case <-ticker.C:
			for i := range legs {
				select {
				case <-legs[i].done:
					if legs[i].err == nil {
						winner = i
					}
				default:
				}
			}
		}
	}
	for i := range legs {
		if i != winner {
			legs[i].cancel()
		}
	}
	traceFrom(ctx).hedgeRead(winner, legs[0].buf.Len(), legs[1].buf.Len())
	if publish != nil {
		publish(legs[winner].buf)
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-legs[winner].done:
	}
	if legs[winner].err != nil {
		return nil, legs[winner].err
	}
	return legs[winner].body, nil
}
