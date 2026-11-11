package transfer

import (
	"context"
	"net"
	"time"

	"github.com/Beam-Network/beam/internal/workload/contracts"
	"github.com/Beam-Network/beam/internal/workload/storagehttp"
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
	return h.readIPRace(ctx, part, publish, 2, false)
}

// readIPRace opens n signed GETs. When pin is set, each one dials a different
// address for that host. The leg with the most bytes after hedgeReadAfter, or
// the first to finish, is kept.
func (h *Handler) readIPRace(ctx context.Context, part contracts.TransferPart, publish func(*followBuffer), n int, pin bool) ([]byte, error) {
	if n < 2 {
		n = 2
	}
	var ips []string
	if pin {
		ips = hostIPs(ctx, hostOf(part.Source.URL))
	}
	return h.raceReads(ctx, part, publish, n, ips)
}

func (h *Handler) raceReads(ctx context.Context, part contracts.TransferPart, publish func(*followBuffer), n int, ips []string) ([]byte, error) {
	type leg struct {
		ctx    context.Context
		cancel context.CancelFunc
		buf    *followBuffer
		body   []byte
		err    error
		done   chan struct{}
	}
	legs := make([]leg, n)
	for i := range legs {
		legs[i].ctx, legs[i].cancel = context.WithCancel(ctx)
		if i < len(ips) {
			legs[i].ctx = storagehttp.WithDialAddr(legs[i].ctx, ips[i])
		}
		legs[i].buf = newFollowBuffer(part.Length)
		legs[i].done = make(chan struct{})
	}
	for i := range legs {
		go func(i int) {
			defer close(legs[i].done)
			body, err := h.readSourceOnce(legs[i].ctx, part, false, false, legs[i].buf, nil, nil)
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
			best := legs[0].buf.Len()
			for i := 1; i < len(legs); i++ {
				if legs[i].buf.Len() > best {
					best = legs[i].buf.Len()
					winner = i
				}
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
	lens := make([]int64, len(legs))
	for i := range legs {
		lens[i] = legs[i].buf.Len()
	}
	traceFrom(ctx).readRace(winner, lens)
	if len(legs) == 2 {
		traceFrom(ctx).hedgeRead(winner, lens[0], lens[1])
	}
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

func hostIPs(ctx context.Context, host string) []string {
	name := host
	if h, _, err := net.SplitHostPort(host); err == nil {
		name = h
	}
	if name == "" || name == "-" {
		return nil
	}
	lookup, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	addrs, err := net.DefaultResolver.LookupNetIP(lookup, "ip", name)
	if err != nil {
		return nil
	}
	out := make([]string, 0, len(addrs))
	seen := map[string]bool{}
	for _, addr := range addrs {
		s := addr.String()
		if seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, s)
	}
	return out
}
