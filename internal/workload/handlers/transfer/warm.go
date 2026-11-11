package transfer

import (
	"context"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/Beam-Network/beam/internal/workload/contracts"
)

// warmHosts opens a few idle connections to each source and destination host
// so a 0.75s probe pays for the request, not a fresh TLS handshake.
func (h *Handler) warmHosts(ctx context.Context, parts []contracts.TransferPart) {
	if h == nil || h.client == nil || warmN <= 0 {
		return
	}
	seen := map[string]struct{}{}
	for _, part := range parts {
		for _, raw := range []string{part.Source.URL, part.Destination.URL} {
			host := hostOf(raw)
			if !warmHost(host) {
				continue
			}
			if _, ok := seen[host]; ok {
				continue
			}
			seen[host] = struct{}{}
			go h.warmHost(ctx, host, warmN)
		}
	}
}

func warmHost(host string) bool {
	name := host
	if h, _, err := net.SplitHostPort(host); err == nil {
		name = h
	}
	if name == "" || name == "-" || loopback(name) {
		return false
	}
	return true
}

func (h *Handler) warmHost(ctx context.Context, host string, n int) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://"+host+"/", nil)
			if err != nil {
				return
			}
			resp, err := h.client.Do(req)
			if resp != nil {
				_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
				_ = resp.Body.Close()
			}
			_ = err
		}()
	}
	wg.Wait()
	if trace := traceFrom(ctx); trace != nil {
		trace.out("transfer warm workload=%s attempt=%s host=%s conns=%d", trace.workload, trace.attempt, host, n)
	}
}
