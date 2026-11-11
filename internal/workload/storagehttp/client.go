package storagehttp

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"net/netip"
	"syscall"
	"time"

	"github.com/Beam-Network/beam/internal/workload/contracts"
)

const MaxSourceRedirects = 3

var (
	errAddressNotPublic  = errors.New("storage address is not public")
	errRedirectLimit     = errors.New("storage source redirect limit reached")
	errRedirectDowngrade = errors.New("storage source redirect leaves HTTPS")
)

// NewClient returns the HTTP client for storage URLs.
// HTTP/2 is disabled so parallel PUTs to one R2 host each get their own TCP
// connection instead of multiplexing on a single stream that can stall.
func NewClient(timeout time.Duration) *http.Client {
	return newClient(timeout, func(address netip.AddrPort) bool {
		return contracts.PublicAddressAllowed(address.Addr())
	})
}

func newClient(timeout time.Duration, allowed func(netip.AddrPort) bool) *http.Client {
	dialer := &net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
		Control: func(_, address string, _ syscall.RawConn) error {
			target, err := netip.ParseAddrPort(address)
			if err != nil || !allowed(target) {
				return errAddressNotPublic
			}
			return nil
		},
	}
	baseDial := dialer.DialContext
	transport := &http.Transport{
		Proxy: nil,
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			if pinned, ok := dialAddr(ctx); ok {
				addr = rewriteDialAddr(addr, pinned)
			}
			return baseDial(ctx, network, addr)
		},
		TLSClientConfig: &tls.Config{
			ClientSessionCache: tls.NewLRUClientSessionCache(128),
		},
		ForceAttemptHTTP2:     false,
		TLSNextProto:          map[string]func(authority string, c *tls.Conn) http.RoundTripper{},
		MaxIdleConns:          4096,
		MaxIdleConnsPerHost:   256,
		MaxConnsPerHost:       0,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: time.Second,
		DisableCompression:    true,
	}
	return &http.Client{Timeout: timeout, Transport: transport, CheckRedirect: checkRedirect}
}

type dialPinKey struct{}

// WithDialAddr forces this request's dial onto one resolved address so
// parallel attempts are not all the first DNS answer.
func WithDialAddr(ctx context.Context, ip string) context.Context {
	if ip == "" {
		return ctx
	}
	return context.WithValue(ctx, dialPinKey{}, ip)
}

func dialAddr(ctx context.Context) (string, bool) {
	ip, ok := ctx.Value(dialPinKey{}).(string)
	return ip, ok && ip != ""
}

func rewriteDialAddr(addr, pinned string) string {
	_, port, err := net.SplitHostPort(addr)
	if err != nil || pinned == "" {
		return addr
	}
	return net.JoinHostPort(pinned, port)
}

func checkRedirect(next *http.Request, via []*http.Request) error {
	if via[0].Method != http.MethodGet {
		return http.ErrUseLastResponse
	}
	if len(via) > MaxSourceRedirects {
		return errRedirectLimit
	}
	if via[len(via)-1].URL.Scheme == "https" && next.URL.Scheme != "https" {
		return errRedirectDowngrade
	}
	return nil
}
