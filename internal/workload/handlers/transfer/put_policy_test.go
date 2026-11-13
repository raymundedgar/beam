package transfer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestPolicyStampUsesTheEmbeddedRevision(t *testing.T) {
	t.Log(policyStamp())
	if !strings.Contains(policyStamp(), "build=") || strings.Contains(policyStamp(), "build=unknown") && revisionFromBuildInfo() != "" {
		t.Fatalf("stamp = %s info=%s", policyStamp(), revisionFromBuildInfo())
	}
}

func TestSlowUploadEvidenceKeepsTheRay(t *testing.T) {
	header := http.Header{}
	header.Set("Cf-Ray", "9abc123def456-DFW")
	header.Set("X-Amz-Request-Id", "req-1")
	header.Set("Date", "Wed, 11 Nov 2026 13:44:00 GMT")
	header.Set("Authorization", "secret-token")
	header.Set("Set-Cookie", "sid=secret")
	ray, fields := putResponseEvidence(header)
	if ray != "9abc123def456-DFW" {
		t.Fatalf("ray = %q", ray)
	}
	if !strings.Contains(fields, "cf_ray=9abc123def456-DFW") || !strings.Contains(fields, "x_amz_request_id=req-1") {
		t.Fatalf("fields = %q", fields)
	}
	if strings.Contains(fields, "secret") {
		t.Fatalf("secret leaked: %q", fields)
	}
	if !strings.Contains(fields, "date=Wed,_11_Nov_2026_13:44:00_GMT") {
		t.Fatalf("date was not one token: %q", fields)
	}
	ray, fields = putResponseEvidence(nil)
	if ray != "-" || fields != "headers=-" {
		t.Fatalf("empty = %s %s", ray, fields)
	}
}

func TestWarmSkipsLoopback(t *testing.T) {
	if warmHost("127.0.0.1:9") || warmHost("::1") || warmHost("-") {
		t.Fatal("loopback would be pre-dialed")
	}
	if !warmHost("example.r2.cloudflarestorage.com") {
		t.Fatal("a storage host was not pre-dialed")
	}
}

func TestIPFamily(t *testing.T) {
	if ipFamilyOf("172.64.66.1") != "v4" {
		t.Fatal("v4")
	}
	if ipFamilyOf("2606:4700:113::1") != "v6" {
		t.Fatal("v6")
	}
	if ipFamilyOf("") != "-" {
		t.Fatal("empty")
	}
}

func TestTwoSlowSecondsDiscardAndOneFastSecondDoesNot(t *testing.T) {
	var windows ackWindows
	start := time.Now()
	if sampled, _, lows := windows.push(start, 0, time.Second, float64(3<<20)); sampled || lows != 0 {
		t.Fatal("the first sample has no window yet")
	}
	// 1 MiB in the first second is under 3 MiB/s.
	sampled, _, lows := windows.push(start.Add(time.Second), 1<<20, time.Second, float64(3<<20))
	if !sampled || lows != 1 {
		t.Fatalf("sampled=%v lows=%d, one slow second is kept", sampled, lows)
	}
	// 4 MiB in the next second clears the streak.
	if _, _, lows = windows.push(start.Add(2*time.Second), 5<<20, time.Second, float64(3<<20)); lows != 0 {
		t.Fatal("a fast second left the slow streak set")
	}
	// Two slow seconds in a row discard. 5 MiB + 1 MiB + 1 MiB.
	windows.push(start.Add(3*time.Second), 6<<20, time.Second, float64(3<<20))
	_, _, lows = windows.push(start.Add(4*time.Second), 7<<20, time.Second, float64(3<<20))
	if lows < 2 {
		t.Fatal("two consecutive slow seconds were kept")
	}
}

func TestTailZeroWaitsAndTinyWindowHolds(t *testing.T) {
	if putInTail(40<<20, 43<<20) != true {
		t.Fatal("40 MiB of 43 was not treated as the tail")
	}
	if putInTail(1<<20, 50<<20) {
		t.Fatal("the first megabyte was treated as the tail")
	}
	if putInTail(0, 50<<20) {
		t.Fatal("a socket with no acknowledgements was treated as the tail")
	}
	if !peerWindowBlocked(tcpSnap{ok: true, sndWnd: 0, sndMss: 1460}) {
		t.Fatal("a zero peer window was discarded")
	}
	if !peerWindowBlocked(tcpSnap{ok: true, sndWnd: 1460, sndMss: 1460}) {
		t.Fatal("a one-segment window was discarded")
	}
	if peerWindowBlocked(tcpSnap{ok: true, sndWnd: 64 << 10, sndMss: 1460}) {
		t.Fatal("an open peer window was held")
	}
	if peerWindowBlocked(tcpSnap{}) {
		t.Fatal("a socket with no TCP info was held")
	}
}

func TestFullRereadKeepsANearlyFinishedSlowRead(t *testing.T) {
	const mib = 1 << 20
	size := int64(43 * mib)
	median := 12.6 * mib
	current := 3.9 * mib
	if readRestartWorth(size*95/100, size, current, median) {
		t.Fatal("a 3.9 MiB/s read at 95% was restarted")
	}
	if !readRestartWorth(size*10/100, size, current, median) {
		t.Fatal("a 3.9 MiB/s read at 10% was kept")
	}
	if readRestartWorth(size*10/100, size, current, 0) {
		t.Fatal("an empty median restarted a read")
	}
}

func TestTwoDisappointingRestartsStop(t *testing.T) {
	var pace readPace
	fast := float64(6 << 20)
	if pace.block(fast) {
		t.Fatal("the first socket was not allowed to restart")
	}
	if pace.block(fast) {
		t.Fatal("a replacement that tied the previous rate stopped immediately")
	}
	if !pace.block(float64(5 << 20)) {
		t.Fatal("a second slower replacement above the keep rate was restarted again")
	}
}

func TestSlowSocketsDoNotStopTheSearch(t *testing.T) {
	var pace readPace
	for i := 0; i < 4; i++ {
		if pace.block(float64(1 << 20)) {
			t.Fatal("a 1 MiB/s socket stopped the search")
		}
	}
	if pace.done() {
		t.Fatal("slow sockets latched the thrash guard")
	}
}

func TestFiveMiBPerSecondIsKept(t *testing.T) {
	if !readFastEnough(5<<20, time.Second) {
		t.Fatal("5 MiB in one second was replaceable")
	}
	if readFastEnough(4<<20, time.Second) {
		t.Fatal("4 MiB/s was kept")
	}
	// 7.5 MiB in 1.5s is 5 MiB/s. The 8s budget still calls that too slow for a
	// 48 MiB part, and the probe must not follow it.
	moved := int64(15 << 20 / 2)
	elapsed := 1500 * time.Millisecond
	if !readFastEnough(moved, elapsed) {
		t.Fatal("5 MiB/s over the probe interval was replaceable")
	}
	if !rateTooSlow(moved, 48<<20, elapsed, 8*time.Second, 1<<20) {
		t.Fatal("the 8s budget no longer calls 5 MiB/s slow")
	}
	if readFastEnough(3<<20/2, elapsed) {
		t.Fatal("1.5 MiB/s was kept")
	}
	if readFastEnough(6<<20, 2*time.Second) {
		t.Fatal("6 MiB over two seconds was treated as 6 MiB/s")
	}
	if keepMeasuredRate(time.Millisecond, 5<<20, time.Second) {
		t.Fatal("a one-millisecond budget kept a fast socket")
	}
}

func TestProvenUploadNeedsFourSlowSeconds(t *testing.T) {
	if putSlowStreak(1<<20, 50<<20) != 2 {
		t.Fatal("a fresh upload discarded on one slow second")
	}
	if putSlowStreak(putAckKeep, 50<<20) != putAckTailFactor {
		t.Fatal("16 MiB acknowledged did not lengthen the slow streak")
	}
	if putSlowStreak(25<<20, 50<<20) != putAckTailFactor {
		t.Fatal("half the object did not lengthen the slow streak")
	}
}

func TestSlowerReplacementIsKept(t *testing.T) {
	var pace putPace
	pace.discarded(4 << 20)
	if !pace.notFaster(1 << 20) {
		t.Fatal("a slower replacement was discarded")
	}
	if !pace.stopped() {
		t.Fatal("discarding continued after a slower replacement")
	}
	var faster putPace
	faster.discarded(1 << 20)
	if faster.notFaster(4 << 20) {
		t.Fatal("a faster replacement was kept from being used")
	}
}

func TestThrottledUploadSendsTheWholeBody(t *testing.T) {
	object := bytes.Repeat([]byte{9}, 256<<10)
	want := sha256.Sum256(object)
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(object)
	}))
	t.Cleanup(source.Close)
	dest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf := make([]byte, 4<<10)
		var got []byte
		for {
			n, err := r.Body.Read(buf)
			if n > 0 {
				got = append(got, buf[:n]...)
				time.Sleep(2 * time.Millisecond)
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Errorf("read: %v", err)
				return
			}
		}
		sum := sha256.Sum256(got)
		if sum != want {
			t.Errorf("uploaded %d bytes", len(got))
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(dest.Close)
	handler := retryingHandler()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := handler.Execute(ctx, presignedSpec(t, source.URL, dest.URL, int64(len(object))))
	if err != nil {
		t.Fatal(err)
	}
	if result.BytesProcessed != int64(len(object)) {
		t.Fatalf("bytes=%d", result.BytesProcessed)
	}
}
