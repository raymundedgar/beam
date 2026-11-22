package transfer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Beam-Network/beam/internal/workload/contracts"
)

func TestUploadReuseWorthKeepsAShortTailAndAFastSocket(t *testing.T) {
	total := int64(50 << 20)
	proven := float64(20 << 20)
	slow := float64(1 << 20)
	if !uploadReuseWorth(40<<20, total, slow, proven) {
		t.Fatal("10 MiB left at 1 MiB/s was kept against a 20 MiB/s batch")
	}
	if uploadReuseWorth(48<<20, total, float64(400<<10), proven) {
		t.Fatal("a 2 MiB tail was re-uploaded")
	}
	if uploadReuseWorth(24<<20, total, float64(8<<20), proven) {
		t.Fatal("an 8 MiB/s socket was discarded")
	}
	if uploadReuseWorth(40<<20, total, slow, float64(5<<20)) {
		t.Fatal("a 5 MiB/s sibling was treated as proof")
	}
	if uploadReuseWorth(40<<20, total, slow, 0) {
		t.Fatal("a batch with no finished upload was re-uploaded")
	}
}

func TestSlowUploadYieldsToAFinishedBatchRate(t *testing.T) {
	batchPuts.note("batch-tail", float64(20<<20))
	ctx := withBatch(context.Background(), "batch-tail")
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(context.Canceled)
	var n atomic.Int64
	n.Store(30 << 20)
	go func() {
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				n.Add(20 << 10)
			}
		}
	}()
	go watchPutRecent(ctx, cancel, 50<<20, 40*time.Millisecond, 0,
		func() (int64, bool) { return n.Load(), true },
		func() tcpSnap { return tcpSnap{ok: true, sndWnd: 1 << 20} },
		func() bool { return false },
		func() bool { return true },
		&putPace{}, nil, nil, nil, true)
	select {
	case <-ctx.Done():
		if context.Cause(ctx) != errReplaced {
			t.Fatalf("cause %v", context.Cause(ctx))
		}
	case <-time.After(time.Second):
		t.Fatal("kept a large slow tail after the batch had finished an upload at 20 MiB/s")
	}
}

func TestMedianStaysOpenForOneTryPastTheCap(t *testing.T) {
	var pace readPace
	if !medianOpen(readRestartCap, &pace, false) {
		t.Fatal("the socket under the cap was left to finish")
	}
	if medianOpen(readRestartCap+1, &pace, false) {
		t.Fatal("restarts continued past the extra try")
	}
	if medianOpen(0, &pace, true) {
		t.Fatal("a frozen download was restarted")
	}
}

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

func TestNonTailHoldUsesItsOwnCounter(t *testing.T) {
	prev := putHoldCap
	putHoldCap = 50 * time.Millisecond
	t.Cleanup(func() { putHoldCap = prev })
	expire := func(pace *putPace) error {
		ctx, cancel := context.WithCancelCause(context.Background())
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			watchPutRecent(ctx, cancel, 50<<20, 20*time.Millisecond, 1<<30,
				func() (int64, bool) { return 1000, true },
				func() tcpSnap { return tcpSnap{ok: true, sndWnd: 0, unacked: 4} },
				nil, nil, pace, nil,
				func(float64, int64, tcpSnap, string, time.Duration, int64) {},
				nil, true)
		}()
		var err error
		select {
		case <-ctx.Done():
			err = context.Cause(ctx)
		case <-time.After(time.Second):
			err = nil
		}
		cancel(context.Canceled)
		wg.Wait()
		return err
	}
	pace := &putPace{}
	if err := expire(pace); err != errHoldExpired {
		t.Fatalf("first hold = %v", err)
	}
	if pace.holds() != 1 || pace.prev.Load() != 0 {
		t.Fatalf("hold counted as a slow discard holds=%d prev=%d", pace.holds(), pace.prev.Load())
	}
	if err := expire(pace); err != errHoldExpired {
		t.Fatalf("second hold = %v", err)
	}
	if err := expire(pace); err != nil {
		t.Fatalf("third hold still expired: %v", err)
	}
}

func TestStarvedReaderDoesNotAdvanceTheHoldClock(t *testing.T) {
	prev := putHoldCap
	putHoldCap = 40 * time.Millisecond
	t.Cleanup(func() { putHoldCap = prev })
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(context.Canceled)
	go watchPutRecent(ctx, cancel, 50<<20, 20*time.Millisecond, 1<<30,
		func() (int64, bool) { return 1000, true },
		func() tcpSnap { return tcpSnap{ok: true, sndWnd: 0, unacked: 8} },
		func() bool { return true }, nil, &putPace{}, nil, nil, nil, true)
	time.Sleep(200 * time.Millisecond)
	if context.Cause(ctx) == errHoldExpired {
		t.Fatal("a starved reader advanced the hold clock")
	}
}

func TestQueuedBodyAdvancesTheHoldClockWhenEveryByteIsAcked(t *testing.T) {
	prev := putHoldCap
	putHoldCap = 40 * time.Millisecond
	t.Cleanup(func() { putHoldCap = prev })
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(context.Canceled)
	go watchPutRecent(ctx, cancel, 50<<20, 20*time.Millisecond, 1<<30,
		func() (int64, bool) { return 1000, true },
		func() tcpSnap { return tcpSnap{ok: true, sndWnd: 0, unacked: 0} },
		nil, func() bool { return false }, &putPace{}, nil, nil, nil, true)
	select {
	case <-ctx.Done():
		if context.Cause(ctx) != errHoldExpired {
			t.Fatalf("cause = %v", context.Cause(ctx))
		}
	case <-time.After(time.Second):
		t.Fatal("a closed window with more body queued did not expire")
	}
}

func TestSkippedTickDoesNotDelayHoldExpiry(t *testing.T) {
	prev := putHoldCap
	putHoldCap = 300 * time.Millisecond
	t.Cleanup(func() { putHoldCap = prev })
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(context.Canceled)
	// A 1s sample window used to skip every tick until the first sample, so a
	// 300ms cap could not fire until that sample. The ticker is 250ms.
	go watchPutRecent(ctx, cancel, 50<<20, time.Second, 1<<30,
		func() (int64, bool) { return 1000, true },
		func() tcpSnap { return tcpSnap{ok: true, sndWnd: 0, unacked: 8} },
		nil, nil, &putPace{}, nil, nil, nil, true)
	select {
	case <-ctx.Done():
		if context.Cause(ctx) != errHoldExpired {
			t.Fatalf("cause = %v", context.Cause(ctx))
		}
	case <-time.After(1200 * time.Millisecond):
		t.Fatal("closed-window time waited for the rate sample")
	}
}

func TestWrittenBodyUsesTheResponseCap(t *testing.T) {
	prevHold, prevResp := putHoldCap, putHoldResponseCap
	t.Cleanup(func() {
		putHoldCap = prevHold
		putHoldResponseCap = prevResp
	})
	putHoldCap = 30 * time.Millisecond
	putHoldResponseCap = 2 * time.Second
	ctx, cancel := context.WithCancelCause(context.Background())
	go watchPutRecent(ctx, cancel, 50<<20, 20*time.Millisecond, 1<<30,
		func() (int64, bool) { return 1000, true },
		func() tcpSnap { return tcpSnap{ok: true, sndWnd: 0, unacked: 0} },
		nil, func() bool { return true }, &putPace{}, nil, nil, nil, true)
	time.Sleep(200 * time.Millisecond)
	if context.Cause(ctx) == errHoldExpired {
		t.Fatal("a written body used the send cap")
	}
	cancel(context.Canceled)

	putHoldCap = 10 * time.Second
	putHoldResponseCap = 40 * time.Millisecond
	ctx, cancel = context.WithCancelCause(context.Background())
	defer cancel(context.Canceled)
	go watchPutRecent(ctx, cancel, 50<<20, 20*time.Millisecond, 1<<30,
		func() (int64, bool) { return 1000, true },
		func() tcpSnap { return tcpSnap{ok: true, sndWnd: 0, unacked: 0} },
		nil, func() bool { return true }, &putPace{}, nil, nil, nil, true)
	select {
	case <-ctx.Done():
		if context.Cause(ctx) != errHoldExpired {
			t.Fatalf("cause = %v", context.Cause(ctx))
		}
	case <-time.After(time.Second):
		t.Fatal("a written body ignored the response cap")
	}
}

func TestTailHoldKeepsTheLongCap(t *testing.T) {
	prev := putHoldCap
	putHoldCap = 50 * time.Millisecond
	t.Cleanup(func() { putHoldCap = prev })
	if holdLimit(10<<20, 50<<20) != putHoldCap {
		t.Fatal("a non-tail upload ignored the configured cap")
	}
	if holdLimit(45<<20, 50<<20) != putHoldTailCap {
		t.Fatal("a tail upload used the short cap")
	}
}

func TestHoldAndReadLinesCarryTheMeasuredFields(t *testing.T) {
	var lines []string
	trace := &transferLog{workload: "w", attempt: "a", printf: func(format string, args ...any) {
		lines = append(lines, fmt.Sprintf(format, args...))
	}}
	trace.putWindow("up", "1", tcpSnap{ok: true, sndWnd: 0}, 1<<20, "hold_rwnd", 8<<20, 48<<20, 3200*time.Millisecond)
	trace.holdExpired("up", "1", 3200*time.Millisecond, 200, 21.5, 1700*time.Millisecond, false)
	trace.readKeep("cdn.example", "rate", 10<<20, 6<<20, 30<<20, 0)
	trace.readRestart("cdn.example", 12<<20, 3<<20, 30<<20, 2)
	got := strings.Join(lines, "\n")
	for _, want := range []string{"decision=hold_rwnd", "progress=8388608", "total=50331648", "hold_age=3.2s", "hold_age_ms=3200", "status=200", "rate_mib=21.50", "dur_ms=1700", "wnd0=false", "keep_mib=5", "below=0", "below=2"} {
		if !strings.Contains(got, want) {
			t.Fatalf("missing %s in %s", want, got)
		}
	}
}

func TestHoldFollowupLogsTheNextAttempt(t *testing.T) {
	var lines []string
	trace := &transferLog{workload: "w", attempt: "a", printf: func(format string, args ...any) {
		lines = append(lines, fmt.Sprintf(format, args...))
	}}
	pace := &putPace{}
	pace.markHold(4500 * time.Millisecond)
	part := contracts.TransferPart{}
	var owed time.Duration
	noteHoldFollowup(trace, part, &owed, pace, partResult{}, errHoldExpired, time.Second)
	if owed != 4500*time.Millisecond || len(lines) != 0 {
		t.Fatalf("expiry logged itself owed=%s lines=%v", owed, lines)
	}
	noteHoldFollowup(trace, part, &owed, pace, partResult{httpStatus: 200, ackRate: 21.5}, nil, 1700*time.Millisecond)
	if owed != 0 {
		t.Fatal("the follow-up did not consume the hold")
	}
	got := strings.Join(lines, "\n")
	for _, want := range []string{"hold_age_ms=4500", "status=200", "rate_mib=21.50", "dur_ms=1700"} {
		if !strings.Contains(got, want) {
			t.Fatalf("missing %s in %s", want, got)
		}
	}
}

func TestRetryCapsRejectBlankAndNonsense(t *testing.T) {
	probe, httpTries := maxSourceReplacements, presignedAttempts
	t.Cleanup(func() {
		maxSourceReplacements, presignedAttempts = probe, httpTries
	})
	t.Setenv("BEAM_WORKER_DL_PROBE_REPLACEMENTS", "nope")
	t.Setenv("BEAM_WORKER_BOTH_HTTP_ATTEMPTS", "0")
	envNonNegative("BEAM_WORKER_DL_PROBE_REPLACEMENTS", &maxSourceReplacements)
	envAtLeastOne("BEAM_WORKER_BOTH_HTTP_ATTEMPTS", &presignedAttempts)
	if maxSourceReplacements != probe || presignedAttempts != httpTries {
		t.Fatalf("caps changed probe=%d http=%d", maxSourceReplacements, presignedAttempts)
	}
	t.Setenv("BEAM_WORKER_DL_PROBE_REPLACEMENTS", "4")
	t.Setenv("BEAM_WORKER_BOTH_HTTP_ATTEMPTS", "2")
	envNonNegative("BEAM_WORKER_DL_PROBE_REPLACEMENTS", &maxSourceReplacements)
	envAtLeastOne("BEAM_WORKER_BOTH_HTTP_ATTEMPTS", &presignedAttempts)
	if maxSourceReplacements != 4 || presignedAttempts != 2 {
		t.Fatalf("caps = probe %d http %d", maxSourceReplacements, presignedAttempts)
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
