package transfer

import (
	"os"
	"strconv"
	"time"
)

// Flags are read from the environment at process start. Tests set the
// variables directly.
var (
	// earlyPut starts the upload once earlyPutBytes are buffered, or
	// earlyPutAfter after the first byte, whichever comes first.
	earlyPut      = true
	earlyPutBytes = int64(1 << 20)
	earlyPutAfter = 500 * time.Millisecond
	// hedgeRead races two identical signed GETs. Off unless BEAM_WORKER_READ_HEDGE=1.
	hedgeRead      = false
	hedgeReadAfter = 1500 * time.Millisecond
	// putPolicy judges an upload on the last second of acknowledgements, twice,
	// and discards at most putMaxDiscards sockets. BEAM_WORKER_PUT_POLICY=0
	// restores the cumulative rate and the five-discard cap.
	putPolicy      = true
	putMaxDiscards = 2
	putWindow      = time.Second
	// putSndBuf 0 leaves TCP send-buffer autotuning on. A positive value is passed
	// to SO_SNDBUF, which locks the buffer: Linux returns twice the requested
	// size and will not grow it with RTT. On this host that turns 1 MiB into
	// 2 MiB, below the buffer autotuning already selects. Set
	// BEAM_WORKER_PUT_SNDBUF to compare a locked buffer.
	putSndBuf = 0
	putLinger = true
	// putParallelProbe measures a second upload only after the first second was
	// slow, and only when BEAM_WORKER_PUT_PARALLEL_PROBE=1. It stays off until
	// a same-part concurrent PUT is confirmed on R2.
	putParallelProbe = false
	// putLateProbe starts a second upload when acknowledgements stop for 2s and
	// leaves the first socket up. Off unless BEAM_WORKER_PUT_LATE_PROBE=1.
	putLateProbe = false
	// readMedian restarts a download when the last second would take more than
	// readMedianFactor times as long as the recent per-host rate. The read cap
	// is higher than the fixed 8s budget's cap.
	readMedian       = true
	readRestartCap   = 10
	readMedianFactor = 1.5
)

func init() {
	earlyPut = envBool("BEAM_WORKER_EARLY_PUT", true)
	hedgeRead = envBool("BEAM_WORKER_READ_HEDGE", false)
	putPolicy = envBool("BEAM_WORKER_PUT_POLICY", true)
	putLinger = envBool("BEAM_WORKER_PUT_LINGER", true)
	putParallelProbe = envBool("BEAM_WORKER_PUT_PARALLEL_PROBE", false)
	putLateProbe = envBool("BEAM_WORKER_PUT_LATE_PROBE", false)
	readMedian = envBool("BEAM_WORKER_READ_MEDIAN", true)
	if raw := os.Getenv("BEAM_WORKER_READ_RESTARTS"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed >= 0 {
			readRestartCap = parsed
		}
	}
	if raw := os.Getenv("BEAM_WORKER_PUT_MAX_DISCARDS"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed >= 0 {
			putMaxDiscards = parsed
		}
	}
	if raw := os.Getenv("BEAM_WORKER_PUT_SNDBUF"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed >= 0 {
			putSndBuf = parsed
		}
	}
}

func envBool(key string, fallback bool) bool {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(raw)
	if err != nil {
		return fallback
	}
	return parsed
}
