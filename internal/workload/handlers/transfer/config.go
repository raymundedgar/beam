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
	putSndBuf      = 1 << 20
	putLinger      = true
	// putParallelProbe keeps the current upload up while a second one is measured.
	// Off unless BEAM_WORKER_PUT_PARALLEL_PROBE=1.
	putParallelProbe = false
)

func init() {
	earlyPut = envBool("BEAM_WORKER_EARLY_PUT", true)
	hedgeRead = envBool("BEAM_WORKER_READ_HEDGE", false)
	putPolicy = envBool("BEAM_WORKER_PUT_POLICY", true)
	putLinger = envBool("BEAM_WORKER_PUT_LINGER", true)
	putParallelProbe = envBool("BEAM_WORKER_PUT_PARALLEL_PROBE", false)
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
