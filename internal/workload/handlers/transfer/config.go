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
)

func init() {
	earlyPut = envBool("BEAM_WORKER_EARLY_PUT", true)
	hedgeRead = envBool("BEAM_WORKER_READ_HEDGE", false)
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
