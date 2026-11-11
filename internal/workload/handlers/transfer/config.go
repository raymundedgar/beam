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
	// deliverFirst lets one part of a batch read and upload while the others wait.
	deliverFirst = true
	deliverWait  = 5 * time.Second
	// hedgeRead races two identical signed GETs for the batch's first part.
	hedgeRead      = false
	hedgeReadAfter = 1500 * time.Millisecond
)

func init() {
	earlyPut = envBool("BEAM_WORKER_EARLY_PUT", true)
	deliverFirst = envBool("BEAM_WORKER_FIRST_DELIVERER", true)
	if raw := os.Getenv("BEAM_WORKER_FIRST_DELIVERER_WAIT"); raw != "" {
		if parsed, err := time.ParseDuration(raw); err == nil && parsed > 0 {
			deliverWait = parsed
		}
	}
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
