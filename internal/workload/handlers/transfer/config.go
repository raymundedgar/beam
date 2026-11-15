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
	// readRace opens three GETs on different addresses and keeps the one that
	// has moved the most. Off unless BEAM_WORKER_READ_RACE=1.
	readRace  = false
	readRaceN = 3
	warmN     = 2
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
	// readKeepMinRate is the download rate, in bytes per second, that is left
	// alone. A 48 MiB part at 5 MiB/s finishes the read in about 10 seconds.
	// Slower sockets are still replaced. BEAM_WORKER_READ_KEEP_RATE is MiB/s.
	readKeepMinRate int64 = 5 << 20
	// readResetNoETag resets the upload when a 206 drops and the response had
	// no ETag. A matching ETag keeps the prefix either way.
	readResetNoETag = true
	// putETagStrict fails an upload whose R2 part ETag is not the MD5 of the
	// bytes sent. Off logs the mismatch and still accepts the 200.
	putETagStrict = false
	// transferReserveSlots is how many socket connections are reserved for
	// different incoming batches. The FCFS slots are BEAM_WORKER_TRANSFER_CONCURRENCY - transferReserveSlots.
	transferReserveSlots = 2
)

func init() {
	earlyPut = envBool("BEAM_WORKER_EARLY_PUT", true)
	hedgeRead = envBool("BEAM_WORKER_READ_HEDGE", false)
	readRace = envBool("BEAM_WORKER_READ_RACE", false)
	putPolicy = envBool("BEAM_WORKER_PUT_POLICY", true)
	putLinger = envBool("BEAM_WORKER_PUT_LINGER", true)
	putParallelProbe = envBool("BEAM_WORKER_PUT_PARALLEL_PROBE", false)
	putLateProbe = envBool("BEAM_WORKER_PUT_LATE_PROBE", false)
	readMedian = envBool("BEAM_WORKER_READ_MEDIAN", true)
	readResetNoETag = envBool("BEAM_WORKER_READ_RESET_NO_ETAG", true)
	putETagStrict = envBool("BEAM_WORKER_PUT_ETAG_STRICT", false)
	envNonNegative("BEAM_WORKER_READ_PROBE_REPLACEMENTS", &maxSourceReplacements)
	envAtLeastOne("BEAM_WORKER_HTTP_ATTEMPTS", &presignedAttempts)
	if raw := os.Getenv("BEAM_WORKER_READ_KEEP_RATE"); raw != "" {
		if parsed, err := strconv.ParseFloat(raw, 64); err == nil && parsed >= 0 {
			readKeepMinRate = int64(parsed * (1 << 20))
		}
	}
	if raw := os.Getenv("BEAM_WORKER_READ_RESTARTS"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed >= 0 {
			readRestartCap = parsed
		}
	}
	if raw := os.Getenv("BEAM_WORKER_PUT_HOLD_MAX"); raw != "" {
		switch raw {
		case "0", "off":
			putHoldCap = 0
		default:
			if parsed, err := time.ParseDuration(raw); err == nil && parsed >= 0 {
				putHoldCap = parsed
			} else if secs, err := strconv.Atoi(raw); err == nil && secs >= 0 {
				putHoldCap = time.Duration(secs) * time.Second
			}
		}
	}
	uploadPriority = envBool("BEAM_WORKER_UPLOAD_PRIORITY", false)
	if raw := os.Getenv("BEAM_WORKER_UPLOAD_PRIORITY_AGE"); raw != "" {
		if parsed, err := time.ParseDuration(raw); err == nil && parsed >= 0 {
			uploadPriorityAge = parsed
		} else if secs, err := strconv.Atoi(raw); err == nil && secs >= 0 {
			uploadPriorityAge = time.Duration(secs) * time.Second
		}
	}
	if raw := os.Getenv("BEAM_WORKER_TRANSFER_RESERVE"); raw != "" {
		if raw == "0" || raw == "off" || raw == "false" {
			transferReserveSlots = 0
		} else if parsed, err := strconv.Atoi(raw); err == nil && parsed >= 0 {
			transferReserveSlots = parsed
		}
	} else if raw := os.Getenv("BEAM_WORKER_RESERVED_SLOTS"); raw != "" {
		if raw == "0" || raw == "off" || raw == "false" {
			transferReserveSlots = 0
		} else if parsed, err := strconv.Atoi(raw); err == nil && parsed >= 0 {
			transferReserveSlots = parsed
		}
	} else if raw := os.Getenv("BEAM_WORKER_BATCH_RESERVE"); raw != "" {
		if raw == "0" || raw == "off" || raw == "false" {
			transferReserveSlots = 0
		} else if parsed, err := strconv.Atoi(raw); err == nil && parsed >= 0 {
			transferReserveSlots = parsed
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
	if raw := os.Getenv("BEAM_WORKER_EARLY_PUT_MIN_BYTES"); raw != "" {
		if raw == "0" || raw == "off" {
			earlyPut = false
		} else if parsed, err := strconv.ParseInt(raw, 10, 64); err == nil && parsed > 0 {
			earlyPutBytes = parsed
		}
	}
}

func envNonNegative(key string, dst *int) {
	raw := os.Getenv(key)
	if raw == "" || dst == nil {
		return
	}
	parsed, err := strconv.Atoi(raw)
	if err != nil || parsed < 0 {
		return
	}
	*dst = parsed
}

func envAtLeastOne(key string, dst *int) {
	raw := os.Getenv(key)
	if raw == "" || dst == nil {
		return
	}
	parsed, err := strconv.Atoi(raw)
	if err != nil || parsed < 1 {
		return
	}
	*dst = parsed
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
