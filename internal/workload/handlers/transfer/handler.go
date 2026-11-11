package transfer

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	workloadcheckpoint "github.com/Beam-Network/beam/internal/workload/checkpoint"
	"github.com/Beam-Network/beam/internal/workload/contracts"
	"github.com/Beam-Network/beam/internal/workload/domain"
	"github.com/Beam-Network/beam/internal/workload/storagehttp"
)

const defaultResponseBodyLimit = 64 << 10
const multipartCheckpointSchema = "beam.transfer.multipart/1"

// maxResumablePart is the largest presigned object this worker buffers so a
// broken GET can resume with Range and If-Range instead of starting over.
const maxResumablePart = 64 << 20

// presignedAttempts is the number of tries for one presigned part, shared with
// the upstream retry budget.
const presignedAttempts = 3

type multipartCheckpoint struct {
	Parts map[string]partCheckpoint `json:"parts"`
	Bytes int64                     `json:"bytes"`
}

type partCheckpoint struct {
	Bytes          int64  `json:"bytes"`
	SHA256         string `json:"sha256"`
	ETag           string `json:"etag,omitempty"`
	SHA1           string `json:"sha1,omitempty"`
	RequestCount   int    `json:"request_count,omitempty"`
	ProviderPartID string `json:"provider_part_id,omitempty"`
}

// defaultTransferSlots is how many presigned transfers may move bytes at once.
// A wave can be sixteen offers. The offer clock runs while a task waits for a
// slot, and Beam cancels a late offer that has not finished in about 10 seconds.
const defaultTransferSlots = 16

// defaultUploadSlots is how many of those transfers may upload at once.
// Sixteen uploads together left some connections near 3 MiB/s. The same object
// on a quiet connection finished in about 2 seconds.
const defaultUploadSlots = 6

type Handler struct {
	client *http.Client
	// wait pauses between upload attempts.
	wait func(context.Context, time.Duration) error
	// logf records transfer measurements. It is log.Printf unless a test replaces it.
	logf func(string, ...any)
	// slots limits transfers that are reading or writing. Nil means no limit.
	slots chan struct{}
	// uploads limits PUTs. Nil means no separate upload limit.
	uploads chan struct{}
}

func NewHandler(client *http.Client) *Handler {
	if client == nil {
		client = storagehttp.NewClient(30 * time.Minute)
	}
	logPolicy()
	return &Handler{client: client, wait: sleepContext, logf: log.Printf, slots: transferSlots(), uploads: uploadSlots()}
}

func transferSlots() chan struct{} {
	n := defaultTransferSlots
	if raw := os.Getenv("BEAM_WORKER_TRANSFER_CONCURRENCY"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err == nil {
			n = parsed
		}
	}
	if n == 0 {
		n = defaultTransferSlots
	}
	if n < 0 {
		return nil
	}
	return make(chan struct{}, n)
}

func (h *Handler) acquireSlot(ctx context.Context) error {
	if h.slots == nil {
		return nil
	}
	select {
	case h.slots <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (h *Handler) releaseSlot() {
	if h.slots == nil {
		return
	}
	<-h.slots
}

func uploadSlots() chan struct{} {
	n := defaultUploadSlots
	if raw := os.Getenv("BEAM_WORKER_UPLOAD_CONCURRENCY"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err == nil {
			n = parsed
		}
	}
	if n == 0 {
		n = defaultUploadSlots
	}
	if n < 0 {
		return nil
	}
	return make(chan struct{}, n)
}

func (h *Handler) acquireUpload(ctx context.Context) error {
	if h.uploads == nil {
		return nil
	}
	select {
	case h.uploads <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (h *Handler) releaseUpload() {
	if h.uploads == nil {
		return
	}
	<-h.uploads
}

func (h *Handler) Kind() domain.Kind { return domain.KindTransferMultipart }

func (h *Handler) Validate(spec domain.Spec) error {
	var transfer contracts.MultipartTransfer
	if err := json.Unmarshal(spec.Payload, &transfer); err != nil {
		return fmt.Errorf("decode multipart transfer: %w", err)
	}
	if len(transfer.Parts) == 0 {
		return errors.New("at least one transfer part is required")
	}
	if transfer.Fanout {
		if err := validateFanout(transfer, spec); err != nil {
			return err
		}
	}
	seen := make(map[int]struct{}, len(transfer.Parts))
	for _, part := range transfer.Parts {
		if _, exists := seen[part.Index]; exists {
			return fmt.Errorf("duplicate transfer part index %d", part.Index)
		}
		seen[part.Index] = struct{}{}
		if err := validateEndpoint("source", part.Source); err != nil {
			return fmt.Errorf("part %d: %w", part.Index, err)
		}
		if err := validateEndpoint("destination", part.Destination); err != nil {
			return fmt.Errorf("part %d: %w", part.Index, err)
		}
		if part.Offset < 0 || part.Length < 0 {
			return fmt.Errorf("part %d: offset and length cannot be negative", part.Index)
		}
		if part.ExpectedSHA256 != "" {
			digest, err := hex.DecodeString(part.ExpectedSHA256)
			if err != nil || len(digest) != sha256.Size {
				return fmt.Errorf("part %d: expected_sha256 must be a hexadecimal SHA-256 digest", part.Index)
			}
		}
		switch {
		case part.Authenticated():
			if err := validateAuthenticatedPart(part, spec.Resources.MemoryBytes); err != nil {
				return fmt.Errorf("part %d: %w", part.Index, err)
			}
		case part.Protocol != "" && part.Protocol != contracts.UploadProtocolPresignedPut:
			return fmt.Errorf("part %d: unsupported destination protocol %q", part.Index, part.Protocol)
		}
	}
	return nil
}

func (h *Handler) Execute(ctx context.Context, spec domain.Spec) (domain.Result, error) {
	if err := h.Validate(spec); err != nil {
		return domain.Result{}, err
	}
	var transfer contracts.MultipartTransfer
	if err := json.Unmarshal(spec.Payload, &transfer); err != nil {
		return domain.Result{}, err
	}
	trace := &transferLog{workload: spec.WorkloadID, attempt: spec.AttemptID, started: time.Now(), printf: h.logf}
	ctx = withTrace(ctx, trace)
	trace.admitted(len(transfer.Parts))
	h.warmHosts(ctx, transfer.Parts)
	leader := true
	ctx = withLeader(ctx, leader)
	defer func() {
		trace.reported(spec.BatchID, leader, ctx.Err() != nil)
	}()
	if transfer.Fanout {
		return h.executeFanout(ctx, transfer)
	}
	resume := multipartCheckpoint{Parts: make(map[string]partCheckpoint)}
	if _, _, err := workloadcheckpoint.Current(ctx, multipartCheckpointSchema, &resume); err != nil {
		return domain.Result{}, fmt.Errorf("restore multipart checkpoint: %w", err)
	}
	if resume.Parts == nil {
		resume.Parts = make(map[string]partCheckpoint)
	}
	outputs := make(map[string]string, len(transfer.Parts)*2)
	var total = resume.Bytes
	pending := make([]contracts.TransferPart, 0, len(transfer.Parts))
	for _, part := range transfer.Parts {
		partKey := strconv.Itoa(part.Index)
		prefix := "part." + partKey + "."
		if completed, ok := resume.Parts[partKey]; ok {
			partResult{sha256: completed.SHA256, etag: completed.ETag, sha1: completed.SHA1,
				requests: completed.RequestCount, providerPartID: completed.ProviderPartID}.record(outputs, prefix, part)
			continue
		}
		pending = append(pending, part)
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	gate := newMemoryGate(spec.Resources.MemoryBytes)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var failed error
	for _, part := range pending {
		wg.Add(1)
		go func(part contracts.TransferPart) {
			defer wg.Done()
			held, acquireErr := gate.acquire(runCtx, bufferedBytes(part.Length))
			if acquireErr != nil {
				mu.Lock()
				if failed == nil {
					failed = fmt.Errorf("part %d: %w", part.Index, acquireErr)
				}
				mu.Unlock()
				return
			}
			defer gate.release(held)
			partKey := strconv.Itoa(part.Index)
			prefix := "part." + partKey + "."
			partResult, err := h.executePart(runCtx, part)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				if part.Authenticated() {
					partResult.record(outputs, prefix, part)
					outputs[prefix+"error"] = uploadFailureClass(err)
					recordThrottle(outputs, prefix, err)
				}
				if failed == nil {
					failed = fmt.Errorf("part %d: %w", part.Index, err)
					cancel()
				}
				return
			}
			total += partResult.bytes
			partResult.record(outputs, prefix, part)
			resume.Bytes = total
			resume.Parts[partKey] = partCheckpoint{Bytes: partResult.bytes, SHA256: partResult.sha256, ETag: partResult.etag,
				SHA1: partResult.sha1, RequestCount: partResult.requests, ProviderPartID: partResult.providerPartID}
			if err := workloadcheckpoint.Save(ctx, multipartCheckpointSchema, map[string]string{
				"completed_part": partKey,
			}, resume); err != nil && !errors.Is(err, workloadcheckpoint.ErrUnavailable) && failed == nil {
				failed = fmt.Errorf("save multipart checkpoint: %w", err)
				cancel()
				return
			}
			trace.partDone(part.Index, partResult.bytes)
		}(part)
	}
	wg.Wait()
	if failed != nil {
		return domain.Result{BytesProcessed: total, Outputs: outputs}, failed
	}
	return domain.Result{BytesProcessed: total, Outputs: outputs}, nil
}

type partResult struct {
	bytes          int64
	sha256         string
	etag           string
	sha1           string
	requests       int
	providerPartID string
}

// record writes a part's evidence outputs. Authenticated destinations always report
// their request count, and their sha1 once the range was read.
func (r partResult) record(outputs map[string]string, prefix string, part contracts.TransferPart) {
	if r.sha256 != "" {
		outputs[prefix+"sha256"] = r.sha256
	}
	if r.etag != "" {
		outputs[prefix+"etag"] = r.etag
	}
	if !part.Authenticated() {
		return
	}
	outputs[prefix+"request_count"] = strconv.Itoa(r.requests)
	if r.sha1 != "" {
		outputs[prefix+"sha1"] = r.sha1
	}
	if r.providerPartID != "" {
		outputs[prefix+"provider_part_id"] = r.providerPartID
	}
}

func (h *Handler) executePart(ctx context.Context, part contracts.TransferPart) (partResult, error) {
	if err := h.acquireSlot(ctx); err != nil {
		return partResult{}, err
	}
	enterTransfer()
	defer leaveTransfer()
	if part.Authenticated() {
		defer h.releaseSlot()
		return h.executeAuthenticatedPart(ctx, part)
	}
	if part.Length > 0 && part.Length <= maxResumablePart {
		// The slot covers the download only. Holding it through a slow upload
		// left the rest of the wave waiting until Beam cancelled those offers.
		// A part whose hash must be checked before the first uploaded byte stays
		// serial. Everything else can upload once the read is fast enough to keep.
		if part.ExpectedSHA256 == "" {
			return h.transferOverlapped(ctx, part)
		}
		body, err := h.readSource(ctx, part)
		h.releaseSlot()
		if err != nil {
			return partResult{bytes: int64(len(body))}, err
		}
		return h.putPresigned(ctx, part, body)
	}
	defer h.releaseSlot()
	return h.retryPresigned(ctx, func() (partResult, error) {
		return h.executePresignedPart(ctx, part)
	})
}

func (h *Handler) readSource(ctx context.Context, part contracts.TransferPart) ([]byte, error) {
	return h.readSourcePublish(ctx, part, nil)
}

func (h *Handler) readSourcePublish(ctx context.Context, part contracts.TransferPart, publish func(*followBuffer)) ([]byte, error) {
	if readRace {
		return h.readIPRace(ctx, part, publish, readRaceN, true)
	}
	if hedgeRead && isLeader(ctx) {
		return h.readSourceRace(ctx, part, publish)
	}
	var lastErr error
	httpAttempts := 0
	replacements := 0
	var pace readPace
	var stamp sourceStamp
	var buf *followBuffer
	if publish != nil {
		buf = newFollowBuffer(part.Length)
		publish(buf)
	}
	for {
		if err := ctx.Err(); err != nil {
			if buf != nil {
				buf.fail(err)
			}
			return nil, err
		}
		// Restart from the signed request. Adding Range or If-Range changes a
		// presigned URL and R2 answers 403 SignatureDoesNotMatch, which the
		// worker would report as its own failure.
		// A slow connection is closed and the signed GET is repeated. Bytes
		// already buffered stay, and the upload keeps reading them.
		body, err := h.readSourceOnce(ctx, part, replacements < maxSourceReplacements, replacements < readRestartCap && !pace.done(), buf, &pace, &stamp)
		if err == nil {
			if buf != nil {
				buf.finish()
			}
			return body, nil
		}
		if ctx.Err() != nil {
			if buf != nil {
				buf.fail(ctx.Err())
			}
			return nil, ctx.Err()
		}
		lastErr = err
		if errors.Is(err, errReplaced) || errors.Is(err, errReadRetry) {
			if errors.Is(err, errReadRetry) {
				traceFrom(ctx).readRetry(bufLen(buf))
				httpAttempts++
				if httpAttempts >= presignedAttempts {
					return nil, err
				}
			} else {
				replacements++
			}
			continue
		}
		if errors.Is(err, errSourceReset) {
			traceFrom(ctx).sourceReset(resetReason(err))
			if buf != nil {
				buf.fail(err)
			}
			buf = newFollowBuffer(part.Length)
			stamp = sourceStamp{}
			if publish != nil {
				publish(buf)
			}
			httpAttempts++
			if httpAttempts >= presignedAttempts {
				return nil, err
			}
			if waitErr := h.backoff(ctx, httpAttempts); waitErr != nil {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				return nil, err
			}
			continue
		}
		if errors.Is(err, errStalled) {
			if waitErr := h.backoff(ctx, 1); waitErr != nil {
				<-ctx.Done()
				if buf != nil {
					buf.fail(ctx.Err())
				}
				return nil, ctx.Err()
			}
			continue
		}
		httpAttempts++
		if !transientTransferError(err) || httpAttempts >= presignedAttempts {
			if buf != nil {
				buf.fail(err)
			}
			return nil, err
		}
		// A failed attempt is not a prefix worth keeping. Clearing the slice
		// in place would leave the upload's read offset past the new bytes, so
		// an attached upload is aborted and a new buffer is published instead.
		if publish != nil && buf != nil {
			traceFrom(ctx).sourceReset("retry")
			buf.fail(err)
			buf = newFollowBuffer(part.Length)
			stamp = sourceStamp{}
			publish(buf)
		} else if buf != nil {
			buf.reset()
		}
		if waitErr := h.backoff(ctx, httpAttempts); waitErr != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, lastErr
		}
	}
}

func (h *Handler) readSourceOnce(ctx context.Context, part contracts.TransferPart, allowReplace, allowMedian bool, buf *followBuffer, pace *readPace, stamp *sourceStamp) ([]byte, error) {
	started := time.Now()
	counter := &byteCounter{onFirst: func() { traceFrom(ctx).moved(part.Index, "source") }}
	var expected atomic.Int64
	expected.Store(part.Length)
	attemptCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(context.Canceled)
	stallCtx, stop := watchStall(attemptCtx, &counter.n, &expected)
	defer stop()
	if allowReplace && part.Length >= hedgeMinBytes {
		// Copied before the goroutine so tests can change the package values.
		probe, budget, minBytes := hedgeAfter, sourceBudget, hedgeMinBytes
		go watchSourceRate(attemptCtx, cancel, &counter.n, part.Length, probe, budget, minBytes)
	}
	if allowMedian && readMedian && part.Length >= hedgeMinBytes {
		have := func() int64 {
			if buf != nil {
				return buf.Len()
			}
			return counter.n.Load()
		}
		go watchReadMedian(attemptCtx, cancel, &counter.n, have, part.Length, hostOf(part.Source.URL), pace)
	}
	body, err := h.readBody(ctx, stallCtx, part, counter, started, buf, stamp)
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if context.Cause(attemptCtx) == errReplaced {
		return nil, errReplaced
	}
	return body, err
}

func (h *Handler) readBody(parent, attempt context.Context, part contracts.TransferPart, counter *byteCounter, started time.Time, buf *followBuffer, stamp *sourceStamp) ([]byte, error) {
	var peer atomic.Value
	response, err := h.openSourceFrom(attempt, part, 0, "", &peer)
	if err != nil {
		traceFrom(parent).pace(part.Index, "source", 0, counter.n.Load(), time.Since(started), counter.blockedFor(), counter.longestGap(), 0, counter.waitedInRead(), part.Source.Region)
		if reset := resetFromStatus(err); reset != nil {
			return nil, reset
		}
		if parent.Err() != nil {
			return nil, parent.Err()
		}
		if stalled(parent, attempt) {
			traceFrom(parent).stalled(part.Index, "source", counter.n.Load())
			return nil, errStalled
		}
		return nil, err
	}
	respWait := time.Since(started)
	defer response.Body.Close()
	if err := stamp.observe(response); err != nil {
		return nil, err
	}
	counter.r = response.Body
	var body []byte
	var readErr error
	if buf == nil {
		chunk := make([]byte, part.Length)
		var n int
		n, readErr = io.ReadFull(counter, chunk)
		body = chunk[:n]
	} else {
		if buf.Len() > 0 {
			traceFrom(parent).readSkip(buf.Len())
		}
		body, readErr = readInto(counter, buf, part.Length, attempt)
	}
	traceFrom(parent).pace(part.Index, "source", response.StatusCode, int64(len(body)), time.Since(started), counter.blockedFor(), counter.longestGap(), respWait, counter.waitedInRead(), part.Source.Region)
	if conn, _ := peer.Load().(net.Conn); conn != nil {
		traceFrom(parent).readPeer(remoteIP(conn), ipFamilyOf(remoteIP(conn)), cfColo(response.Header), int64(len(body)), time.Since(started))
	}
	if int64(len(body)) == part.Length && part.Length > 0 {
		elapsed := time.Since(started).Seconds()
		if elapsed > 0 {
			readHosts.observe(hostOf(part.Source.URL), float64(len(body))/elapsed)
		}
	}
	if int64(len(body)) == part.Length {
		return body, nil
	}
	if parent.Err() != nil {
		return nil, parent.Err()
	}
	if cause := context.Cause(attempt); cause == errReplaced || cause == errSourceReset {
		return body, cause
	}
	if stalled(parent, attempt) {
		traceFrom(parent).stalled(part.Index, "source", int64(len(body)))
		return nil, errStalled
	}
	return nil, classifyShortRead(response.StatusCode, stamp, readErr)
}

// classifyShortRead keeps a validated 206 prefix when the connection drops.
// An invalid response, or a 206 with no ETag while that reset is enabled,
// still aborts the upload.
func classifyShortRead(status int, stamp *sourceStamp, readErr error) error {
	reason := "read_error"
	if readErr == nil || errors.Is(readErr, io.EOF) || errors.Is(readErr, io.ErrUnexpectedEOF) {
		reason = "short_body"
	}
	if status == http.StatusPartialContent && stamp != nil && stamp.set {
		if stamp.etag != "" {
			return errReadRetry
		}
		if !readResetNoETag {
			return errReadRetry
		}
		return &resetError{reason: "no_etag"}
	}
	return &resetError{reason: reason}
}

func bufLen(buf *followBuffer) int64 {
	if buf == nil {
		return 0
	}
	return buf.Len()
}

// uploadRef returns a short hash of the multipart upload id and the part
// number. The signed query string is not returned.
func uploadRef(raw string) (id, part string) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", ""
	}
	query := parsed.Query()
	part = query.Get("partNumber")
	if part == "" {
		part = query.Get("part_number")
	}
	uid := query.Get("uploadId")
	if uid == "" {
		uid = query.Get("upload_id")
	}
	if uid == "" {
		return "", part
	}
	sum := sha256.Sum256([]byte(uid))
	return hex.EncodeToString(sum[:4]), part
}

// bodyOpen returns a reader positioned at the first byte and, when the read
// can block on a download that is still in progress, a flag the acknowledgement
// watcher treats as "not the socket's fault".
type bodyOpen func(context.Context) (io.Reader, func() bool)

func (h *Handler) putPresigned(ctx context.Context, part contracts.TransferPart, body []byte) (partResult, error) {
	sum := sha256.Sum256(body)
	digest := hex.EncodeToString(sum[:])
	if part.ExpectedSHA256 != "" && !strings.EqualFold(digest, part.ExpectedSHA256) {
		return partResult{bytes: int64(len(body)), sha256: digest}, fmt.Errorf("SHA-256 mismatch: got %s", digest)
	}
	return h.putStream(ctx, part, func(context.Context) (io.Reader, func() bool) {
		return bytes.NewReader(body), nil
	}, int64(len(body)), digest)
}

func (h *Handler) putStream(ctx context.Context, part contracts.TransferPart, open bodyOpen, size int64, digest string) (partResult, error) {
	// Waiting here, instead of starting every PUT in the wave, keeps each
	// upload on a connection that can finish.
	slotBegan := time.Now()
	if err := h.acquireUpload(ctx); err != nil {
		return partResult{bytes: size}, err
	}
	traceFrom(ctx).addSlotWait(time.Since(slotBegan))
	defer h.releaseUpload()
	if putParallelProbe && size >= hedgeMinBytes {
		return h.putParallel(ctx, part, open, size, digest)
	}
	if putLateProbe && size >= hedgeMinBytes {
		replaced := 0
		pace := &putPace{}
		return h.retryPresigned(ctx, func() (partResult, error) {
			if pace.stopped() || !replaceSlowUpload(ctx, replaced) {
				return h.putPresignedOnce(ctx, part, open, size, digest, false, nil, pace, nil)
			}
			result, err := h.putLate(ctx, part, open, size, digest)
			if errors.Is(err, errReplaced) {
				replaced++
			}
			return result, err
		})
	}
	replaced := 0
	etagTries := 0
	pace := &putPace{}
	var waiting atomic.Bool
	open = wrapStarved(open, &waiting)
	return h.retryPresigned(ctx, func() (partResult, error) {
		allow := !pace.stopped() && replaceSlowUpload(ctx, replaced)
		// A second upload is opened only when the first is still below the
		// acknowledgement floor halfway through the probe, and only while two
		// discards remain. Fast uploads never open it. The last attempt stays
		// single so a path that is slow on every socket still delivers.
		if allow && size >= hedgeMinBytes && replaceSlowUpload(ctx, replaced+1) {
			result, err, used := h.putPresignedHedged(ctx, part, open, size, digest, waiting.Load)
			replaced += used
			return result, err
		}
		result, err := h.putPresignedOnce(ctx, part, open, size, digest, allow, nil, pace, nil)
		if putETagStrict && errors.Is(err, errETagMismatch) && etagTries == 0 {
			etagTries++
			result, err = h.putPresignedOnce(ctx, part, open, size, digest, allow, nil, pace, nil)
		}
		if errors.Is(err, errReplaced) {
			replaced++
		}
		return result, err
	})
}

func wrapStarved(open bodyOpen, waiting *atomic.Bool) bodyOpen {
	return func(ctx context.Context) (io.Reader, func() bool) {
		reader, paused := open(ctx)
		if paused == nil {
			return reader, nil
		}
		return reader, func() bool {
			starved := paused()
			if waiting != nil {
				waiting.Store(starved)
			}
			return starved
		}
	}
}

// putPresignedHedged starts one upload and, if it has not acknowledged at the
// minimum rate by half the probe, starts a second with the same bytes. Either
// acceptance cancels the other. A discard from the slow socket does not cancel
// the one that is still running. used counts connections that were discarded.
func (h *Handler) putPresignedHedged(ctx context.Context, part contracts.TransferPart, open bodyOpen, size int64, digest string, starved func() bool) (partResult, error, int) {
	attemptCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(context.Canceled)
	type outcome struct {
		result partResult
		err    error
	}
	ch := make(chan outcome, 2)
	var progress atomic.Int64
	go func() {
		result, err := h.putPresignedOnce(attemptCtx, part, open, size, digest, true, &progress, nil, nil)
		ch <- outcome{result, err}
	}()
	hedgeAt := putAckQuiet / 2
	if hedgeAt < time.Millisecond {
		hedgeAt = time.Millisecond
	}
	timer := time.NewTimer(hedgeAt)
	defer timer.Stop()
	pending := 1
	spare := false
	replaced := 0
	var last partResult
	var lastErr error
	for pending > 0 {
		select {
		case <-ctx.Done():
			return last, ctx.Err(), replaced
		case <-timer.C:
			if spare {
				continue
			}
			if starved != nil && starved() {
				continue
			}
			spare = true
			// No sample yet stays at zero. A socket that has already passed
			// the floor does not need a spare.
			if progress.Load() >= putAckMinRate*int64(hedgeAt)/int64(time.Second) {
				continue
			}
			pending++
			go func() {
				result, err := h.putPresignedOnce(attemptCtx, part, open, size, digest, true, nil, nil, nil)
				ch <- outcome{result, err}
			}()
		case o := <-ch:
			pending--
			if o.err == nil {
				cancel(errReplaced)
				return o.result, nil, replaced
			}
			if errors.Is(o.err, errReplaced) {
				replaced++
				if lastErr == nil {
					last, lastErr = o.result, o.err
				}
				continue
			}
			last, lastErr = o.result, o.err
		}
	}
	return last, lastErr, replaced
}

// retryPresigned repeats a presigned attempt. HTTP 408, 429, and 5xx stop after
// presignedAttempts. A stall retries until the offer context ends, and that end
// is the returned error, so the task is cancelled rather than failed.
func (h *Handler) retryPresigned(ctx context.Context, attemptFn func() (partResult, error)) (partResult, error) {
	var last partResult
	var lastErr error
	httpAttempts := 0
	for {
		if err := ctx.Err(); err != nil {
			return last, err
		}
		last, lastErr = attemptFn()
		if lastErr == nil {
			return last, nil
		}
		if ctx.Err() != nil {
			return last, ctx.Err()
		}
		if errors.Is(lastErr, errStalled) || errors.Is(lastErr, errReplaced) {
			if waitErr := h.backoff(ctx, 1); waitErr != nil {
				<-ctx.Done()
				return last, ctx.Err()
			}
			continue
		}
		httpAttempts++
		if !transientTransferError(lastErr) || httpAttempts >= presignedAttempts {
			return last, lastErr
		}
		if waitErr := h.backoff(ctx, httpAttempts); waitErr != nil {
			if ctx.Err() != nil {
				return last, ctx.Err()
			}
			return last, lastErr
		}
	}
}

func (h *Handler) putPresignedOnce(ctx context.Context, part contracts.TransferPart, open bodyOpen, size int64, digest string, allowReplace bool, progress *atomic.Int64, pace *putPace, onLate func()) (partResult, error) {
	destinationMethod := strings.ToUpper(strings.TrimSpace(part.Destination.Method))
	if destinationMethod == "" {
		destinationMethod = http.MethodPut
	}
	started := time.Now()
	attemptCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(context.Canceled)
	reader, paused := open(attemptCtx)
	starvedReader, _ := reader.(*followReader)
	sentMD5 := md5.New()
	counter := &byteCounter{r: io.TeeReader(reader, sentMD5), onFirst: func() { traceFrom(ctx).moved(part.Index, "destination") }}
	defer func() {
		if starvedReader != nil {
			traceFrom(ctx).addSlotStarved(starvedReader.starvedFor())
		}
	}()
	var held atomic.Value
	var ackBytes atomic.Int64
	var ackSeen atomic.Bool
	var ackMoved atomic.Int64
	var ackBase atomic.Int64
	ackMoved.Store(time.Now().UnixNano())
	uploadID, partNo := uploadRef(part.Destination.URL)
	if size >= hedgeMinBytes {
		quiet, minRate := putAckQuiet, putAckMinRate
		total := size
		var stallCancel context.CancelCauseFunc
		if allowReplace {
			stallCancel = cancel
		}
		sampleAck := func() (int64, bool) {
			conn, _ := held.Load().(net.Conn)
			if conn == nil {
				return 0, false
			}
			n, ok := bytesAcked(conn)
			if !ok {
				return 0, false
			}
			if ackSeen.CompareAndSwap(false, true) {
				ackBase.Store(n)
				ackMoved.Store(time.Now().UnixNano())
			} else if n != ackBytes.Load() {
				ackMoved.Store(time.Now().UnixNano())
				traceFrom(ctx).firstAck()
			}
			if ackSeen.Load() && n > ackBase.Load() {
				traceFrom(ctx).firstAck()
			}
			ackBytes.Store(n)
			if progress != nil {
				delta := n - ackBase.Load()
				if delta < 0 {
					delta = 0
				}
				progress.Store(delta)
			}
			return n, true
		}
		snapNow := func() tcpSnap {
			conn, _ := held.Load().(net.Conn)
			return readTCP(conn)
		}
		if putPolicy {
			go watchPutRecent(attemptCtx, stallCancel, total, putWindow, minRate, sampleAck, snapNow, paused, pace,
				func(rate float64, n int64) {
					traceFrom(ctx).putSample(uploadID, partNo, rate, n)
				},
				func(rate float64, n int64, snap tcpSnap, decision string) {
					traceFrom(ctx).putWindow(uploadID, partNo, snap, rate, decision)
					if decision == "hold_rwnd" {
						return
					}
					traceFrom(ctx).replaced(part.Index, "destination", "ack_stall", counter.n.Load(), int64(rate))
					traceFrom(ctx).putDiscard(uploadID, partNo, decision, rate, n, snap)
				}, onLate)
		} else {
			go watchPutAck(attemptCtx, stallCancel, &counter.n, total, quiet, minRate, sampleAck, func(written, acked int64) {
				traceFrom(ctx).replaced(part.Index, "destination", "ack_stall", written, acked)
			}, paused)
		}
	}
	destinationRequest, err := http.NewRequestWithContext(attemptCtx, destinationMethod, part.Destination.URL, counter)
	if err != nil {
		return partResult{}, fmt.Errorf("create destination request: %w", err)
	}
	destinationRequest = destinationRequest.WithContext(httptrace.WithClientTrace(attemptCtx, &httptrace.ClientTrace{
		GotConn: func(info httptrace.GotConnInfo) {
			if info.Conn != nil {
				tuneUploadSocket(info.Conn)
				held.Store(info.Conn)
			}
		},
	}))
	copyHeaders(destinationRequest.Header, part.Destination.Headers)
	if destinationRequest.Header.Get("Content-Type") == "" {
		destinationRequest.Header.Set("Content-Type", "application/octet-stream")
	}
	destinationRequest.ContentLength = size
	destinationResponse, err := h.client.Do(destinationRequest)
	respWait := time.Duration(0)
	if !counter.last.IsZero() {
		respWait = time.Since(counter.last)
	}
	acked := int64(0)
	if ackSeen.Load() {
		acked = ackBytes.Load() - ackBase.Load()
		if acked < 0 {
			acked = 0
		}
	}
	ackAge := time.Duration(0)
	if ackSeen.Load() {
		ackAge = time.Since(time.Unix(0, ackMoved.Load()))
	}
	traceFrom(ctx).pacePut(part.Index, statusCode(destinationResponse), counter.n.Load(), time.Since(started), counter.blockedFor(), counter.longestGap(), respWait, counter.waitedInRead(), acked, ackSeen.Load(), ackAge, uploadID, partNo, part.Destination.Region)
	var responseHeader http.Header
	if destinationResponse != nil {
		responseHeader = destinationResponse.Header
	}
	traceFrom(ctx).putSlow(bucketOf(part.Destination.URL), uploadID, partNo, time.Since(started), statusCode(destinationResponse), responseHeader)
	conn, _ := held.Load().(net.Conn)
	snap := readTCP(conn)
	if err != nil {
		// A failed attempt must not be the next idle socket. The pool is
		// last-in, so it would be handed to the retry ahead of a fast socket
		// another task just released.
		releaseResponse(destinationResponse, conn, true)
		if ctx.Err() != nil {
			return partResult{bytes: size}, ctx.Err()
		}
		switch context.Cause(attemptCtx) {
		case errReplaced:
			return partResult{bytes: counter.n.Load()}, errReplaced
		case errSourceReset:
			return partResult{bytes: counter.n.Load()}, errSourceReset
		}
		return partResult{bytes: size}, fmt.Errorf("write destination: %w", err)
	}
	drop := dropSlowUpload(destinationResponse.StatusCode, acked, ackSeen.Load(), time.Since(started), size)
	reason := "ok"
	switch {
	case destinationResponse.StatusCode >= 500:
		reason = "http_5xx"
	case drop:
		reason = "slow"
	}
	starvedFor := time.Duration(0)
	if starvedReader != nil {
		starvedFor = starvedReader.starvedFor()
	}
	traceFrom(ctx).putAttempt(remoteIP(conn), cfColo(destinationResponse.Header), bucketOf(part.Destination.URL), uploadID, partNo, reason, snap, acked, starvedFor)
	releaseResponse(destinationResponse, conn, drop)
	if destinationResponse.StatusCode < 200 || destinationResponse.StatusCode >= 300 {
		return partResult{bytes: size}, &statusError{label: "destination", status: destinationResponse.StatusCode}
	}
	etag := strings.Trim(destinationResponse.Header.Get("ETag"), `"`)
	sent := hex.EncodeToString(sentMD5.Sum(nil))
	if etag != "" && !strings.Contains(etag, "-") && !strings.EqualFold(etag, sent) {
		log.Printf("ERROR transfer etag_mismatch upload=%s part_no=%s etag=%s md5=%s", uploadID, partNo, etag, sent)
		traceFrom(ctx).etagMismatch(uploadID, partNo, etag, sent)
		if putETagStrict {
			return partResult{bytes: size, sha256: digest, etag: etag}, errETagMismatch
		}
	}
	return partResult{
		bytes: size, sha256: digest,
		etag: etag,
	}, nil
}

// dropSlowUpload reports that this upload socket should not be reused. A 5xx
// or an acknowledgement rate below the keep line leaves the next request on
// the fast idle socket a sibling just released, instead of this one.
func dropSlowUpload(status int, acked int64, known bool, dur time.Duration, size int64) bool {
	if status >= 500 {
		return true
	}
	if !known || acked < 0 || dur <= 0 || size < hedgeMinBytes {
		return false
	}
	return float64(acked)/dur.Seconds() < float64(putAckMinRate)
}

// releaseResponse reads and closes the body. drop closes the TCP connection
// first so the transport cannot put it back in the idle pool.
func releaseResponse(resp *http.Response, conn net.Conn, drop bool) {
	if drop && conn != nil {
		discardUploadSocket(conn)
	}
	if resp == nil || resp.Body == nil {
		return
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, defaultResponseBodyLimit))
	_ = resp.Body.Close()
}

func (h *Handler) backoff(ctx context.Context, attempt int) error {
	delay := time.Duration(attempt) * uploadRetryBackoff
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) <= delay {
		return context.DeadlineExceeded
	}
	return h.wait(ctx, delay)
}

func statusCode(response *http.Response) int {
	if response == nil {
		return 0
	}
	return response.StatusCode
}

func (h *Handler) executePresignedPart(ctx context.Context, part contracts.TransferPart) (partResult, error) {
	started := time.Now()
	sourceCounter := &byteCounter{onFirst: func() { traceFrom(ctx).moved(part.Index, "source") }}
	var expected atomic.Int64
	expected.Store(part.Length)
	attemptCtx, stop := watchStall(ctx, &sourceCounter.n, &expected)
	defer stop()
	sourceResponse, err := h.openSource(attemptCtx, part)
	if err != nil {
		if ctx.Err() != nil {
			return partResult{}, ctx.Err()
		}
		if stalled(ctx, attemptCtx) {
			traceFrom(ctx).stalled(part.Index, "source", sourceCounter.n.Load())
			return partResult{}, fmt.Errorf("fetch source: %w", errStalled)
		}
		return partResult{}, err
	}
	defer sourceResponse.Body.Close()

	hash := sha256.New()
	sourceCounter.r = sourceResponse.Body
	var source io.Reader = sourceCounter
	if part.Length > 0 {
		source = io.LimitReader(source, part.Length)
	}
	counter := &countingReader{reader: io.TeeReader(source, hash)}
	destinationMethod := strings.ToUpper(strings.TrimSpace(part.Destination.Method))
	if destinationMethod == "" {
		destinationMethod = http.MethodPut
	}
	destinationRequest, err := http.NewRequestWithContext(attemptCtx, destinationMethod, part.Destination.URL, counter)
	if err != nil {
		return partResult{}, fmt.Errorf("create destination request: %w", err)
	}
	copyHeaders(destinationRequest.Header, part.Destination.Headers)
	if destinationRequest.Header.Get("Content-Type") == "" {
		destinationRequest.Header.Set("Content-Type", "application/octet-stream")
	}
	if part.Length > 0 {
		destinationRequest.ContentLength = part.Length
	} else if sourceResponse.ContentLength >= 0 {
		destinationRequest.ContentLength = sourceResponse.ContentLength
	}
	destinationResponse, err := h.client.Do(destinationRequest)
	// This path copies the source into the destination in one request. blocked is
	// time the upload stopped pulling bytes; read_wait is time spent reading the source.
	traceFrom(ctx).pace(part.Index, "stream", statusCode(destinationResponse), sourceCounter.n.Load(), time.Since(started), sourceCounter.blockedFor(), sourceCounter.longestGap(), 0, sourceCounter.waitedInRead(), part.Destination.Region)
	if err != nil {
		if ctx.Err() != nil {
			return partResult{bytes: counter.count}, ctx.Err()
		}
		if stalled(ctx, attemptCtx) {
			traceFrom(ctx).stalled(part.Index, "source", sourceCounter.n.Load())
			return partResult{bytes: counter.count}, fmt.Errorf("fetch source: %w", errStalled)
		}
		return partResult{bytes: counter.count}, fmt.Errorf("write destination: %w", err)
	}
	defer destinationResponse.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(destinationResponse.Body, defaultResponseBodyLimit))
	if destinationResponse.StatusCode < 200 || destinationResponse.StatusCode >= 300 {
		return partResult{bytes: counter.count}, &statusError{label: "destination", status: destinationResponse.StatusCode}
	}
	if part.Length > 0 && counter.count != part.Length {
		return partResult{bytes: counter.count}, fmt.Errorf("source returned %d bytes, expected %d", counter.count, part.Length)
	}
	digest := hex.EncodeToString(hash.Sum(nil))
	if part.ExpectedSHA256 != "" && !strings.EqualFold(digest, part.ExpectedSHA256) {
		return partResult{bytes: counter.count, sha256: digest}, fmt.Errorf("SHA-256 mismatch: got %s", digest)
	}
	return partResult{
		bytes: counter.count, sha256: digest,
		etag: strings.Trim(destinationResponse.Header.Get("ETag"), `"`),
	}, nil
}

// openSource returns a successful source response; the caller closes its body.
func (h *Handler) openSource(ctx context.Context, part contracts.TransferPart) (*http.Response, error) {
	return h.openSourceFrom(ctx, part, 0, "", nil)
}

// openSourceFrom reads part, or the unread tail when resume is past the start.
// A strong etag sends If-Range so a changed object comes back as a full 200
// instead of a 206 that would append bytes from a different version.
func (h *Handler) openSourceFrom(ctx context.Context, part contracts.TransferPart, resume int64, etag string, peer *atomic.Value) (*http.Response, error) {
	sourceRequest, err := http.NewRequestWithContext(ctx, http.MethodGet, part.Source.URL, nil)
	if err != nil {
		return nil, fmt.Errorf("create source request: %w", err)
	}
	if peer != nil {
		sourceRequest = sourceRequest.WithContext(httptrace.WithClientTrace(sourceRequest.Context(), &httptrace.ClientTrace{
			GotConn: func(info httptrace.GotConnInfo) {
				if info.Conn != nil {
					peer.Store(info.Conn)
				}
			},
		}))
	}
	copyHeaders(sourceRequest.Header, part.Source.Headers)
	if spec, ok := sourceRangeHeader(part, resume); ok {
		sourceRequest.Header.Set("Range", spec)
	}
	if resume > 0 && etag != "" {
		sourceRequest.Header.Set("If-Range", etag)
	}
	sourceResponse, err := h.client.Do(sourceRequest)
	if err != nil {
		return nil, fmt.Errorf("fetch source: %w", err)
	}
	if sourceResponse.StatusCode != http.StatusOK && sourceResponse.StatusCode != http.StatusPartialContent {
		defer sourceResponse.Body.Close()
		return nil, responseError("source", sourceResponse)
	}
	if resume == 0 && part.SourceRange && part.Length > 0 && sourceResponse.StatusCode != http.StatusPartialContent {
		_ = sourceResponse.Body.Close()
		return nil, fmt.Errorf("source ignored required byte range: HTTP %d", sourceResponse.StatusCode)
	}
	if resume > 0 && sourceResponse.StatusCode == http.StatusPartialContent {
		if start, ok := contentRangeStart(sourceResponse.Header.Get("Content-Range")); ok && start != rangeStart(part, resume) {
			_ = sourceResponse.Body.Close()
			return nil, fmt.Errorf("source resumed at byte %d, want %d", start, rangeStart(part, resume))
		}
	}
	return sourceResponse, nil
}

func sourceRangeHeader(part contracts.TransferPart, resume int64) (string, bool) {
	if resume < 0 {
		resume = 0
	}
	if part.SourceRange && part.Length > 0 {
		start := part.Offset + resume
		end := part.Offset + part.Length - 1
		if start > end {
			return "", false
		}
		return fmt.Sprintf("bytes=%d-%d", start, end), true
	}
	if resume > 0 && part.Length > 0 && resume < part.Length {
		return fmt.Sprintf("bytes=%d-%d", resume, part.Length-1), true
	}
	return "", false
}

func rangeStart(part contracts.TransferPart, resume int64) int64 {
	if part.SourceRange {
		return part.Offset + resume
	}
	return resume
}

func strongETag(value string) string {
	value = strings.TrimSpace(value)
	if value == "" || strings.HasPrefix(value, "W/") {
		return ""
	}
	return value
}

func contentRangeStart(value string) (int64, bool) {
	value = strings.TrimSpace(value)
	value = strings.TrimPrefix(value, "bytes ")
	dash := strings.IndexByte(value, '-')
	if dash <= 0 {
		return 0, false
	}
	start, err := strconv.ParseInt(value[:dash], 10, 64)
	if err != nil {
		return 0, false
	}
	return start, true
}

func validateEndpoint(label string, endpoint contracts.HTTPEndpoint) error {
	parsed, err := url.Parse(endpoint.URL)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return fmt.Errorf("%s endpoint must be an absolute HTTP(S) URL", label)
	}
	return nil
}

func responseError(label string, response *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(response.Body, defaultResponseBodyLimit))
	return &statusError{label: label, status: response.StatusCode, message: strings.TrimSpace(string(body))}
}

type statusError struct {
	label   string
	status  int
	message string
}

func (e *statusError) Error() string {
	if e.message == "" {
		return fmt.Sprintf("%s returned HTTP %d", e.label, e.status)
	}
	return fmt.Sprintf("%s returned HTTP %d: %s", e.label, e.status, e.message)
}

func (e *statusError) transient() bool {
	return e.status == http.StatusRequestTimeout || e.status == http.StatusTooManyRequests || e.status >= 500
}

func transientTransferError(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var status *statusError
	if errors.As(err, &status) {
		return status.transient()
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	return errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, syscall.ECONNRESET)
}

type resetError struct{ reason string }

func (e *resetError) Error() string {
	if e == nil || e.reason == "" {
		return "source reset"
	}
	return "source reset: " + e.reason
}

func (e *resetError) Is(target error) bool { return target == errSourceReset }

func resetReason(err error) string {
	var reset *resetError
	if errors.As(err, &reset) && reset.reason != "" {
		return reset.reason
	}
	return "reset"
}

func resetFromStatus(err error) error {
	var status *statusError
	if !errors.As(err, &status) {
		return nil
	}
	if status.status < 500 {
		return nil
	}
	return &resetError{reason: fmt.Sprintf("http_%d", status.status)}
}

type sourceStamp struct {
	etag     string
	modified string
	total    int64
	set      bool
}

func (s *sourceStamp) observe(resp *http.Response) error {
	if s == nil || resp == nil {
		return nil
	}
	etag := strings.Trim(resp.Header.Get("ETag"), `"`)
	modified := resp.Header.Get("Last-Modified")
	total, _ := contentRangeTotal(resp.Header.Get("Content-Range"))
	if !s.set {
		s.etag, s.modified, s.total, s.set = etag, modified, total, true
		return nil
	}
	if s.etag != "" && etag != "" && s.etag != etag {
		return &resetError{reason: "etag"}
	}
	if s.modified != "" && modified != "" && s.modified != modified {
		return &resetError{reason: "last_modified"}
	}
	if s.total > 0 && total > 0 && s.total != total {
		return &resetError{reason: "length"}
	}
	return nil
}

func contentRangeTotal(value string) (int64, bool) {
	slash := strings.LastIndex(value, "/")
	if slash < 0 || slash+1 >= len(value) || value[slash+1:] == "*" {
		return 0, false
	}
	n, err := strconv.ParseInt(value[slash+1:], 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

func copyHeaders(destination http.Header, source map[string]string) {
	for key, value := range source {
		destination.Set(key, value)
	}
}

type countingReader struct {
	reader io.Reader
	count  int64
}

func (r *countingReader) Read(buffer []byte) (int, error) {
	n, err := r.reader.Read(buffer)
	r.count += int64(n)
	return n, err
}
