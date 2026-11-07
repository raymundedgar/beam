package transfer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
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
	var lastErr error
	httpAttempts := 0
	replacements := 0
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		// Restart from the signed request. Adding Range or If-Range changes a
		// presigned URL and R2 answers 403 SignatureDoesNotMatch, which the
		// worker would report as its own failure.
		// A slow connection is closed and the signed GET is repeated. Keeping
		// the slow one, and only one extra, left both near 1 MiB/s until Beam
		// cancelled the offer.
		body, err := h.readSourceOnce(ctx, part, replacements < maxSourceReplacements)
		if err == nil {
			return body, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		lastErr = err
		if errors.Is(err, errReplaced) {
			replacements++
			continue
		}
		if errors.Is(err, errStalled) {
			if waitErr := h.backoff(ctx, 1); waitErr != nil {
				<-ctx.Done()
				return nil, ctx.Err()
			}
			continue
		}
		httpAttempts++
		if !transientTransferError(err) || httpAttempts >= presignedAttempts {
			return nil, err
		}
		if waitErr := h.backoff(ctx, httpAttempts); waitErr != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, lastErr
		}
	}
}

func (h *Handler) readSourceOnce(ctx context.Context, part contracts.TransferPart, allowReplace bool) ([]byte, error) {
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
	body, err := h.readBody(ctx, stallCtx, part, counter, started)
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if context.Cause(attemptCtx) == errReplaced {
		return nil, errReplaced
	}
	return body, err
}

func (h *Handler) readBody(parent, attempt context.Context, part contracts.TransferPart, counter *byteCounter, started time.Time) ([]byte, error) {
	response, err := h.openSourceFrom(attempt, part, 0, "")
	if err != nil {
		traceFrom(parent).pace(part.Index, "source", 0, counter.n.Load(), time.Since(started), counter.blockedFor(), counter.longestGap(), 0, counter.waitedInRead())
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
	counter.r = response.Body
	chunk := make([]byte, part.Length)
	n, readErr := io.ReadFull(counter, chunk)
	body := chunk[:n]
	traceFrom(parent).pace(part.Index, "source", response.StatusCode, int64(len(body)), time.Since(started), counter.blockedFor(), counter.longestGap(), respWait, counter.waitedInRead())
	if int64(len(body)) == part.Length {
		return body, nil
	}
	if parent.Err() != nil {
		return nil, parent.Err()
	}
	if stalled(parent, attempt) {
		traceFrom(parent).stalled(part.Index, "source", int64(len(body)))
		return nil, errStalled
	}
	if readErr == nil || readErr == io.EOF {
		readErr = io.ErrUnexpectedEOF
	}
	return nil, fmt.Errorf("fetch source: %w", readErr)
}

func (h *Handler) putPresigned(ctx context.Context, part contracts.TransferPart, body []byte) (partResult, error) {
	sum := sha256.Sum256(body)
	digest := hex.EncodeToString(sum[:])
	if part.ExpectedSHA256 != "" && !strings.EqualFold(digest, part.ExpectedSHA256) {
		return partResult{bytes: int64(len(body)), sha256: digest}, fmt.Errorf("SHA-256 mismatch: got %s", digest)
	}
	// The body is already in memory. Waiting here, instead of starting every
	// PUT in the wave, keeps each upload on a connection that can finish.
	if err := h.acquireUpload(ctx); err != nil {
		return partResult{bytes: int64(len(body))}, err
	}
	defer h.releaseUpload()
	replaced := 0
	return h.retryPresigned(ctx, func() (partResult, error) {
		result, err := h.putPresignedOnce(ctx, part, body, digest, replaced < putReplacements)
		if errors.Is(err, errReplaced) {
			replaced++
		}
		return result, err
	})
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

func (h *Handler) putPresignedOnce(ctx context.Context, part contracts.TransferPart, body []byte, digest string, allowReplace bool) (partResult, error) {
	destinationMethod := strings.ToUpper(strings.TrimSpace(part.Destination.Method))
	if destinationMethod == "" {
		destinationMethod = http.MethodPut
	}
	started := time.Now()
	counter := &byteCounter{r: bytes.NewReader(body), onFirst: func() { traceFrom(ctx).moved(part.Index, "destination") }}
	attemptCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(context.Canceled)
	if allowReplace && int64(len(body)) >= hedgeMinBytes {
		probe, budget, minBytes := putProbe, putBudget, hedgeMinBytes
		go watchPutRate(attemptCtx, cancel, &counter.n, int64(len(body)), probe, budget, minBytes)
	}
	destinationRequest, err := http.NewRequestWithContext(attemptCtx, destinationMethod, part.Destination.URL, counter)
	if err != nil {
		return partResult{}, fmt.Errorf("create destination request: %w", err)
	}
	copyHeaders(destinationRequest.Header, part.Destination.Headers)
	if destinationRequest.Header.Get("Content-Type") == "" {
		destinationRequest.Header.Set("Content-Type", "application/octet-stream")
	}
	destinationRequest.ContentLength = int64(len(body))
	destinationResponse, err := h.client.Do(destinationRequest)
	respWait := time.Duration(0)
	if !counter.last.IsZero() {
		respWait = time.Since(counter.last)
	}
	traceFrom(ctx).pace(part.Index, "destination", statusCode(destinationResponse), counter.n.Load(), time.Since(started), counter.blockedFor(), counter.longestGap(), respWait, counter.waitedInRead())
	if err != nil {
		if ctx.Err() != nil {
			return partResult{bytes: int64(len(body))}, ctx.Err()
		}
		if context.Cause(attemptCtx) == errReplaced {
			return partResult{bytes: counter.n.Load()}, errReplaced
		}
		return partResult{bytes: int64(len(body))}, fmt.Errorf("write destination: %w", err)
	}
	defer destinationResponse.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(destinationResponse.Body, defaultResponseBodyLimit))
	if destinationResponse.StatusCode < 200 || destinationResponse.StatusCode >= 300 {
		return partResult{bytes: int64(len(body))}, &statusError{label: "destination", status: destinationResponse.StatusCode}
	}
	return partResult{
		bytes: int64(len(body)), sha256: digest,
		etag: strings.Trim(destinationResponse.Header.Get("ETag"), `"`),
	}, nil
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
	traceFrom(ctx).pace(part.Index, "stream", statusCode(destinationResponse), sourceCounter.n.Load(), time.Since(started), sourceCounter.blockedFor(), sourceCounter.longestGap(), 0, sourceCounter.waitedInRead())
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
	return h.openSourceFrom(ctx, part, 0, "")
}

// openSourceFrom reads part, or the unread tail when resume is past the start.
// A strong etag sends If-Range so a changed object comes back as a full 200
// instead of a 206 that would append bytes from a different version.
func (h *Handler) openSourceFrom(ctx context.Context, part contracts.TransferPart, resume int64, etag string) (*http.Response, error) {
	sourceRequest, err := http.NewRequestWithContext(ctx, http.MethodGet, part.Source.URL, nil)
	if err != nil {
		return nil, fmt.Errorf("create source request: %w", err)
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
