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
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"syscall"
	"time"

	workloadcheckpoint "github.com/Beam-Network/beam/internal/workload/checkpoint"
	"github.com/Beam-Network/beam/internal/workload/contracts"
	"github.com/Beam-Network/beam/internal/workload/domain"
	"github.com/Beam-Network/beam/internal/workload/storagehttp"
)

const defaultResponseBodyLimit = 64 << 10
const multipartCheckpointSchema = "beam.transfer.multipart/1"
const presignedRetryAttempts = 4
const presignedRetryBackoff = 250 * time.Millisecond

// maxResumablePart is the largest presigned object this worker buffers so a
// broken GET can resume with Range and If-Range instead of starting over.
const maxResumablePart = 64 << 20

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

type Handler struct {
	client *http.Client
	// wait pauses between authenticated upload attempts.
	wait func(context.Context, time.Duration) error
}

func NewHandler(client *http.Client) *Handler {
	if client == nil {
		client = storagehttp.NewClient(30 * time.Minute)
	}
	return &Handler{client: client, wait: sleepContext}
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
	total := resume.Bytes
	for _, part := range transfer.Parts {
		partKey := strconv.Itoa(part.Index)
		prefix := "part." + partKey + "."
		if completed, ok := resume.Parts[partKey]; ok {
			partResult{sha256: completed.SHA256, etag: completed.ETag, sha1: completed.SHA1,
				requests: completed.RequestCount, providerPartID: completed.ProviderPartID}.record(outputs, prefix, part)
			continue
		}
		partResult, err := h.executePart(ctx, part)
		if err != nil {
			if part.Authenticated() {
				partResult.record(outputs, prefix, part)
				outputs[prefix+"error"] = uploadFailureClass(err)
				recordThrottle(outputs, prefix, err)
			}
			return domain.Result{BytesProcessed: total, Outputs: outputs}, fmt.Errorf("part %d: %w", part.Index, err)
		}
		total += partResult.bytes
		partResult.record(outputs, prefix, part)
		resume.Bytes = total
		resume.Parts[partKey] = partCheckpoint{Bytes: partResult.bytes, SHA256: partResult.sha256, ETag: partResult.etag,
			SHA1: partResult.sha1, RequestCount: partResult.requests, ProviderPartID: partResult.providerPartID}
		if err := workloadcheckpoint.Save(ctx, multipartCheckpointSchema, map[string]string{
			"completed_part": partKey,
		}, resume); err != nil && !errors.Is(err, workloadcheckpoint.ErrUnavailable) {
			return domain.Result{BytesProcessed: total, Outputs: outputs}, fmt.Errorf("save multipart checkpoint: %w", err)
		}
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
	if part.Authenticated() {
		return h.executeAuthenticatedPart(ctx, part)
	}
	if part.Length > 0 && part.Length <= maxResumablePart {
		return h.executeResumablePresigned(ctx, part)
	}
	var last partResult
	var lastErr error
	for attempt := 1; attempt <= presignedRetryAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			if lastErr != nil {
				return last, lastErr
			}
			return partResult{}, err
		}
		last, lastErr = h.executePresignedPart(ctx, part)
		if lastErr == nil || !transientTransferError(lastErr) || attempt == presignedRetryAttempts {
			return last, lastErr
		}
		delay := time.Duration(attempt) * presignedRetryBackoff
		if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) <= delay {
			return last, lastErr
		}
		if err := h.wait(ctx, delay); err != nil {
			return last, lastErr
		}
	}
	return last, lastErr
}

func (h *Handler) executeResumablePresigned(ctx context.Context, part contracts.TransferPart) (partResult, error) {
	body, err := h.readSource(ctx, part)
	if err != nil {
		return partResult{bytes: int64(len(body))}, err
	}
	return h.putPresigned(ctx, part, body)
}

func (h *Handler) readSource(ctx context.Context, part contracts.TransferPart) ([]byte, error) {
	buf := make([]byte, 0, part.Length)
	var etag string
	var lastErr error
	for attempt := 1; attempt <= presignedRetryAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			if lastErr != nil {
				return buf, lastErr
			}
			return buf, err
		}
		resume := int64(len(buf))
		match := etag
		if resume > 0 && match == "" {
			buf = buf[:0]
			resume = 0
		}
		response, err := h.openSourceFrom(ctx, part, resume, match)
		if err != nil {
			lastErr = err
			if !transientTransferError(err) || attempt == presignedRetryAttempts {
				return buf, err
			}
			if err := h.backoff(ctx, attempt); err != nil {
				return buf, lastErr
			}
			continue
		}
		if resume > 0 && response.StatusCode == http.StatusOK {
			buf = buf[:0]
		}
		if tag := strongETag(response.Header.Get("ETag")); tag != "" {
			etag = tag
		}
		need := int(part.Length - int64(len(buf)))
		chunk := make([]byte, need)
		n, readErr := io.ReadFull(response.Body, chunk)
		_ = response.Body.Close()
		if n > 0 {
			buf = append(buf, chunk[:n]...)
		}
		if int64(len(buf)) == part.Length {
			return buf, nil
		}
		if readErr == nil || readErr == io.EOF {
			readErr = io.ErrUnexpectedEOF
		}
		lastErr = fmt.Errorf("fetch source: %w", readErr)
		if !transientTransferError(readErr) || attempt == presignedRetryAttempts {
			return buf, lastErr
		}
		if err := h.backoff(ctx, attempt); err != nil {
			return buf, lastErr
		}
	}
	return buf, lastErr
}

func (h *Handler) putPresigned(ctx context.Context, part contracts.TransferPart, body []byte) (partResult, error) {
	sum := sha256.Sum256(body)
	digest := hex.EncodeToString(sum[:])
	if part.ExpectedSHA256 != "" && !strings.EqualFold(digest, part.ExpectedSHA256) {
		return partResult{bytes: int64(len(body)), sha256: digest}, fmt.Errorf("SHA-256 mismatch: got %s", digest)
	}
	var last partResult
	var lastErr error
	for attempt := 1; attempt <= presignedRetryAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			if lastErr != nil {
				return last, lastErr
			}
			return partResult{}, err
		}
		last, lastErr = h.putPresignedOnce(ctx, part, body, digest)
		if lastErr == nil || !transientTransferError(lastErr) || attempt == presignedRetryAttempts {
			return last, lastErr
		}
		if err := h.backoff(ctx, attempt); err != nil {
			return last, lastErr
		}
	}
	return last, lastErr
}

func (h *Handler) putPresignedOnce(ctx context.Context, part contracts.TransferPart, body []byte, digest string) (partResult, error) {
	destinationMethod := strings.ToUpper(strings.TrimSpace(part.Destination.Method))
	if destinationMethod == "" {
		destinationMethod = http.MethodPut
	}
	destinationRequest, err := http.NewRequestWithContext(ctx, destinationMethod, part.Destination.URL, bytes.NewReader(body))
	if err != nil {
		return partResult{}, fmt.Errorf("create destination request: %w", err)
	}
	copyHeaders(destinationRequest.Header, part.Destination.Headers)
	if destinationRequest.Header.Get("Content-Type") == "" {
		destinationRequest.Header.Set("Content-Type", "application/octet-stream")
	}
	destinationRequest.ContentLength = int64(len(body))
	destinationResponse, err := h.client.Do(destinationRequest)
	if err != nil {
		return partResult{bytes: int64(len(body))}, fmt.Errorf("write destination: %w", err)
	}
	defer destinationResponse.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(destinationResponse.Body, defaultResponseBodyLimit))
	if destinationResponse.StatusCode < 200 || destinationResponse.StatusCode >= 300 {
		return partResult{bytes: int64(len(body))}, &httpStatusError{label: "destination", code: destinationResponse.StatusCode}
	}
	return partResult{
		bytes: int64(len(body)), sha256: digest,
		etag: strings.Trim(destinationResponse.Header.Get("ETag"), `"`),
	}, nil
}

func (h *Handler) backoff(ctx context.Context, attempt int) error {
	delay := time.Duration(attempt) * presignedRetryBackoff
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) <= delay {
		return context.DeadlineExceeded
	}
	return h.wait(ctx, delay)
}

func (h *Handler) executePresignedPart(ctx context.Context, part contracts.TransferPart) (partResult, error) {
	sourceResponse, err := h.openSource(ctx, part)
	if err != nil {
		return partResult{}, err
	}
	defer sourceResponse.Body.Close()

	hash := sha256.New()
	var source io.Reader = sourceResponse.Body
	if part.Length > 0 {
		source = io.LimitReader(source, part.Length)
	}
	counter := &countingReader{reader: io.TeeReader(source, hash)}
	destinationMethod := strings.ToUpper(strings.TrimSpace(part.Destination.Method))
	if destinationMethod == "" {
		destinationMethod = http.MethodPut
	}
	destinationRequest, err := http.NewRequestWithContext(ctx, destinationMethod, part.Destination.URL, counter)
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
	if err != nil {
		return partResult{bytes: counter.count}, fmt.Errorf("write destination: %w", err)
	}
	defer destinationResponse.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(destinationResponse.Body, defaultResponseBodyLimit))
	if destinationResponse.StatusCode < 200 || destinationResponse.StatusCode >= 300 {
		return partResult{bytes: counter.count}, &httpStatusError{label: "destination", code: destinationResponse.StatusCode}
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
	return &httpStatusError{label: label, code: response.StatusCode, body: strings.TrimSpace(string(body))}
}

type httpStatusError struct {
	label string
	code  int
	body  string
}

func (e *httpStatusError) Error() string {
	if e.body == "" {
		return fmt.Sprintf("%s returned HTTP %d", e.label, e.code)
	}
	return fmt.Sprintf("%s returned HTTP %d: %s", e.label, e.code, e.body)
}

func (e *httpStatusError) transient() bool {
	return e.code == http.StatusRequestTimeout || e.code == http.StatusTooManyRequests || e.code >= 500
}

func transientTransferError(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var status *httpStatusError
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
