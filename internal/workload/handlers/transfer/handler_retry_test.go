package transfer

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Beam-Network/beam/internal/workload/contracts"
	"github.com/Beam-Network/beam/internal/workload/domain"
)

func TestPresignedPartRetriesSource503ThenCompletes(t *testing.T) {
	object := []byte("retry-source-object")
	var sourceHits atomic.Int32
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if sourceHits.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, `{"Code":"ServiceUnavailable"}`)
			return
		}
		_, _ = w.Write(object)
	}))
	t.Cleanup(source.Close)
	var destHits atomic.Int32
	dest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		destHits.Add(1)
		if string(body) != string(object) {
			t.Errorf("destination body = %q", body)
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(dest.Close)

	result, err := retryingHandler().Execute(context.Background(), presignedSpec(t, source.URL, dest.URL, int64(len(object))))
	if err != nil {
		t.Fatal(err)
	}
	if result.BytesProcessed != int64(len(object)) || sourceHits.Load() != 2 || destHits.Load() != 1 {
		t.Fatalf("bytes=%d sourceHits=%d destHits=%d", result.BytesProcessed, sourceHits.Load(), destHits.Load())
	}
}

func TestPresignedPartRetriesDestination500ThenCompletes(t *testing.T) {
	object := []byte("retry-dest-object")
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(object)
	}))
	t.Cleanup(source.Close)
	var destHits atomic.Int32
	dest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		if destHits.Add(1) == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(dest.Close)

	result, err := retryingHandler().Execute(context.Background(), presignedSpec(t, source.URL, dest.URL, int64(len(object))))
	if err != nil {
		t.Fatal(err)
	}
	if result.BytesProcessed != int64(len(object)) || destHits.Load() != 2 {
		t.Fatalf("bytes=%d destHits=%d err=%v", result.BytesProcessed, destHits.Load(), err)
	}
}

func TestPresignedPartDoesNotRetryHTTP403(t *testing.T) {
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	t.Cleanup(source.Close)
	dest := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("destination should not be called")
	}))
	t.Cleanup(dest.Close)

	_, err := retryingHandler().Execute(context.Background(), presignedSpec(t, source.URL, dest.URL, 4))
	if err == nil || err.Error() != "part 0: source returned HTTP 403" {
		t.Fatalf("err=%v", err)
	}
}

func retryingHandler() *Handler {
	handler := NewHandler(http.DefaultClient)
	handler.wait = func(context.Context, time.Duration) error { return nil }
	return handler
}

func presignedSpec(t *testing.T, sourceURL, destURL string, length int64) domain.Spec {
	t.Helper()
	payload, err := json.Marshal(contracts.MultipartTransfer{Parts: []contracts.TransferPart{{
		Index: 0, Offset: 0, Length: length,
		Source:      contracts.HTTPEndpoint{URL: sourceURL},
		Destination: contracts.HTTPEndpoint{URL: destURL, Method: http.MethodPut},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	return domain.Spec{Kind: domain.KindTransferMultipart, Payload: payload,
		Resources: domain.Resources{MemoryBytes: 4 << 20}}
}
