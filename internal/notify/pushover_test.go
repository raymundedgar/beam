package notify

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestClassifyOverseerReasons(t *testing.T) {
	code, meaning := Classify("cancelled", "deadline_expired")
	if code != "deadline_expired" || meaning == "" {
		t.Fatalf("deadline_expired: %s %s", code, meaning)
	}
	code, meaning = Classify("failed", "destination returned HTTP 403")
	if code != "worker_failed" {
		t.Fatalf("failed default: %s %s", code, meaning)
	}
	code, _ = Classify("cancelled", "batch_cancelled")
	if code != "batch_cancelled" {
		t.Fatalf("batch_cancelled: %s", code)
	}
	code, _ = Classify("cancelled", "progress-timeout on lane")
	if code != "progress_timeout" {
		t.Fatalf("hyphenated: %s", code)
	}
}

func TestAlertUnsuccessfulPosts(t *testing.T) {
	var posted url.Values
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		body, _ := io.ReadAll(request.Body)
		posted, _ = url.ParseQuery(string(body))
		response.WriteHeader(http.StatusOK)
		_, _ = response.Write([]byte(`{"status":1}`))
	}))
	t.Cleanup(server.Close)
	client := &Pushover{token: "tok", user: "usr", endpoint: server.URL, client: server.Client()}
	client.AlertUnsuccessful("transfer.multipart", "cancelled", "deadline_expired")
	if posted.Get("token") != "tok" || posted.Get("user") != "usr" {
		t.Fatalf("credentials: %v", posted)
	}
	if posted.Get("title") != "Beam task not completed" {
		t.Fatalf("title %q", posted.Get("title"))
	}
	message := posted.Get("message")
	if !strings.Contains(message, "transfer.multipart") || !strings.Contains(message, "deadline_expired") {
		t.Fatalf("message %q", message)
	}
}
