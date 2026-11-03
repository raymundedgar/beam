package notify

import (
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// Overseer reasons from https://miner.b1m.ai/guide/transfer-overseer
var overseer = map[string]string{
	"deadline_expired": "The batch reached the deadline in its offer.",
	"progress_timeout": "A room lane stopped making progress.",
	"no_delivery_cut":  "The batch had delivered nothing and was taken back before that deadline.",
	"tail_cut":         "The batch was taken back before that deadline, late in the transfer.",
	"worker_failed":    "The participant failed the work.",
	"storage_failed":   "The participant reported a storage failure.",
	"path_failed":      "The participant reported that a room path was refused.",
	"endpoint_failed":  "A room endpoint failed.",
	"beam_side":        "Beam failed.",
	"superseded":       "Another attempt covered the work.",
	"transfer_ended":   "The transfer ended first.",
	"batch_cancelled":  "BeamCore cancelled the batch (deadline, take-back, or recovery).",
}

// Pushover sends task-failure alerts when PUSHOVER_TOKEN and PUSHOVER_USER are set.
type Pushover struct {
	token, user, endpoint string
	client                *http.Client
}

// FromEnv returns a client when both keys are present, otherwise nil.
func FromEnv() *Pushover {
	token := strings.TrimSpace(os.Getenv("PUSHOVER_TOKEN"))
	user := strings.TrimSpace(os.Getenv("PUSHOVER_USER"))
	if token == "" || user == "" {
		return nil
	}
	return &Pushover{token: token, user: user, client: &http.Client{Timeout: 15 * time.Second}}
}

// AlertUnsuccessful implements dispatch.FailureAlerter.
func (p *Pushover) AlertUnsuccessful(kind, state, detail string) {
	if p == nil || p.token == "" || p.user == "" {
		return
	}
	reason, meaning := Classify(state, detail)
	var b strings.Builder
	b.WriteString(kind)
	b.WriteString("  ")
	b.WriteString(state)
	b.WriteString("\nreason: ")
	b.WriteString(reason)
	b.WriteString("\n")
	b.WriteString(meaning)
	if extra := strings.TrimSpace(detail); extra != "" && !strings.EqualFold(strings.ReplaceAll(extra, "-", "_"), reason) {
		b.WriteString("\n")
		if len(extra) > 300 {
			extra = extra[:300]
		}
		b.WriteString(extra)
	}
	form := url.Values{
		"token":    {p.token},
		"user":     {p.user},
		"title":    {"Beam task not completed"},
		"message":  {b.String()},
		"priority": {priority()},
	}
	endpoint := p.endpoint
	if endpoint == "" {
		endpoint = "https://api.pushover.net/1/messages.json"
	}
	resp, err := p.client.PostForm(endpoint, form)
	if err != nil {
		log.Printf("pushover: %v", err)
		return
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
	_ = resp.Body.Close()
	if resp.StatusCode >= 300 {
		log.Printf("pushover: HTTP %d", resp.StatusCode)
	}
}

// Classify maps local state/error text onto a transfer-overseer reason.
func Classify(state, detail string) (string, string) {
	blob := strings.ToLower(strings.ReplaceAll(detail, "-", "_"))
	for code, meaning := range overseer {
		if strings.Contains(blob, code) {
			return code, meaning
		}
	}
	switch strings.ToLower(state) {
	case "cancelled":
		return "batch_cancelled", overseer["batch_cancelled"]
	case "failed", "rejected":
		return "worker_failed", overseer["worker_failed"]
	default:
		return state, "The task did not complete successfully."
	}
}

func priority() string {
	if value := strings.TrimSpace(os.Getenv("PUSHOVER_PRIORITY")); value != "" {
		return value
	}
	return "1"
}
