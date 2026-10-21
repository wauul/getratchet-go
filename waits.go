package getratchet

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"time"
)

// WaitOptions configure a wait step. TimeoutMs is required (at most 30 days).
type WaitOptions struct {
	Name      string // defaults to the key
	TimeoutMs int64
	Approval  bool // also show Approve / Reject in the console run view
}

// Wait is a wait-for-event step. Chain jobs after it with Dependency{StepID: w.StepID}.
type Wait struct {
	StepID       string `json:"stepId"`
	Key          string `json:"key"`
	Status       string `json:"status"`
	Approval     bool   `json:"approval"`
	ExpiresAt    string `json:"expiresAt"`
	Deduplicated bool   `json:"deduplicated"`
}

// WaitFor adds a step that waits for Signal(key) or a console approval. The same
// key returns the existing wait, so it is retried on transient failures.
func (r *Run) WaitFor(ctx context.Context, key string, opts WaitOptions) (*Wait, error) {
	name := opts.Name
	if name == "" {
		name = key
	}
	body := map[string]any{"name": name, "key": key, "timeoutMs": opts.TimeoutMs, "approval": opts.Approval}
	var wait Wait
	if err := r.client.do(ctx, http.MethodPost, "/runs/"+url.PathEscape(r.ID)+"/waits", body, &wait, true); err != nil {
		return nil, fmt.Errorf("wait for %s: %w", key, err)
	}
	return &wait, nil
}

// Signal resolves a wait exactly once with outcome Succeeded or Failed (empty
// means Succeeded). Signalling a resolved wait returns a 409 *APIError.
func (r *Run) Signal(ctx context.Context, key string, payload any, outcome Status) (*Wait, error) {
	body := map[string]any{}
	if payload != nil {
		body["payload"] = payload
	}
	if outcome != "" {
		body["outcome"] = outcome
	}
	var wait Wait
	path := "/runs/" + url.PathEscape(r.ID) + "/waits/" + url.PathEscape(key) + "/signal"
	if err := r.client.do(ctx, http.MethodPost, path, body, &wait, false); err != nil {
		return nil, fmt.Errorf("signal %s: %w", key, err)
	}
	return &wait, nil
}

var signatureHeader = regexp.MustCompile(`^t=(\d+),v1=([a-f0-9]{64})$`)

// VerifySignature checks an HTTP worker request: header is X-GetRatchet-Signature,
// body the raw request body. Signatures outside tolerance are rejected.
func VerifySignature(body []byte, header, secret string, tolerance time.Duration) bool {
	match := signatureHeader.FindStringSubmatch(header)
	if match == nil {
		return false
	}
	timestamp, err := strconv.ParseInt(match[1], 10, 64)
	if err != nil || math.Abs(float64(time.Now().Unix()-timestamp)) > tolerance.Seconds() {
		return false
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(match[1] + "."))
	mac.Write(body)
	given, _ := hex.DecodeString(match[2])
	return hmac.Equal(mac.Sum(nil), given)
}
