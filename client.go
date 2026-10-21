// Package getratchet is the Go SDK for GetRatchet durable tool execution.
//
// A producer starts a run and enqueues named, versioned tool calls. A separate,
// long-running worker registers handlers for those exact names and versions,
// claims jobs, and reports results. Execution is at least once: pass
// Job.IdempotencyKey to the destination service for irreversible writes.
package getratchet

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Version is the SDK version sent in the User-Agent header.
const Version = "0.1.0"

// maxRetryAfter bounds how long a client request waits on a Retry-After hint;
// longer hints return the error to the caller instead.
const maxRetryAfter = 30 * time.Second

// Client calls the GetRatchet API with one API key. It is safe for concurrent use.
type Client struct {
	baseURL    string
	apiKey     string
	httpClient *http.Client
	retryBase  time.Duration
}

// Option configures a Client.
type Option func(*Client)

// WithHTTPClient replaces the default HTTP client (15-second timeout).
func WithHTTPClient(h *http.Client) Option { return func(c *Client) { c.httpClient = h } }

// New returns a client for baseURL (for example https://getratchet.app).
// HTTPS is required except for localhost development.
func New(baseURL, apiKey string, opts ...Option) (*Client, error) {
	if !strings.HasPrefix(baseURL, "https://") &&
		!strings.HasPrefix(baseURL, "http://localhost") &&
		!strings.HasPrefix(baseURL, "http://127.0.0.1") {
		return nil, errors.New("getratchet: use HTTPS outside local development")
	}
	if apiKey == "" {
		return nil, errors.New("getratchet: an API key is required")
	}
	c := &Client{
		baseURL:    strings.TrimRight(baseURL, "/"),
		apiKey:     apiKey,
		httpClient: &http.Client{Timeout: 15 * time.Second},
		retryBase:  250 * time.Millisecond,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c, nil
}

// Sentinel errors matched by APIError via errors.Is.
var (
	ErrUnauthorized      = errors.New("getratchet: unauthorized")
	ErrValidation        = errors.New("getratchet: validation failed")
	ErrCircuitOpen       = errors.New("getratchet: circuit open")
	ErrQuota             = errors.New("getratchet: rate limit or quota exceeded")
	ErrWorkerUnavailable = errors.New("getratchet: no worker available")
	ErrPlanLimit         = errors.New("getratchet: plan limit reached")
)

// APIError is a non-2xx API response.
type APIError struct {
	Status  int
	Code    string
	Message string
}

func (e *APIError) Error() string {
	if e.Code != "" {
		return fmt.Sprintf("getratchet: HTTP %d %s: %s", e.Status, e.Code, e.Message)
	}
	return fmt.Sprintf("getratchet: HTTP %d: %s", e.Status, e.Message)
}

var (
	circuitPattern = regexp.MustCompile(`(?i)circuit`)
	workerPattern  = regexp.MustCompile(`(?i)worker`)
)

// Is maps the response to a sentinel error, mirroring the TypeScript SDK's error classes.
func (e *APIError) Is(target error) bool {
	switch target {
	case ErrPlanLimit:
		return e.Code == "plan_limit_reached"
	case ErrUnauthorized:
		return (e.Status == 401 || e.Status == 403) && e.Code != "plan_limit_reached"
	case ErrValidation:
		return e.Status == 400 || e.Status == 413 || e.Status == 422
	case ErrCircuitOpen:
		return e.Status == 409 && (e.Code == "CIRCUIT_OPEN" || circuitPattern.MatchString(e.Message))
	case ErrQuota:
		return e.Status == 429
	case ErrWorkerUnavailable:
		return e.Status == 503 && workerPattern.MatchString(e.Message)
	}
	return false
}

func retryableStatus(status int) bool {
	return status == 429 || status == 502 || status == 503 || status == 504
}

// do sends one API request. GET and safeRetry requests are attempted up to three
// times on transport errors and 429/502/503/504, honoring Retry-After. out is left
// untouched on 204 No Content.
func (c *Client) do(ctx context.Context, method, path string, body, out any, safeRetry bool) error {
	var payload []byte
	if body != nil {
		var err error
		if payload, err = json.Marshal(body); err != nil {
			return fmt.Errorf("getratchet: encode request: %w", err)
		}
	}
	attempts := 1
	if method == http.MethodGet || safeRetry {
		attempts = 3
	}
	for attempt := 0; ; attempt++ {
		wait := c.retryBase << attempt
		req, err := http.NewRequestWithContext(ctx, method, c.baseURL+"/api/v1"+path, bytes.NewReader(payload))
		if err != nil {
			return fmt.Errorf("getratchet: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("User-Agent", "getratchet-go/"+Version)
		resp, err := c.httpClient.Do(req)
		if err != nil {
			if attempt+1 < attempts && ctx.Err() == nil {
				if sleep(ctx, wait) == nil {
					continue
				}
			}
			return fmt.Errorf("getratchet: transport unavailable: %w", err)
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			defer resp.Body.Close()
			if resp.StatusCode == http.StatusNoContent || out == nil {
				return nil
			}
			if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
				return fmt.Errorf("getratchet: decode response: %w", err)
			}
			return nil
		}
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		resp.Body.Close()
		var data struct {
			Error string `json:"error"`
			Code  string `json:"code"`
		}
		_ = json.Unmarshal(raw, &data)
		if data.Error == "" {
			data.Error = http.StatusText(resp.StatusCode)
		}
		apiErr := &APIError{Status: resp.StatusCode, Code: data.Code, Message: data.Error}
		if attempt+1 < attempts && retryableStatus(resp.StatusCode) {
			if hint, ok := retryAfter(resp.Header.Get("Retry-After")); ok {
				if hint > maxRetryAfter {
					return apiErr
				}
				wait = max(wait, hint)
			}
			if sleep(ctx, wait) == nil {
				continue
			}
		}
		return apiErr
	}
}

func retryAfter(value string) (time.Duration, bool) {
	if value == "" {
		return 0, false
	}
	if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second, true
	}
	if at, err := http.ParseTime(value); err == nil {
		return max(0, time.Until(at)), true
	}
	return 0, false
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
