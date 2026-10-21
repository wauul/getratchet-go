package getratchet

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func testClient(t *testing.T, h http.HandlerFunc) *Client {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	c, err := New(srv.URL, "test-key")
	if err != nil {
		t.Fatal(err)
	}
	c.retryBase = time.Millisecond
	return c
}

func TestNewRequiresHTTPS(t *testing.T) {
	if _, err := New("http://example.com", "k"); err == nil {
		t.Fatal("expected plain HTTP to be rejected")
	}
	if _, err := New("https://getratchet.app", ""); err == nil {
		t.Fatal("expected an empty key to be rejected")
	}
}

func TestAPIErrors(t *testing.T) {
	cases := []struct {
		status int
		body   string
		is     error
		not    error
	}{
		{403, `{"error":"Limit","code":"plan_limit_reached"}`, ErrPlanLimit, ErrUnauthorized},
		{401, `{"error":"Unauthorized"}`, ErrUnauthorized, ErrPlanLimit},
		{400, `{"error":"Invalid durable tool request"}`, ErrValidation, ErrQuota},
		{409, `{"error":"Circuit open","code":"CIRCUIT_OPEN"}`, ErrCircuitOpen, ErrValidation},
		{503, `not json`, nil, ErrWorkerUnavailable},
	}
	for _, tc := range cases {
		c := testClient(t, func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(tc.status)
			w.Write([]byte(tc.body))
		})
		_, err := c.StartRun(context.Background(), "run", nil)
		var apiErr *APIError
		if !errors.As(err, &apiErr) || apiErr.Status != tc.status {
			t.Fatalf("%d: got %v", tc.status, err)
		}
		if tc.is != nil && !errors.Is(err, tc.is) {
			t.Errorf("%d: expected errors.Is %v", tc.status, tc.is)
		}
		if errors.Is(err, tc.not) {
			t.Errorf("%d: unexpected errors.Is %v", tc.status, tc.not)
		}
	}
}

func TestGetRetriesHonoringRetryAfter(t *testing.T) {
	var calls atomic.Int32
	c := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-key" || r.Header.Get("User-Agent") != "getratchet-go/"+Version {
			t.Errorf("headers: %v", r.Header)
		}
		if r.URL.EscapedPath() != "/api/v1/endpoints/ep%201/policy" {
			t.Errorf("path %q", r.URL.EscapedPath())
		}
		if calls.Add(1) == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(503)
			return
		}
		w.Write([]byte(`{"endpoint":{"id":"ep 1","policyPreset":"STANDARD","policyVersion":3,"rateLimitPerMinute":null}}`))
	})
	start := time.Now()
	policy, err := c.GetEndpointPolicy(context.Background(), "ep 1")
	if err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed < time.Second {
		t.Errorf("Retry-After ignored: retried after %v", elapsed)
	}
	if calls.Load() != 2 || policy.PolicyVersion != 3 || policy.RateLimitPerMinute != nil {
		t.Errorf("calls=%d policy=%+v", calls.Load(), policy)
	}
}

func TestUnsafePostIsNotRetried(t *testing.T) {
	var calls atomic.Int32
	c := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(503)
	})
	if _, err := c.StartRun(context.Background(), "run", nil); err == nil || calls.Load() != 1 {
		t.Fatalf("err=%v calls=%d", err, calls.Load())
	}
}

func TestTransportErrorIsRetriedThenWrapped(t *testing.T) {
	var calls atomic.Int32
	c := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		conn, _, _ := w.(http.Hijacker).Hijack()
		conn.Close()
	})
	_, err := c.GetEndpointPolicy(context.Background(), "ep")
	var apiErr *APIError
	if err == nil || errors.As(err, &apiErr) || calls.Load() != 3 {
		t.Fatalf("err=%v calls=%d", err, calls.Load())
	}
}

func TestEnqueueRequestShapeAndKey(t *testing.T) {
	var keys []string
	c := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/v1/runs/run_1/enqueue" {
			t.Errorf("%s %s", r.Method, r.URL.Path)
		}
		var body map[string]json.RawMessage
		json.NewDecoder(r.Body).Decode(&body)
		for _, field := range []string{"name", "version", "endpoint", "idempotencyKey", "input", "after"} {
			if _, ok := body[field]; !ok {
				t.Errorf("missing %s", field)
			}
		}
		for _, field := range []string{"contractVersion", "timeoutMs", "maxAttempts"} {
			if _, ok := body[field]; ok {
				t.Errorf("unexpected %s", field)
			}
		}
		if string(body["after"]) != `{"stepId":"step_0","on":"SUCCEEDED"}` || string(body["endpoint"]) != `"Echo"` {
			t.Errorf("body %s %s", body["after"], body["endpoint"])
		}
		var key string
		json.Unmarshal(body["idempotencyKey"], &key)
		keys = append(keys, key)
		if len(keys) == 1 {
			w.WriteHeader(502) // enqueue is retried: the key deduplicates it
			return
		}
		w.WriteHeader(202)
		w.Write([]byte(`{"stepId":"step_1","jobId":"job_1","status":"PENDING"}`))
	})
	input := map[string]any{"z": 1, "a": []any{true, "<x>&"}, "m": map[string]any{"b": nil, "a": 2.5}}
	result, err := c.Run("run_1").Enqueue(context.Background(), input, EnqueueOptions{
		Name: "echo", Version: "1", Endpoint: "Echo", After: &Dependency{StepID: "step_0", On: Succeeded},
	})
	if err != nil {
		t.Fatal(err)
	}
	// Same operation hashed by the TypeScript SDK's canonicalJson.
	const tsKey = "2dce2db310717ad9bd9408485e8fe9cba3de60eb2ba3981248bbff7992abba5a"
	if len(keys) != 2 || keys[0] != tsKey || keys[1] != tsKey {
		t.Errorf("keys %v", keys)
	}
	if result.JobID != "job_1" || result.StepID != "step_1" {
		t.Errorf("result %+v", result)
	}
}
