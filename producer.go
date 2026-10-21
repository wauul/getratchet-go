package getratchet

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
)

// Status is a terminal run or step status.
type Status string

// Terminal statuses for Run.Finish and Dependency.On.
const (
	Succeeded Status = "SUCCEEDED"
	Failed    Status = "FAILED"
)

// Scope assigns a run to a project and environment. Scoped keys may omit it.
type Scope struct {
	ProjectID     string `json:"projectId"`
	EnvironmentID string `json:"environmentId"`
}

// Run is a handle to an open run. Create one with StartRun, or with Client.Run
// for an existing run ID.
type Run struct {
	ID     string
	client *Client
}

// StartRun creates a run. scope may be nil.
func (c *Client) StartRun(ctx context.Context, name string, scope *Scope) (*Run, error) {
	body := map[string]string{"name": name}
	if scope != nil {
		body["projectId"], body["environmentId"] = scope.ProjectID, scope.EnvironmentID
	}
	var resp struct {
		Run struct {
			ID string `json:"id"`
		} `json:"run"`
	}
	if err := c.do(ctx, http.MethodPost, "/runs", body, &resp, false); err != nil {
		return nil, fmt.Errorf("start run: %w", err)
	}
	return &Run{ID: resp.Run.ID, client: c}, nil
}

// Run returns a handle to an existing run, like the TypeScript useRun.
func (c *Client) Run(id string) *Run { return &Run{ID: id, client: c} }

// Finish asks the server to close the run with Succeeded or Failed. The run
// remains open until its pending work settles.
func (r *Run) Finish(ctx context.Context, status Status) error {
	if err := r.client.do(ctx, http.MethodPatch, "/runs/"+url.PathEscape(r.ID), map[string]Status{"status": status}, nil, false); err != nil {
		return fmt.Errorf("finish run: %w", err)
	}
	return nil
}

// Dependency makes a job run only after StepID reaches On.
type Dependency struct {
	StepID string `json:"stepId"`
	On     Status `json:"on"`
}

// EnqueueOptions identify a durable tool call. Name and Version are required.
type EnqueueOptions struct {
	Name            string
	Version         string
	ContractVersion string      // optional published contract version
	Endpoint        string      // defaults to Name
	IdempotencyKey  string      // defaults to a hash of the operation; see README
	TimeoutMs       int         // optional per-attempt timeout
	MaxAttempts     int         // optional; overrides only the policy's attempt count
	After           *Dependency // optional parent step
}

// EnqueueResult is the accepted (or deduplicated) step.
type EnqueueResult struct {
	JobID        string          `json:"jobId"`
	StepID       string          `json:"stepId"`
	Status       string          `json:"status"`
	Deduplicated bool            `json:"deduplicated"`
	Output       json.RawMessage `json:"output,omitempty"` // set when a deduplicated step already succeeded
}

// Enqueue persists a named, versioned tool call for a worker. It is retried on
// transient failures because the idempotency key deduplicates it server-side.
func (r *Run) Enqueue(ctx context.Context, input any, opts EnqueueOptions) (*EnqueueResult, error) {
	endpoint := opts.Endpoint
	if endpoint == "" {
		endpoint = opts.Name
	}
	key := opts.IdempotencyKey
	if key == "" {
		var contract, after any
		if opts.ContractVersion != "" {
			contract = opts.ContractVersion
		}
		if opts.After != nil {
			after = opts.After
		}
		canonical, err := canonicalJSON([]any{r.ID, opts.Name, opts.Version, contract, endpoint, after, input})
		if err != nil {
			return nil, fmt.Errorf("enqueue: encode input: %w", err)
		}
		sum := sha256.Sum256(canonical)
		key = hex.EncodeToString(sum[:])
	}
	body := struct {
		Name            string      `json:"name"`
		Version         string      `json:"version"`
		ContractVersion string      `json:"contractVersion,omitempty"`
		Endpoint        string      `json:"endpoint"`
		IdempotencyKey  string      `json:"idempotencyKey"`
		Input           any         `json:"input"`
		TimeoutMs       int         `json:"timeoutMs,omitempty"`
		MaxAttempts     int         `json:"maxAttempts,omitempty"`
		After           *Dependency `json:"after,omitempty"`
	}{opts.Name, opts.Version, opts.ContractVersion, endpoint, key, input, opts.TimeoutMs, opts.MaxAttempts, opts.After}
	var result EnqueueResult
	if err := r.client.do(ctx, http.MethodPost, "/runs/"+url.PathEscape(r.ID)+"/enqueue", body, &result, true); err != nil {
		return nil, fmt.Errorf("enqueue %s@%s: %w", opts.Name, opts.Version, err)
	}
	return &result, nil
}

// canonicalJSON encodes v with object keys sorted at every level, matching the
// TypeScript and Python SDKs' generated idempotency keys for the same JSON.
func canonicalJSON(v any) ([]byte, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber() // keep numbers exactly as first encoded
	var generic any
	if err := dec.Decode(&generic); err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(generic); err != nil { // maps encode with sorted keys
		return nil, err
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

// CustomRetryPolicy is a CUSTOM retry policy. Preset must be "CUSTOM".
type CustomRetryPolicy struct {
	Preset         string  `json:"preset"`
	MaxAttempts    int     `json:"maxAttempts"`
	InitialDelayMs int     `json:"initialDelayMs"`
	Multiplier     float64 `json:"multiplier"`
	MaxDelayMs     int     `json:"maxDelayMs"`
	JitterPercent  float64 `json:"jitterPercent"`
}

// ToolContract is a tool contract publication. Optional fields use server defaults when empty.
type ToolContract struct {
	Name               string             `json:"name"`
	ContractVersion    string             `json:"contractVersion"`
	HandlerVersion     string             `json:"handlerVersion"`
	Description        string             `json:"description"`
	Owner              string             `json:"owner"`
	InputSchema        map[string]any     `json:"inputSchema"`
	OutputSchema       map[string]any     `json:"outputSchema"`
	TimeoutMs          int                `json:"timeoutMs,omitempty"`
	DataClassification string             `json:"dataClassification,omitempty"` // PUBLIC, INTERNAL or SENSITIVE
	ReplaySafety       string             `json:"replaySafety,omitempty"`       // SAFE, CAUTION or FORBIDDEN
	RetryPreset        string             `json:"retryPreset,omitempty"`        // STANDARD, AGGRESSIVE, RELAXED or CUSTOM
	RetryPolicy        *CustomRetryPolicy `json:"retryPolicy,omitempty"`        // required only for CUSTOM
}

// PublishedTool identifies a published contract.
type PublishedTool struct {
	ID              string `json:"id"`
	ContractVersion string `json:"contractVersion"`
	HandlerVersion  string `json:"handlerVersion"`
}

// PublishTool publishes a tool contract (ADMIN key).
func (c *Client) PublishTool(ctx context.Context, contract ToolContract) (*PublishedTool, error) {
	var resp struct {
		Tool PublishedTool `json:"tool"`
	}
	if err := c.do(ctx, http.MethodPost, "/tools", contract, &resp, false); err != nil {
		return nil, fmt.Errorf("publish tool %s: %w", contract.Name, err)
	}
	return &resp.Tool, nil
}

// TestToolOptions select a published contract and target scope for a synthetic test.
type TestToolOptions struct {
	Name            string `json:"name"`
	ContractVersion string `json:"contractVersion"`
	ProjectID       string `json:"projectId"`
	EnvironmentID   string `json:"environmentId"`
}

// TestToolResult is a queued one-attempt synthetic test.
type TestToolResult struct {
	RunID  string `json:"runId"`
	StepID string `json:"stepId"`
	JobID  string `json:"jobId"`
	Status string `json:"status"`
	IsTest bool   `json:"isTest"`
}

// TestTool queues a synthetic one-attempt test of a published contract (ADMIN key).
func (c *Client) TestTool(ctx context.Context, input any, opts TestToolOptions) (*TestToolResult, error) {
	body := struct {
		TestToolOptions
		Input any `json:"input"`
	}{opts, input}
	var result TestToolResult
	if err := c.do(ctx, http.MethodPost, "/tools/test", body, &result, false); err != nil {
		return nil, fmt.Errorf("test tool %s: %w", opts.Name, err)
	}
	return &result, nil
}

// EndpointPolicy is an endpoint's current retry, timeout and concurrency policy.
type EndpointPolicy struct {
	ID                 string             `json:"id"`
	PolicyPreset       string             `json:"policyPreset"`
	RetryPolicy        *CustomRetryPolicy `json:"retryPolicy"`
	PolicyVersion      int                `json:"policyVersion"`
	DefaultTimeoutMs   int                `json:"defaultTimeoutMs"`
	MaxConcurrent      int                `json:"maxConcurrent"`
	RateLimitPerMinute *int               `json:"rateLimitPerMinute"`
}

// EndpointPolicyUpdate replaces an endpoint policy if ExpectedVersion is current.
// For Preset "CUSTOM", also set CustomRetryPolicy (its own Preset field is ignored here).
type EndpointPolicyUpdate struct {
	ExpectedVersion    int    `json:"expectedVersion"`
	DefaultTimeoutMs   int    `json:"defaultTimeoutMs"`
	MaxConcurrent      int    `json:"maxConcurrent"`
	RateLimitPerMinute *int   `json:"rateLimitPerMinute"` // nil for no limit
	Preset             string `json:"preset"`
	*CustomRetryPolicy
}

// GetEndpointPolicy reads an endpoint policy (ADMIN key).
func (c *Client) GetEndpointPolicy(ctx context.Context, id string) (*EndpointPolicy, error) {
	return c.endpointPolicy(ctx, http.MethodGet, id, nil)
}

// UpdateEndpointPolicy replaces an endpoint policy (ADMIN key).
func (c *Client) UpdateEndpointPolicy(ctx context.Context, id string, policy EndpointPolicyUpdate) (*EndpointPolicy, error) {
	return c.endpointPolicy(ctx, http.MethodPut, id, policy)
}

func (c *Client) endpointPolicy(ctx context.Context, method, id string, body any) (*EndpointPolicy, error) {
	var resp struct {
		Endpoint EndpointPolicy `json:"endpoint"`
	}
	if err := c.do(ctx, method, "/endpoints/"+url.PathEscape(id)+"/policy", body, &resp, false); err != nil {
		return nil, fmt.Errorf("endpoint policy %s: %w", id, err)
	}
	return &resp.Endpoint, nil
}
