package getratchet

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode/utf16"
)

// Job describes the claimed attempt passed to a handler.
type Job struct {
	RunID          string
	StepID         string
	JobID          string
	Attempt        int
	IdempotencyKey string // stable across attempts; pass it to the destination service
	IsTest         bool
	Traceparent    string // W3C trace context from the producer, if any
	Input          json.RawMessage
}

// HandlerFunc runs one attempt. ctx is cancelled on timeout, cancel request,
// lease loss or forced shutdown. The returned output must encode to JSON of at most 64 KiB.
type HandlerFunc func(ctx context.Context, job Job) (any, error)

// RetryDecision is a classifier's verdict for a failed attempt.
type RetryDecision struct {
	Retryable  bool
	RetryAfter time.Duration // optional minimum delay, 1s to 24h, retryable only
}

// Handler is a named, versioned tool implementation.
type Handler struct {
	Name    string
	Version string
	Handle  HandlerFunc
	// ClassifyError optionally decides whether an unknown error is retryable.
	// It is not consulted for RetryableError, NonRetryableError, schema failures or cancellation.
	ClassifyError func(err error, job Job) RetryDecision
}

// Typed adapts a function with JSON-decoded input to a HandlerFunc. Input that
// does not decode into In fails the attempt without a retry.
func Typed[In, Out any](fn func(ctx context.Context, job Job, input In) (Out, error)) HandlerFunc {
	return func(ctx context.Context, job Job) (any, error) {
		var input In
		if err := json.Unmarshal(job.Input, &input); err != nil {
			return nil, NonRetryable(errors.New("Input schema rejected payload"))
		}
		return fn(ctx, job, input)
	}
}

// RetryableError marks a failure as retryable with an optional minimum delay.
type RetryableError struct {
	Err        error
	RetryAfter time.Duration
}

func (e *RetryableError) Error() string { return e.Err.Error() }
func (e *RetryableError) Unwrap() error { return e.Err }

// Retryable wraps err as retryable. retryAfter is optional (0) or 1s to 24h.
func Retryable(err error, retryAfter time.Duration) error {
	return &RetryableError{Err: err, RetryAfter: retryAfter}
}

// NonRetryableError marks a failure as final.
type NonRetryableError struct{ Err error }

func (e *NonRetryableError) Error() string { return e.Err.Error() }
func (e *NonRetryableError) Unwrap() error { return e.Err }

// NonRetryable wraps err so the job is not attempted again.
func NonRetryable(err error) error { return &NonRetryableError{Err: err} }

var (
	errCancelled = NonRetryable(errors.New("Tool cancelled"))
	errLeaseLost = NonRetryable(errors.New("Tool lease lost"))
	errTimedOut  = errors.New("Tool timed out")
)

// WorkerOptions configure Worker.Run.
type WorkerOptions struct {
	Concurrency  int             // jobs in flight, 1–32; default 4
	PollInterval time.Duration   // wait after an empty or failed claim, 1s–60s; default 30s
	OnError      func(err error) // optional local diagnostics; never sent to the API
}

// Worker claims and executes durable jobs for its registered handlers.
type Worker struct {
	// ID is "worker-<uuid>", unique per Worker, as in the TypeScript SDK.
	ID       string
	client   *Client
	mu       sync.Mutex
	handlers map[string]Handler
	running  bool
	stopping bool
	stop     chan struct{}
	done     chan struct{}
	cancel   context.CancelFunc
}

// NewWorker returns a worker using this client's (WORKER-scoped) key.
func (c *Client) NewWorker() *Worker {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	id := fmt.Sprintf("worker-%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
	return &Worker{ID: id, client: c, handlers: map[string]Handler{}}
}

// Register adds a handler. Register every handler before Run.
func (w *Worker) Register(h Handler) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.running {
		return errors.New("getratchet: register handlers before starting the worker")
	}
	if h.Name == "" || h.Version == "" || h.Handle == nil {
		return errors.New("getratchet: a named, versioned handler is required")
	}
	key := h.Name + "\x00" + h.Version
	if _, ok := w.handlers[key]; ok {
		return fmt.Errorf("getratchet: handler already registered: %s@%s", h.Name, h.Version)
	}
	w.handlers[key] = h
	return nil
}

// Run registers the worker and claims jobs until Shutdown is called or ctx is
// done. Cancelling ctx also cancels in-flight handlers; use Shutdown to drain.
// Run returns after every claimed job has been reported (nil after Shutdown).
func (w *Worker) Run(ctx context.Context, opts WorkerOptions) error {
	if opts.Concurrency == 0 {
		opts.Concurrency = 4
	}
	if opts.PollInterval == 0 {
		opts.PollInterval = 30 * time.Second
	}
	if opts.Concurrency < 1 || opts.Concurrency > 32 {
		return errors.New("getratchet: concurrency must be between 1 and 32")
	}
	if opts.PollInterval < time.Second || opts.PollInterval > time.Minute {
		return errors.New("getratchet: poll interval must be between 1 and 60 seconds")
	}
	jobsCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	w.mu.Lock()
	if w.running {
		w.mu.Unlock()
		return errors.New("getratchet: worker already running")
	}
	if len(w.handlers) == 0 {
		w.mu.Unlock()
		return errors.New("getratchet: register at least one handler before starting")
	}
	handlers := make([]map[string]string, 0, len(w.handlers))
	for _, h := range w.handlers {
		handlers = append(handlers, map[string]string{"name": h.Name, "version": h.Version})
	}
	w.running, w.stopping = true, false
	w.stop, w.done, w.cancel = make(chan struct{}), make(chan struct{}), cancel
	stop, done := w.stop, w.done
	w.mu.Unlock()
	defer func() {
		w.mu.Lock()
		w.running = false
		w.mu.Unlock()
		close(done)
	}()

	notify := func(err error) {
		if opts.OnError == nil {
			return
		}
		defer func() { _ = recover() }() // a failing observer must not stop the worker
		opts.OnError(err)
	}
	register := map[string]any{"workerId": w.ID, "capacity": opts.Concurrency, "handlers": handlers}
	if err := w.client.do(ctx, http.MethodPost, "/workers/register", register, nil, false); err != nil {
		return fmt.Errorf("register worker: %w", err)
	}

	slots := make(chan struct{}, opts.Concurrency)
	var wg sync.WaitGroup
	defer wg.Wait()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-stop:
			return nil
		case slots <- struct{}{}:
		}
		var resp struct {
			Job *claimedJob `json:"job"`
		}
		err := w.client.do(ctx, http.MethodPost, "/workers/claim", map[string]string{"workerId": w.ID}, &resp, false)
		if err != nil || resp.Job == nil {
			<-slots
			if err != nil && ctx.Err() == nil {
				notify(fmt.Errorf("getratchet: worker could not claim a job: %w", err))
			}
			t := time.NewTimer(opts.PollInterval)
			select {
			case <-t.C:
			case <-ctx.Done():
			case <-stop:
			}
			t.Stop()
			continue
		}
		wg.Add(1)
		go func(job *claimedJob) {
			defer wg.Done()
			defer func() { <-slots }()
			if err := w.execute(jobsCtx, job, notify); err != nil {
				notify(fmt.Errorf("getratchet: worker could not report an attempt; its lease may expire and the side effect may repeat: %w", err))
			}
		}(resp.Job)
	}
}

// Shutdown stops claiming and waits for in-flight jobs to finish and report.
// If ctx ends first, in-flight handlers are cancelled (and reported as failed
// attempts) before Shutdown returns ctx.Err().
func (w *Worker) Shutdown(ctx context.Context) error {
	w.mu.Lock()
	if !w.running {
		w.mu.Unlock()
		return nil
	}
	if !w.stopping {
		w.stopping = true
		close(w.stop)
	}
	done, cancel := w.done, w.cancel
	w.mu.Unlock()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		cancel()
		<-done
		return ctx.Err()
	}
}

type claimedJob struct {
	JobID          string          `json:"jobId"`
	RunID          string          `json:"runId"`
	StepID         string          `json:"stepId"`
	ToolName       string          `json:"toolName"`
	ToolVersion    string          `json:"toolVersion"`
	Input          json.RawMessage `json:"input"`
	IdempotencyKey string          `json:"idempotencyKey"`
	IsTest         bool            `json:"isTest"`
	TimeoutMs      int             `json:"timeoutMs"`
	Attempt        int             `json:"attempt"`
	LeaseToken     int64           `json:"leaseToken"`
	Traceparent    string          `json:"traceparent"`
}

type report struct {
	WorkerID     string          `json:"workerId"`
	LeaseToken   int64           `json:"leaseToken"`
	Status       Status          `json:"status"`
	Output       json.RawMessage `json:"output,omitempty"`
	Error        string          `json:"error,omitempty"`
	Retryable    *bool           `json:"retryable,omitempty"`
	RetryAfterMs int64           `json:"retryAfterMs,omitempty"`
}

func (w *Worker) execute(parent context.Context, cj *claimedJob, notify func(error)) error {
	w.mu.Lock()
	h, ok := w.handlers[cj.ToolName+"\x00"+cj.ToolVersion]
	w.mu.Unlock()
	if !ok {
		return nil // the lease expires; the server never substitutes a version
	}
	job := Job{cj.RunID, cj.StepID, cj.JobID, cj.Attempt, cj.IdempotencyKey, cj.IsTest, cj.Traceparent, cj.Input}
	ctx, cancel := context.WithCancelCause(parent)
	ctx, cancelTimeout := context.WithTimeoutCause(ctx, time.Duration(cj.TimeoutMs)*time.Millisecond, errTimedOut)
	defer cancelTimeout()
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		w.heartbeat(ctx, cj, cancel)
	}()

	type outcome struct {
		output any
		err    error
	}
	results := make(chan outcome, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				results <- outcome{err: fmt.Errorf("handler panicked: %v", r)}
			}
		}()
		output, err := h.Handle(ctx, job)
		results <- outcome{output, err}
	}()
	var result outcome
	select {
	case result = <-results:
	case <-ctx.Done(): // report promptly even if the handler ignores ctx, as the TS SDK does
		result.err = context.Cause(ctx)
	}
	cause := context.Cause(ctx)
	cancel(nil)
	<-heartbeatDone

	r := report{WorkerID: w.ID, LeaseToken: cj.LeaseToken}
	if result.err == nil {
		output, err := json.Marshal(result.output)
		if err == nil && len(output) <= 64<<10 {
			r.Status, r.Output = Succeeded, output
		} else {
			result.err = NonRetryable(errors.New("Output schema rejected payload"))
		}
	}
	if result.err != nil {
		failure := result.err
		var final *NonRetryableError
		if errors.As(cause, &final) {
			failure = cause // cancellation and lease loss win over the handler's own error
		}
		retryable, retryAfter := decide(failure, job, h.ClassifyError, notify)
		r.Status, r.Error, r.Retryable = Failed, failureMessage(failure), &retryable
		r.RetryAfterMs = retryAfter.Milliseconds()
	}
	// The report must reach the server even when ctx was cancelled by shutdown.
	return w.client.do(context.WithoutCancel(parent), http.MethodPost, "/workers/jobs/"+url.PathEscape(cj.JobID)+"/report", r, nil, false)
}

// heartbeat extends the lease every min(10s, max(1s, timeout/3)) until ctx is
// done, cancelling the attempt on a cancel request or lease loss.
func (w *Worker) heartbeat(ctx context.Context, cj *claimedJob, cancel context.CancelCauseFunc) {
	interval := min(10*time.Second, max(time.Second, time.Duration(cj.TimeoutMs/3)*time.Millisecond))
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	body := map[string]any{"workerId": w.ID, "leaseToken": cj.LeaseToken}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		var state struct {
			Accepted        bool `json:"accepted"`
			CancelRequested bool `json:"cancelRequested"`
		}
		err := w.client.do(ctx, http.MethodPost, "/workers/jobs/"+url.PathEscape(cj.JobID)+"/heartbeat", body, &state, false)
		var apiErr *APIError
		switch {
		case err == nil && state.CancelRequested:
			cancel(errCancelled)
		case errors.As(err, &apiErr) && apiErr.Status == http.StatusConflict, err == nil && !state.Accepted:
			cancel(errLeaseLost)
		}
		// Transient heartbeat failures are ignored; the report is fenced server-side.
	}
}

func validRetryAfter(d time.Duration) bool { return d >= time.Second && d <= 24*time.Hour }

func decide(err error, job Job, classify func(error, Job) RetryDecision, notify func(error)) (retryable bool, retryAfter time.Duration) {
	var final *NonRetryableError
	if errors.As(err, &final) {
		return false, 0
	}
	var retry *RetryableError
	if errors.As(err, &retry) {
		if retry.RetryAfter != 0 && !validRetryAfter(retry.RetryAfter) {
			notify(errors.New("getratchet: RetryAfter must be from 1s through 24h; hint dropped"))
			return true, 0
		}
		return true, retry.RetryAfter
	}
	if classify == nil {
		return true, 0
	}
	defer func() {
		if r := recover(); r != nil {
			notify(fmt.Errorf("getratchet: error classifier failed: %v", r))
			retryable, retryAfter = true, 0
		}
	}()
	d := classify(err, job)
	if d.RetryAfter != 0 && (!d.Retryable || !validRetryAfter(d.RetryAfter)) {
		notify(errors.New("getratchet: invalid classifier RetryAfter"))
		return true, 0
	}
	return d.Retryable, d.RetryAfter
}

var (
	bearerPattern = regexp.MustCompile(`(?i)Bearer\s+\S+`)
	emailPattern  = regexp.MustCompile(`(?i)[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}`)
	secretPattern = regexp.MustCompile(`(?i)(api[_-]?key|token|secret|password)(\s*[=:]\s*)[^\s,;]+`)
)

// failureMessage sends only a redacted first line of at most 2000 UTF-16 units,
// never the error chain's other data.
func failureMessage(err error) string {
	message := bearerPattern.ReplaceAllString(err.Error(), "Bearer [REDACTED]")
	message = emailPattern.ReplaceAllString(message, "[REDACTED EMAIL]")
	message = secretPattern.ReplaceAllString(message, "${1}${2}[REDACTED]")
	message, _, _ = strings.Cut(strings.ReplaceAll(message, "\r\n", "\n"), "\n")
	units := 0
	for i, r := range message {
		if units += utf16.RuneLen(r); units > 2000 {
			message = message[:i]
			break
		}
	}
	if message == "" {
		return "Tool failed"
	}
	return message
}
