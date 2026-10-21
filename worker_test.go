package getratchet

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeAPI serves the worker routes: each queued job is claimed once, then 204.
type fakeAPI struct {
	mu              sync.Mutex
	jobs            []string
	claims          int
	register        map[string]any
	cancelRequested bool
	reports         chan report
}

func newWorker(t *testing.T, jobs ...string) (*Worker, *fakeAPI) {
	api := &fakeAPI{jobs: jobs, reports: make(chan report, 10)}
	c := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		api.mu.Lock()
		defer api.mu.Unlock()
		switch {
		case r.URL.Path == "/api/v1/workers/register":
			json.NewDecoder(r.Body).Decode(&api.register)
			w.Write([]byte(`{"worker":{}}`))
		case r.URL.Path == "/api/v1/workers/claim":
			api.claims++
			if len(api.jobs) == 0 {
				w.WriteHeader(204)
				return
			}
			w.Write([]byte(`{"job":` + api.jobs[0] + `}`))
			api.jobs = api.jobs[1:]
		case strings.HasSuffix(r.URL.Path, "/heartbeat"):
			json.NewEncoder(w).Encode(map[string]bool{"accepted": true, "cancelRequested": api.cancelRequested})
		case r.URL.Path == "/api/v1/workers/jobs/job_1/report":
			var rep report
			json.NewDecoder(r.Body).Decode(&rep)
			api.reports <- rep
			w.Write([]byte(`{"accepted":true}`))
		default:
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
			w.WriteHeader(404)
		}
	})
	return c.NewWorker(), api
}

func job(timeoutMs string) string {
	return `{"jobId":"job_1","runId":"run_1","stepId":"step_1","toolName":"echo","toolVersion":"1",
		"input":{"message":"hello"},"idempotencyKey":"key-1234","isTest":false,"timeoutMs":` + timeoutMs + `,
		"attempt":1,"leaseToken":7}`
}

func start(t *testing.T, w *Worker) chan error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- w.Run(context.Background(), WorkerOptions{PollInterval: time.Second}) }()
	t.Cleanup(func() { w.Shutdown(context.Background()) })
	return done
}

func nextReport(t *testing.T, api *fakeAPI) report {
	t.Helper()
	select {
	case rep := <-api.reports:
		return rep
	case <-time.After(5 * time.Second):
		t.Fatal("no report")
		return report{}
	}
}

func TestWorkerClaimHandleReport(t *testing.T) {
	w, api := newWorker(t, job("30000"))
	type in struct{ Message string }
	err := w.Register(Handler{Name: "echo", Version: "1", Handle: Typed(func(ctx context.Context, job Job, input in) (map[string]string, error) {
		if job.IdempotencyKey != "key-1234" || job.Attempt != 1 {
			t.Errorf("job %+v", job)
		}
		return map[string]string{"echo": input.Message}, nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	done := start(t, w)
	rep := nextReport(t, api)
	if rep.Status != Succeeded || string(rep.Output) != `{"echo":"hello"}` || rep.LeaseToken != 7 || rep.WorkerID != w.ID {
		t.Errorf("report %+v", rep)
	}
	if err := w.Shutdown(context.Background()); err != nil || <-done != nil {
		t.Fatalf("shutdown %v", err)
	}
	api.mu.Lock()
	defer api.mu.Unlock()
	if !strings.HasPrefix(w.ID, "worker-") || len(w.ID) != 43 || api.register["workerId"] != w.ID || api.register["capacity"] != 4.0 {
		t.Errorf("register %v id %s", api.register, w.ID)
	}
}

func TestWorkerFailureDecisions(t *testing.T) {
	cases := []struct {
		err       error
		classify  func(error, Job) RetryDecision
		retryable bool
		afterMs   int64
		message   string
	}{
		{Retryable(errors.New("rate limited token=abc\nstack"), 5*time.Second), nil, true, 5000, "rate limited token=[REDACTED]"},
		{NonRetryable(errors.New("bad request for a@b.com")), nil, false, 0, "bad request for [REDACTED EMAIL]"},
		{errors.New("boom"), nil, true, 0, "boom"},
		{errors.New("boom"), func(error, Job) RetryDecision { return RetryDecision{Retryable: false} }, false, 0, "boom"},
		{errors.New("boom"), func(error, Job) RetryDecision { panic("classifier bug") }, true, 0, "boom"},
	}
	for _, tc := range cases {
		w, api := newWorker(t, job("30000"))
		w.Register(Handler{Name: "echo", Version: "1", ClassifyError: tc.classify,
			Handle: func(context.Context, Job) (any, error) { return nil, tc.err }})
		start(t, w)
		rep := nextReport(t, api)
		if rep.Status != Failed || *rep.Retryable != tc.retryable || rep.RetryAfterMs != tc.afterMs || rep.Error != tc.message {
			t.Errorf("%v: report %+v retryable=%v", tc.err, rep, *rep.Retryable)
		}
	}
}

func TestHeartbeatCancelRequest(t *testing.T) {
	w, api := newWorker(t, job("3000")) // heartbeat every second
	api.cancelRequested = true
	w.Register(Handler{Name: "echo", Version: "1", Handle: func(ctx context.Context, _ Job) (any, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}})
	start(t, w)
	rep := nextReport(t, api)
	if rep.Status != Failed || *rep.Retryable || rep.Error != "Tool cancelled" {
		t.Errorf("report %+v", rep)
	}
}

func TestGracefulShutdownDrainsInFlight(t *testing.T) {
	w, api := newWorker(t, job("30000"))
	started, release := make(chan struct{}), make(chan struct{})
	w.Register(Handler{Name: "echo", Version: "1", Handle: func(ctx context.Context, _ Job) (any, error) {
		close(started)
		<-release
		return "done", ctx.Err()
	}})
	done := start(t, w)
	<-started
	shutdown := make(chan error, 1)
	go func() { shutdown <- w.Shutdown(context.Background()) }()
	select {
	case <-done:
		t.Fatal("Run returned before the in-flight job finished")
	case <-time.After(100 * time.Millisecond):
	}
	api.mu.Lock()
	claims := api.claims
	api.mu.Unlock()
	close(release)
	if rep := nextReport(t, api); rep.Status != Succeeded || string(rep.Output) != `"done"` {
		t.Errorf("report %+v", rep)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := <-shutdown; err != nil {
		t.Fatal(err)
	}
	api.mu.Lock()
	defer api.mu.Unlock()
	if api.claims != claims {
		t.Errorf("claimed after shutdown: %d -> %d", claims, api.claims)
	}
}

func TestShutdownDeadlineCancelsHandlers(t *testing.T) {
	w, api := newWorker(t, job("30000"))
	started := make(chan struct{})
	w.Register(Handler{Name: "echo", Version: "1", Handle: func(ctx context.Context, _ Job) (any, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}})
	done := start(t, w)
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := w.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shutdown %v", err)
	}
	if rep := nextReport(t, api); rep.Status != Failed || !*rep.Retryable {
		t.Errorf("report %+v", rep)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
