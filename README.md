# GetRatchet Go SDK

Durable, named and versioned tool execution for Go 1.23+. Standard library only.

```sh
go get github.com/wauul/getratchet-go
```

Create separate INGEST and WORKER keys in GetRatchet Settings. Scope both to the same project/environment and allow `echo@1` on the worker key. Keep keys in environment variables.

## Producer

```go
package main

import (
	"context"
	"log"
	"os"

	getratchet "github.com/wauul/getratchet-go"
)

func main() {
	ctx := context.Background()
	client, err := getratchet.New("https://getratchet.app", os.Getenv("GETRATCHET_INGEST_KEY"))
	if err != nil {
		log.Fatal(err)
	}
	run, err := client.StartRun(ctx, "First durable run", &getratchet.Scope{
		ProjectID:     os.Getenv("GETRATCHET_PROJECT_ID"),
		EnvironmentID: os.Getenv("GETRATCHET_ENVIRONMENT_ID"),
	})
	if err != nil {
		log.Fatal(err)
	}
	if _, err := run.Enqueue(ctx, map[string]string{"message": "hello"}, getratchet.EnqueueOptions{
		Name: "echo", Version: "1", Endpoint: "Echo", IdempotencyKey: "first-echo-1",
	}); err != nil {
		log.Fatal(err)
	}
	if err := run.Finish(ctx, getratchet.Succeeded); err != nil {
		log.Fatal(err)
	}
}
```

Use `client.Run(id)` to enqueue into an existing run. `EnqueueOptions` also accepts `ContractVersion`, `TimeoutMs`, `MaxAttempts` and `After: &getratchet.Dependency{StepID: id, On: getratchet.Failed}` for a fallback (or `Succeeded` for a follow-up).

## Worker (a separate, long-running process)

```go
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	getratchet "github.com/wauul/getratchet-go"
)

type echoInput struct {
	Message string `json:"message"`
}

func main() {
	client, err := getratchet.New("https://getratchet.app", os.Getenv("GETRATCHET_WORKER_KEY"))
	if err != nil {
		log.Fatal(err)
	}
	worker := client.NewWorker()
	err = worker.Register(getratchet.Handler{
		Name: "echo", Version: "1",
		Handle: getratchet.Typed(func(ctx context.Context, job getratchet.Job, in echoInput) (map[string]string, error) {
			return map[string]string{"echo": in.Message}, nil
		}),
	})
	if err != nil {
		log.Fatal(err)
	}

	signals, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-signals.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = worker.Shutdown(ctx) // stop claiming; drain in-flight jobs for up to 30s
	}()
	if err := worker.Run(context.Background(), getratchet.WorkerOptions{Concurrency: 2}); err != nil {
		log.Fatal(err)
	}
}
```

`Run` registers the worker, claims jobs (`Concurrency` 1–32, default 4; `PollInterval` 1–60s, default 30s) and heartbeats each lease every `min(10s, max(1s, timeout/3))`. The handler's `ctx` is cancelled on timeout, a cancel request, lease loss, or when `Shutdown`'s deadline passes. Cancelling `Run`'s own context stops immediately and cancels in-flight handlers; each claimed job is still reported.

GetRatchet stores and schedules work; your worker executes handlers. Handlers may run again after failure or an expired lease: pass `job.IdempotencyKey` to the destination and honor `ctx` in downstream requests. This SDK does not guarantee exactly-once external side effects.

## Errors

API failures are `*getratchet.APIError` (`Status`, `Code`, `Message`). Match categories with `errors.Is`: `ErrUnauthorized`, `ErrValidation`, `ErrCircuitOpen`, `ErrQuota`, `ErrWorkerUnavailable`, `ErrPlanLimit`. GET requests and `Enqueue` (deduplicated by its idempotency key) are retried up to three times on transport errors and HTTP 429/502/503/504, honoring `Retry-After` up to 30 seconds. Other requests are not retried. Pass `getratchet.WithHTTPClient` to change the default 15-second timeout.

Handlers return `getratchet.Retryable(err, retryAfter)` (hint 1s–24h, or 0) or `getratchet.NonRetryable(err)`, and may set `Handler.ClassifyError` for third-party errors. Unknown errors default to retryable; input decoding failures in `Typed`, oversized output (64 KiB) and cancellation are non-retryable. A panicking classifier falls back to retryable and is reported only to `WorkerOptions.OnError`. Only a redacted first line of the error message is sent to the API.

## Producer administration

`PublishTool`, `TestTool`, `GetEndpointPolicy` and `UpdateEndpointPolicy` mirror the TypeScript SDK and need an ADMIN key.

## Generated idempotency keys

When `IdempotencyKey` is empty, the key is the SHA-256 of canonical JSON (sorted object keys) of the run ID, name, version, contract version (or null), effective endpoint, dependency (or null) and input — the same fields as the TypeScript and Python SDKs. Numeric and string encoding can differ between languages; use an explicit shared key when producers in different languages must deduplicate the same operation.

## Not included

The TypeScript `wrap()` process-bound mode and OpenTelemetry export are not provided. The job's `Traceparent` is passed to handlers so you can continue the trace with your own tracer.

License: MIT.
