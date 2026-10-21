package getratchet

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"
)

func TestWaitForAndSignal(t *testing.T) {
	var paths []string
	var bodies []map[string]any
	c := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.EscapedPath())
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		bodies = append(bodies, body)
		if len(paths) == 3 {
			w.WriteHeader(http.StatusConflict)
			w.Write([]byte(`{"error":"Wait was already resolved"}`))
			return
		}
		w.Write([]byte(`{"stepId":"s1","key":"approval:1","status":"WAITING"}`))
	})
	run := c.Run("run-1")
	wait, err := run.WaitFor(context.Background(), "approval:1", WaitOptions{TimeoutMs: 60000, Approval: true})
	if err != nil || wait.StepID != "s1" {
		t.Fatalf("wait: %v %+v", err, wait)
	}
	if _, err := run.Signal(context.Background(), "approval:1", map[string]bool{"ok": true}, ""); err != nil {
		t.Fatal(err)
	}
	_, err = run.Signal(context.Background(), "approval:1", nil, Failed)
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Status != http.StatusConflict {
		t.Fatalf("expected 409, got %v", err)
	}
	if paths[0] != "/api/v1/runs/run-1/waits" || paths[1] != "/api/v1/runs/run-1/waits/approval:1/signal" {
		t.Fatalf("paths %v", paths)
	}
	if bodies[0]["name"] != "approval:1" || bodies[0]["approval"] != true || bodies[0]["timeoutMs"] != float64(60000) {
		t.Fatalf("wait body %v", bodies[0])
	}
	if _, ok := bodies[1]["outcome"]; ok || bodies[2]["outcome"] != "FAILED" {
		t.Fatalf("signal bodies %v", bodies[1:])
	}
}

func TestVerifySignature(t *testing.T) {
	body := []byte(`{"jobId":"j"}`)
	sign := func(secret string, ts int64) string {
		mac := hmac.New(sha256.New, []byte(secret))
		fmt.Fprintf(mac, "%d.%s", ts, body)
		return fmt.Sprintf("t=%d,v1=%s", ts, hex.EncodeToString(mac.Sum(nil)))
	}
	now := time.Now().Unix()
	cases := []struct {
		body   []byte
		header string
		want   bool
	}{
		{body, sign("whsec_a", now), true},
		{[]byte(`{"jobId":"k"}`), sign("whsec_a", now), false},
		{body, sign("whsec_b", now), false},
		{body, sign("whsec_a", now-600), false},
		{body, "", false},
	}
	for i, tc := range cases {
		if got := VerifySignature(tc.body, tc.header, "whsec_a", 5*time.Minute); got != tc.want {
			t.Errorf("case %d: got %v", i, got)
		}
	}
}
