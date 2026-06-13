package notifier_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/namburisnehitha/cluster-pulse/internal/ai"
	"github.com/namburisnehitha/cluster-pulse/internal/notifier"
)

func makeAnalysis() ai.Analysis {
	return ai.Analysis{
		PodName:        "pod-1",
		Namespace:      "default",
		Severity:       "critical",
		RootCause:      "OOMKilled",
		Fix:            "increase memory limit",
		KubectlCommand: "kubectl describe pod pod-1",
	}
}

func TestNewNotifier(t *testing.T) {

	// Situation 1: empty URL — should return noopNotifier, Notify returns nil
	n := notifier.NewNotifier("")
	if n == nil {
		t.Fatalf("empty url: got nil, want noopNotifier")
	}
	err := n.Notify(context.Background(), makeAnalysis())
	if err != nil {
		t.Errorf("empty url: notify: got %v, want nil", err)
	}

	// Situation 2: non-empty URL — should return SlackNotifier, not nil
	n = notifier.NewNotifier("https://hooks.slack.com/fake")
	if n == nil {
		t.Fatalf("non-empty url: got nil, want SlackNotifier")
	}
}

func TestNoopNotifier(t *testing.T) {

	// Situation 1: Notify with full analysis — always returns nil, no HTTP calls made
	n := notifier.NewNotifier("")
	err := n.Notify(context.Background(), makeAnalysis())
	if err != nil {
		t.Errorf("full analysis: got %v, want nil", err)
	}

	// Situation 2: Notify with empty analysis — always returns nil, no panic
	n = notifier.NewNotifier("")
	err = n.Notify(context.Background(), ai.Analysis{})
	if err != nil {
		t.Errorf("empty analysis: got %v, want nil", err)
	}

	// Situation 3: Notify with cancelled context — noop ignores context, still returns nil
	n = notifier.NewNotifier("")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = n.Notify(ctx, makeAnalysis())
	if err != nil {
		t.Errorf("cancelled context: got %v, want nil", err)
	}
}

func TestSlackNotifier(t *testing.T) {

	// Situation 1: happy path — sends POST with correct Content-Type and JSON payload
	var capturedReq *http.Request
	var capturedBody []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedReq = r
		capturedBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	n := notifier.NewNotifier(server.URL)
	a := makeAnalysis()
	err := n.Notify(context.Background(), a)
	if err != nil {
		t.Fatalf("happy path: got %v, want nil", err)
	}
	if capturedReq.Method != http.MethodPost {
		t.Errorf("happy path: method: got %s, want POST", capturedReq.Method)
	}
	if capturedReq.Header.Get("Content-Type") != "application/json" {
		t.Errorf("happy path: content-type: got %s, want application/json", capturedReq.Header.Get("Content-Type"))
	}
	var payload map[string]string
	if err := json.Unmarshal(capturedBody, &payload); err != nil {
		t.Fatalf("happy path: payload not valid JSON: %v", err)
	}
	text, ok := payload["text"]
	if !ok {
		t.Fatalf("happy path: payload missing text field")
	}
	if !strings.Contains(text, "pod-1") {
		t.Errorf("happy path: text missing pod name")
	}
	if !strings.Contains(text, "default") {
		t.Errorf("happy path: text missing namespace")
	}
	if !strings.Contains(text, "critical") {
		t.Errorf("happy path: text missing severity")
	}
	if !strings.Contains(text, "OOMKilled") {
		t.Errorf("happy path: text missing root cause")
	}
	if !strings.Contains(text, "increase memory limit") {
		t.Errorf("happy path: text missing fix")
	}
	if !strings.Contains(text, "kubectl describe pod pod-1") {
		t.Errorf("happy path: text missing kubectl command")
	}

	// Situation 2: server returns 400 — should return error with status code
	server400 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer server400.Close()

	n = notifier.NewNotifier(server400.URL)
	err = n.Notify(context.Background(), makeAnalysis())
	if err == nil {
		t.Errorf("400 error: got nil, want error")
	}
	if !strings.Contains(err.Error(), "400") {
		t.Errorf("400 error: error message missing status code: %s", err.Error())
	}

	// Situation 3: server returns 500 — should return error with status code
	server500 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server500.Close()

	n = notifier.NewNotifier(server500.URL)
	err = n.Notify(context.Background(), makeAnalysis())
	if err == nil {
		t.Errorf("500 error: got nil, want error")
	}
	if !strings.Contains(err.Error(), "500") {
		t.Errorf("500 error: error message missing status code: %s", err.Error())
	}

	// Situation 4: server returns 429 rate limited — should return error with status code
	server429 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server429.Close()

	n = notifier.NewNotifier(server429.URL)
	err = n.Notify(context.Background(), makeAnalysis())
	if err == nil {
		t.Errorf("429 rate limited: got nil, want error")
	}
	if !strings.Contains(err.Error(), "429") {
		t.Errorf("429 rate limited: error message missing status code: %s", err.Error())
	}

	// Situation 5: context cancelled before request — should return error
	serverSlow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer serverSlow.Close()

	n = notifier.NewNotifier(serverSlow.URL)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = n.Notify(ctx, makeAnalysis())
	if err == nil {
		t.Errorf("cancelled context: got nil, want error")
	}

	// Situation 6: invalid webhook URL — HTTP request fails, should return error
	n = notifier.NewNotifier("http://localhost:0/invalid")
	err = n.Notify(context.Background(), makeAnalysis())
	if err == nil {
		t.Errorf("invalid url: got nil, want error")
	}

	// Situation 7: empty analysis fields — no panic, sends whatever is in struct
	serverEmpty := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer serverEmpty.Close()

	n = notifier.NewNotifier(serverEmpty.URL)
	err = n.Notify(context.Background(), ai.Analysis{})
	if err != nil {
		t.Errorf("empty analysis: got %v, want nil", err)
	}
}
